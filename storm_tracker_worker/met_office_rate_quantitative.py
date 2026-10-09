"""Unwired quantitative decoder for one exact Met Office UK RATE capture.

Only a complete, byte-verified schema-3 capture produced by
``met_office_native`` is accepted.  The delivered float32 ODIM ``RATE`` field
is decoded on its native grid with the delivered gain and offset.  ``nodata``
and ``undetect`` remain separate masks.  No threshold, interpolation,
reprojection, wet/dry classification, cell extraction, admission or fallback
exists in this module.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import errno
import hashlib
import hmac
import io
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
from typing import Any, Callable, Mapping
from urllib.parse import unquote
from xml.etree import ElementTree as ET
import zlib

import h5py
import numpy as np

from .canonical_evidence_store import (
    CanonicalEvidenceStore,
    DecoderRunReceipt,
    EVIDENCE_CONTRACT,
)
from . import met_office_native as native
from .raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RAW_ARTIFACT_CAPTURE_RECEIPT_SCHEMA,
    RAW_ARTIFACT_CAPTURE_SCHEMA,
    RAW_ARTIFACT_SCHEMA_VERSION,
    RawArtifactCaptureReceipt,
    RawArtifactIntegrityError,
    RawArtifactReceipt,
    RawArtifactStore,
)


PROVIDER = native.PROVIDER
CAPTURE_PRODUCT = native.CAPTURE_PRODUCT
LISTING_PRODUCT = native.LISTING_PRODUCT
HDF5_PRODUCT = native.HDF5_PRODUCT
MEMBER_ROLES = ("listing_current", "listing_previous", "odim_hdf5")

DECODER_IDENTITY = "met-office-uk-rate-odim-h5-quantitative"
DECODER_VERSION = "met-office-rate-quantitative-v1"
ARTIFACT_SCHEMA = "stv3-worker-met-office-rate-raster-v1"
ARTIFACT_MAGIC = b"STV3-MET-OFFICE-RATE-Q1\n"
ARTIFACT_HANDLE_PREFIX = "artifact:met-office-rate-quantitative:"
STORE_PROFILE_SCHEMA = "stv3-worker-met-office-rate-derived-store-profile-v1"
STORE_PROFILE_FILENAME = ".store-profile.json"

RATE_UNIT = "mm/h"
RATE_GAIN = 1.0
RATE_OFFSET = 0.0
RATE_NODATA = -1.0
RATE_UNDETECT = 0.0
QC_STATE = "unavailable"
QC_REASON_CODE = "odim_quality_dataset_absent"

NATIVE_MINIMUM_X_METRES = -405_000.0
NATIVE_MINIMUM_Y_METRES = -625_000.0
NATIVE_MAXIMUM_X_METRES = 1_320_000.0
NATIVE_MAXIMUM_Y_METRES = 1_550_000.0

MAX_CAPTURE_MEMBER_BYTES = native.MAX_HDF5_BYTES + 2 * native.MAX_LISTING_BYTES
MAX_HDF_OBJECTS = 128
MAX_HEADER_BYTES = 128 * 1024
MAX_UNCOMPRESSED_ARRAY_BYTES = 64 * 1024 * 1024
MAX_ARTIFACT_BYTES = 64 * 1024 * 1024
DEFAULT_MAX_DERIVED_BYTES = 4 * 1024 * 1024 * 1024
DEFAULT_MAX_DERIVED_OBJECTS = 8_192
DEFAULT_MAX_STAGING_OBJECTS = 8
DECODE_TOTAL_DEADLINE_SECONDS = 90.0
STORE_INITIALIZE_DEADLINE_SECONDS = 10.0
LOCK_POLL_SECONDS = 0.01
ZLIB_LEVEL = 9

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_RAW_RECEIPT_RE = re.compile(r"^raw-[0-9a-f]{64}$")
_CAPTURE_ID_RE = re.compile(r"^capture-[0-9a-f]{64}$")
_DECODER_JOB_RE = re.compile(r"^decoder-job-[0-9a-f]{64}$")
_ATTEMPT_RE = re.compile(r"^decoder-attempt-[0-9a-f]{64}$")
_CAPTURE_FETCH_RE = re.compile(
    r"^(?P<job>[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,191}):capture$"
)
_HANDLE_RE = re.compile(
    rf"^{re.escape(ARTIFACT_HANDLE_PREFIX)}(?P<sha>[0-9a-f]{{64}})$"
)
_OBJECT_RE = re.compile(r"^(?P<sha>[0-9a-f]{64})\.morq$")
_STAGING_RE = re.compile(
    r"^\.(?P<sha>[0-9a-f]{64})\.[A-Za-z0-9_-]{1,128}$"
)
_KEY_RE = re.compile(
    r"^radar/(?P<day>\d{4}/\d{2}/\d{2})/"
    r"(?P<stamp>\d{12})_ODIM_ng_radar_rainrate_composite_1km_UK\.h5$"
)
_PREFIX_RE = re.compile(r"^radar/\d{4}/\d{2}/\d{2}/$")
_ETAG_RE = re.compile(r'^"[0-9A-Fa-f]{32}"$')
_WMO_RE = re.compile(r"^WMO:\d{5}$")
_NODE_RE = re.compile(r"^[a-z0-9]{3,16}$")
_ARTIFACT_ARRAYS = (
    ("rate_encoded", "<f4"),
    ("rate_mm_h", "<f4"),
    ("rate_valid_mask", "|u1"),
    ("rate_nodata_mask", "|u1"),
    ("rate_undetect_mask", "|u1"),
)
_HDF_PAYLOAD_ERRORS = (
    OSError,
    RuntimeError,
    ValueError,
    KeyError,
    TypeError,
    IndexError,
    OverflowError,
    EOFError,
    AttributeError,
    BufferError,
    struct.error,
)
_STORE_LOCKS_GUARD = threading.Lock()
_STORE_LOCKS: dict[str, threading.RLock] = {}


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


_GRID_DEFINITION = {
    "axis_order": "yx",
    "columns": native.GRID_COLUMNS,
    "corners_wgs84": native.GRID_CORNERS,
    "origin": "upper_left",
    "projection": native.GRID_PROJECTION,
    "rows": native.GRID_ROWS,
    "xscale_metres": native.GRID_SCALE_METRES,
    "yscale_metres": native.GRID_SCALE_METRES,
}
GRID_DEFINITION_SHA256 = hashlib.sha256(
    _canonical_json(_GRID_DEFINITION)
).hexdigest()
_COVERAGE_DEFINITION = {
    "crs": native.GRID_PROJECTION,
    "maximum_x": NATIVE_MAXIMUM_X_METRES,
    "maximum_y": NATIVE_MAXIMUM_Y_METRES,
    "minimum_x": NATIVE_MINIMUM_X_METRES,
    "minimum_y": NATIVE_MINIMUM_Y_METRES,
    "pixel_bounds": True,
}
COVERAGE_DEFINITION_SHA256 = hashlib.sha256(
    _canonical_json(_COVERAGE_DEFINITION)
).hexdigest()
_DECODER_CONFIG = {
    "artifact_schema": ARTIFACT_SCHEMA,
    "capture_product": CAPTURE_PRODUCT,
    "decoder_identity": DECODER_IDENTITY,
    "decoder_version": DECODER_VERSION,
    "grid_definition_sha256": GRID_DEFINITION_SHA256,
    "member_roles": MEMBER_ROLES,
    "odim": {
        "conventions": native.ODIM_CONVENTIONS,
        "gain": RATE_GAIN,
        "nodata": RATE_NODATA,
        "object": native.ODIM_OBJECT,
        "offset": RATE_OFFSET,
        "product": native.ODIM_PRODUCT,
        "quantity": native.ODIM_QUANTITY,
        "undetect": RATE_UNDETECT,
        "unit": RATE_UNIT,
        "version": native.ODIM_VERSION,
    },
    "policies": {
        "interpolation": "forbidden",
        "quality_when_absent": QC_STATE,
        "reprojection": "forbidden",
        "thresholding": "forbidden",
        "wet_dry_classification": "forbidden",
    },
    "resource_contract": {
        "deadline_seconds": DECODE_TOTAL_DEADLINE_SECONDS,
        "hard_child_isolation": False,
        "max_artifact_bytes": MAX_ARTIFACT_BYTES,
        "max_derived_objects": DEFAULT_MAX_DERIVED_OBJECTS,
        "max_hdf_objects": MAX_HDF_OBJECTS,
        "max_staging_objects": DEFAULT_MAX_STAGING_OBJECTS,
        "max_uncompressed_array_bytes": MAX_UNCOMPRESSED_ARRAY_BYTES,
    },
    "serialization": {
        "array_order": "C",
        "float_byte_order": "little",
        "zlib_level": ZLIB_LEVEL,
        "zlib_runtime_version": zlib.ZLIB_RUNTIME_VERSION,
    },
}
DECODER_CONFIG_SHA256 = hashlib.sha256(
    _canonical_json(_DECODER_CONFIG)
).hexdigest()


class MetOfficeRateQuantitativeError(RuntimeError):
    """Base failure for this isolated decoder candidate."""


class MetOfficeRateCaptureMissingError(MetOfficeRateQuantitativeError):
    """The requested complete schema-3 capture does not exist."""


class MetOfficeRateArtifactError(MetOfficeRateQuantitativeError):
    """The immutable derived artifact store failed closed."""


class MetOfficeRateArtifactCapacityError(MetOfficeRateArtifactError):
    """An immutable derived-store bound was reached."""


class MetOfficeRateDecodeDeadlineError(MetOfficeRateQuantitativeError):
    """The absolute cooperative monotonic deadline expired."""


class _RejectedDecode(Exception):
    def __init__(self, outcome: str, reason_code: str) -> None:
        self.outcome = outcome
        self.reason_code = reason_code
        super().__init__(reason_code)


@dataclass(frozen=True, slots=True)
class MetOfficeRateArtifactRef:
    handle: str
    sha256: str
    size_bytes: int


@dataclass(frozen=True, slots=True)
class MetOfficeRateRasterArtifact:
    metadata: Mapping[str, Any]
    rate_encoded: np.ndarray
    rate_mm_h: np.ndarray
    rate_valid_mask: np.ndarray
    rate_nodata_mask: np.ndarray
    rate_undetect_mask: np.ndarray


@dataclass(frozen=True, slots=True)
class MetOfficeRateDecodeResult:
    outcome: str
    reason_code: str
    evidence_receipt: DecoderRunReceipt
    artifact: MetOfficeRateArtifactRef | None


@dataclass(frozen=True, slots=True)
class _SelectedObject:
    key: str
    product_timestamp: float
    last_modified: float
    etag: str
    size_bytes: int
    storage_class: str
    checksum_algorithm: str | None
    checksum_type: str | None


@dataclass(frozen=True, slots=True)
class _VerifiedSource:
    capture: RawArtifactCaptureReceipt
    receipts: Mapping[str, RawArtifactReceipt]
    hdf5_payload: bytes
    selected: _SelectedObject


@dataclass(frozen=True, slots=True)
class _DecodedFrame:
    arrays: Mapping[str, np.ndarray]
    nominal_time: float
    input_window_start: float
    input_window_end: float
    source_wmo_ids: tuple[str, ...]
    source_nodes: tuple[str, ...]
    software: str
    software_version: str
    task: str
    zr_a: float
    zr_b: float
    counts: Mapping[str, int]
    detected_min_mm_h: float | None
    detected_max_mm_h: float | None


class MetOfficeRateRawAuthority:
    """One-capture authority that hashes every raw member at most once."""

    def __init__(self, raw_store: RawArtifactStore) -> None:
        if not isinstance(raw_store, RawArtifactStore):
            raise TypeError("raw_store moet RawArtifactStore zijn")
        self.raw_store = raw_store
        self._lock = threading.RLock()
        self._capture_id: str | None = None
        self._bundle: tuple[
            RawArtifactCaptureReceipt,
            dict[str, RawArtifactReceipt],
            dict[str, bytes],
        ] | None = None
        self._receipt_only: RawArtifactCaptureReceipt | None = None
        self._absence_cached = False
        self._raw_verification_count = 0

    @property
    def raw_verification_count(self) -> int:
        return self._raw_verification_count

    def _bind(self, capture_id: str) -> None:
        if not isinstance(capture_id, str) or _CAPTURE_ID_RE.fullmatch(capture_id) is None:
            raise ValueError("capture_id is geen schema-3 capture-identiteit")
        if self._capture_id is not None and self._capture_id != capture_id:
            raise MetOfficeRateQuantitativeError(
                "één raw-authoritysessie mag niet over captures heen worden hergebruikt"
            )

    def load_capture_bundle_bytes(self, capture_id: str):
        with self._lock:
            self._bind(capture_id)
            if self._capture_id is None:
                self._capture_id = capture_id
            if self._bundle is not None:
                capture, receipts, payloads = self._bundle
                return capture, dict(receipts), dict(payloads)
            if self._absence_cached:
                return None
            if self._receipt_only is not None:
                raise MetOfficeRateQuantitativeError(
                    "capture werd al zonder decoderbytes geverifieerd"
                )
            self.raw_store.initialize()
            bundle = self.raw_store.load_capture_bundle_bytes(capture_id)
            self._raw_verification_count += 1
            if bundle is None:
                self._absence_cached = True
                return None
            capture, receipts, payloads = bundle
            self._bundle = (capture, dict(receipts), dict(payloads))
            return capture, dict(receipts), dict(payloads)

    def load_capture_receipt(self, capture_id: str):
        with self._lock:
            self._bind(capture_id)
            if self._capture_id is None:
                self._capture_id = capture_id
            if self._bundle is not None:
                return self._bundle[0]
            if self._receipt_only is not None:
                return self._receipt_only
            if self._absence_cached:
                return None
            self.raw_store.initialize()
            capture = self.raw_store.load_capture_receipt(capture_id)
            self._receipt_only = capture
            self._absence_cached = capture is None
            self._raw_verification_count += 1
            return capture


def open_met_office_rate_evidence_store(
    path: str | Path,
    *,
    raw_authority: MetOfficeRateRawAuthority,
    clock: Callable[[], float] = time.time,
    fault_injector: Callable[[str], None] | None = None,
    max_database_bytes: int = 256 * 1024 * 1024,
) -> CanonicalEvidenceStore:
    if not isinstance(raw_authority, MetOfficeRateRawAuthority):
        raise TypeError("raw_authority heeft verkeerd type")
    return CanonicalEvidenceStore(
        path,
        raw_artifact_reader=raw_authority,
        clock=clock,
        fault_injector=fault_injector,
        max_database_bytes=max_database_bytes,
    )


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _thread_store_lock(root: Path) -> threading.RLock:
    identity = os.path.normcase(str(root.resolve()))
    with _STORE_LOCKS_GUARD:
        lock = _STORE_LOCKS.get(identity)
        if lock is None:
            lock = threading.RLock()
            _STORE_LOCKS[identity] = lock
        return lock


@contextmanager
def _exclusive_store_lock(root: Path, *, checkpoint: Callable[[str], None]):
    if not callable(checkpoint):
        raise TypeError("artifact-lock checkpoint moet callable zijn")
    thread_lock = _thread_store_lock(root)
    thread_acquired = False
    descriptor: int | None = None
    file_acquired = False
    try:
        while not thread_acquired:
            checkpoint("artifact_thread_lock_wait")
            thread_acquired = thread_lock.acquire(blocking=False)
            if not thread_acquired:
                time.sleep(LOCK_POLL_SECONDS)
        lock_path = root / ".publish.lock"
        flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_BINARY", 0) | getattr(
            os, "O_NOFOLLOW", 0
        )
        descriptor = os.open(lock_path, flags, 0o600)
        opened = os.fstat(descriptor)
        current = lock_path.lstat()
        if (
            lock_path.is_symlink()
            or not stat.S_ISREG(opened.st_mode)
            or not stat.S_ISREG(current.st_mode)
            or (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino)
        ):
            raise MetOfficeRateArtifactError("artifact-processlock is onveilig")
        if opened.st_size == 0:
            os.ftruncate(descriptor, 1)
            os.fsync(descriptor)
        while not file_acquired:
            checkpoint("artifact_process_lock_wait")
            os.lseek(descriptor, 0, os.SEEK_SET)
            try:
                if os.name == "nt":  # pragma: no cover - selected platform
                    import msvcrt

                    msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
                else:  # pragma: no branch - selected platform
                    import fcntl

                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                file_acquired = True
            except OSError as error:
                if error.errno not in {
                    errno.EACCES,
                    errno.EAGAIN,
                    errno.EDEADLK,
                    errno.EWOULDBLOCK,
                }:
                    raise
                time.sleep(LOCK_POLL_SECONDS)
        checkpoint("artifact_process_lock_acquired")
        yield
    except (MetOfficeRateArtifactError, MetOfficeRateDecodeDeadlineError):
        raise
    except OSError as error:
        raise MetOfficeRateArtifactError("artifact-processlock faalde") from error
    finally:
        if descriptor is not None:
            try:
                if file_acquired:
                    os.lseek(descriptor, 0, os.SEEK_SET)
                    if os.name == "nt":  # pragma: no cover - selected platform
                        import msvcrt

                        msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
                    else:  # pragma: no branch - selected platform
                        import fcntl

                        fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)
        if thread_acquired:
            thread_lock.release()


class MetOfficeRateDerivedArtifactStore:
    """Bounded content-addressed store for immutable candidate artifacts."""

    def __init__(
        self,
        root: str | Path,
        *,
        max_total_bytes: int = DEFAULT_MAX_DERIVED_BYTES,
        max_objects: int = DEFAULT_MAX_DERIVED_OBJECTS,
        max_staging_objects: int = DEFAULT_MAX_STAGING_OBJECTS,
        fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        supplied = Path(root)
        if not supplied.is_absolute():
            raise ValueError("derived artifact root moet absoluut zijn")
        for value, name, minimum in (
            (max_total_bytes, "max_total_bytes", MAX_ARTIFACT_BYTES),
            (max_objects, "max_objects", 1),
            (max_staging_objects, "max_staging_objects", 1),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
                raise ValueError(f"derived artifactgrens {name} is ongeldig")
        self.root = supplied
        self.objects_path = supplied / "objects"
        self.staging_path = supplied / "staging"
        self.profile_path = supplied / STORE_PROFILE_FILENAME
        self.max_total_bytes = max_total_bytes
        self.max_objects = max_objects
        self.max_staging_objects = max_staging_objects
        self._fault_injector = fault_injector
        self.initialize()

    def _fault(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(point)

    def _profile_bytes(self) -> bytes:
        return _canonical_json({
            "artifact_schema": ARTIFACT_SCHEMA,
            "filename_suffix": ".morq",
            "limits": {
                "max_object_bytes": MAX_ARTIFACT_BYTES,
                "max_objects": self.max_objects,
                "max_staging_objects": self.max_staging_objects,
                "max_total_bytes": self.max_total_bytes,
            },
            "schema": STORE_PROFILE_SCHEMA,
        })

    def initialize(self) -> None:
        initialize_deadline = time.monotonic() + STORE_INITIALIZE_DEADLINE_SECONDS

        def initialize_checkpoint(stage: str) -> None:
            current = time.monotonic()
            if not math.isfinite(current) or current >= initialize_deadline:
                raise MetOfficeRateDecodeDeadlineError(
                    f"artifact-store initialisatiedeadline verstreek bij {stage}"
                )

        for path in (self.root, self.objects_path, self.staging_path):
            if path.exists() or path.is_symlink():
                info = path.lstat()
                if path.is_symlink() or not stat.S_ISDIR(info.st_mode):
                    raise MetOfficeRateArtifactError("artifactlayout is onveilig")
        self.root.mkdir(parents=True, exist_ok=True)
        self.objects_path.mkdir(mode=0o700, exist_ok=True)
        self.staging_path.mkdir(mode=0o700, exist_ok=True)
        expected = self._profile_bytes()
        with _exclusive_store_lock(
            self.root, checkpoint=initialize_checkpoint
        ):
            if self.profile_path.exists() or self.profile_path.is_symlink():
                actual = self._verify_regular(
                    self.profile_path,
                    expected_size=len(expected),
                    expected_sha256=hashlib.sha256(expected).hexdigest(),
                    allowed_parent=self.root,
                    return_bytes=True,
                )
                if actual != expected:
                    raise MetOfficeRateArtifactError("artifact-storeprofiel wijkt af")
            else:
                self._publish_profile(expected)
            self._scan_layout(checkpoint=initialize_checkpoint)

    def _publish_profile(self, payload: bytes) -> None:
        digest = hashlib.sha256(payload).hexdigest()
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                dir=self.staging_path, prefix=f".{digest}.", delete=False
            ) as temporary:
                temporary.write(payload)
                temporary.flush()
                os.fsync(temporary.fileno())
                temporary_path = Path(temporary.name)
            _fsync_directory(self.staging_path)
            try:
                os.link(temporary_path, self.profile_path)
            except FileExistsError:
                pass
            _fsync_directory(self.root)
        except OSError as error:
            if error.errno in {errno.ENOSPC, errno.EDQUOT, errno.EFBIG}:
                raise MetOfficeRateArtifactCapacityError(
                    "artifact-storeprofiel heeft onvoldoende opslag"
                ) from error
            raise MetOfficeRateArtifactError(
                "artifact-storeprofiel kon niet duurzaam worden gepubliceerd"
            ) from error
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)
                _fsync_directory(self.staging_path)

    def _scan_layout(
        self, *, checkpoint: Callable[[str], None]
    ) -> tuple[int, int, int]:
        allowed_root = {"objects", "staging", STORE_PROFILE_FILENAME, ".publish.lock"}
        root_entries = list(self.root.iterdir())
        if len(root_entries) > 4 or any(path.name not in allowed_root for path in root_entries):
            raise MetOfficeRateArtifactError("artifactroot bevat onbekende entries")
        total = 0
        objects = 0
        staging = 0
        for directory, pattern, limit, stage in (
            (self.objects_path, _OBJECT_RE, self.max_objects, "objects"),
            (self.staging_path, _STAGING_RE, self.max_staging_objects, "staging"),
        ):
            for path in directory.iterdir():
                checkpoint(f"artifact_{stage}_inventory")
                count = objects if directory == self.objects_path else staging
                if count >= limit:
                    raise MetOfficeRateArtifactCapacityError(
                        f"artifact {stage}-objectgrens is bereikt"
                    )
                if pattern.fullmatch(path.name) is None:
                    raise MetOfficeRateArtifactError(
                        f"artifact {stage} bevat vreemde entry"
                    )
                info = path.lstat()
                if path.is_symlink() or not stat.S_ISREG(info.st_mode):
                    raise MetOfficeRateArtifactError("artifactlayout bevat link/niet-bestand")
                if not 0 < info.st_size <= MAX_ARTIFACT_BYTES:
                    raise MetOfficeRateArtifactCapacityError(
                        "artifactobject overschrijdt bytegrens"
                    )
                total += int(info.st_size)
                if directory == self.objects_path:
                    objects += 1
                else:
                    staging += 1
        for path in (self.profile_path, self.root / ".publish.lock"):
            if path.exists():
                info = path.lstat()
                if path.is_symlink() or not stat.S_ISREG(info.st_mode):
                    raise MetOfficeRateArtifactError("artifactmetadata is onveilig")
                total += int(info.st_size)
        if total > self.max_total_bytes:
            raise MetOfficeRateArtifactCapacityError("artifact-store is over capaciteit")
        return total, objects, staging

    @staticmethod
    def _verify_regular(
        path: Path,
        *,
        expected_size: int,
        expected_sha256: str,
        allowed_parent: Path,
        return_bytes: bool,
    ) -> bytes | None:
        if path.parent.resolve() != allowed_parent.resolve() or path.is_symlink():
            raise MetOfficeRateArtifactError("artifactpad ontsnapt of is een link")
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_size != expected_size:
            raise MetOfficeRateArtifactError("artifactgrootte/type wijkt af")
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
        chunks = [] if return_bytes else None
        digest = hashlib.sha256()
        total = 0
        try:
            opened = os.fstat(descriptor)
            if not stat.S_ISREG(opened.st_mode) or opened.st_size != expected_size:
                raise MetOfficeRateArtifactError("artifact wijzigde tijdens openen")
            while True:
                chunk = os.read(descriptor, min(1024 * 1024, expected_size + 1 - total))
                if not chunk:
                    break
                total += len(chunk)
                if total > expected_size:
                    raise MetOfficeRateArtifactError("artifact bevat extra bytes")
                digest.update(chunk)
                if chunks is not None:
                    chunks.append(chunk)
        finally:
            os.close(descriptor)
        after = path.lstat()
        if (
            total != expected_size
            or not hmac.compare_digest(digest.hexdigest(), expected_sha256)
            or after.st_size != opened.st_size
            or (after.st_dev, after.st_ino) != (opened.st_dev, opened.st_ino)
        ):
            raise MetOfficeRateArtifactError("artifact hash/grootte/inode wijkt af")
        return None if chunks is None else b"".join(chunks)

    def _reference(self, digest: str, size: int) -> MetOfficeRateArtifactRef:
        return MetOfficeRateArtifactRef(
            handle=f"{ARTIFACT_HANDLE_PREFIX}{digest}",
            sha256=digest,
            size_bytes=size,
        )

    def publish(
        self,
        payload: bytes,
        *,
        checkpoint: Callable[[str], None],
    ) -> MetOfficeRateArtifactRef:
        if not isinstance(payload, bytes) or not 0 < len(payload) <= MAX_ARTIFACT_BYTES:
            raise MetOfficeRateArtifactCapacityError("artifactpayload overschrijdt objectgrens")
        digest = hashlib.sha256(payload).hexdigest()
        target = self.objects_path / f"{digest}.morq"
        with _exclusive_store_lock(self.root, checkpoint=checkpoint):
            total, object_count, staging_count = self._scan_layout(checkpoint=checkpoint)
            candidates = [
                path for path in self.staging_path.iterdir()
                if path.name.startswith(f".{digest}.")
            ]
            if target.exists() or target.is_symlink():
                self._verify_regular(
                    target,
                    expected_size=len(payload),
                    expected_sha256=digest,
                    allowed_parent=self.objects_path,
                    return_bytes=False,
                )
                for candidate in candidates:
                    self._verify_regular(
                        candidate,
                        expected_size=len(payload),
                        expected_sha256=digest,
                        allowed_parent=self.staging_path,
                        return_bytes=False,
                    )
                    candidate.unlink()
                _fsync_directory(self.staging_path)
                return self._reference(digest, len(payload))
            if object_count >= self.max_objects:
                raise MetOfficeRateArtifactCapacityError("artifactobjectgrens is bereikt")
            if total + len(payload) > self.max_total_bytes:
                raise MetOfficeRateArtifactCapacityError("artifactbytegrens is bereikt")
            temporary_path: Path | None = None
            try:
                if candidates:
                    if len(candidates) != 1:
                        raise MetOfficeRateArtifactError("meerdere gelijke stagingobjecten")
                    temporary_path = candidates[0]
                    self._verify_regular(
                        temporary_path,
                        expected_size=len(payload),
                        expected_sha256=digest,
                        allowed_parent=self.staging_path,
                        return_bytes=False,
                    )
                else:
                    if staging_count >= self.max_staging_objects:
                        raise MetOfficeRateArtifactCapacityError(
                            "artifact stagingobjectgrens is bereikt"
                        )
                    with tempfile.NamedTemporaryFile(
                        dir=self.staging_path,
                        prefix=f".{digest}.",
                        delete=False,
                    ) as temporary:
                        temporary.write(payload)
                        temporary.flush()
                        os.fsync(temporary.fileno())
                        temporary_path = Path(temporary.name)
                    _fsync_directory(self.staging_path)
                self._fault("artifact.after_stage_fsync")
                try:
                    os.link(temporary_path, target)
                except FileExistsError:
                    pass
                _fsync_directory(self.objects_path)
                self._fault("artifact.after_link")
                self._verify_regular(
                    target,
                    expected_size=len(payload),
                    expected_sha256=digest,
                    allowed_parent=self.objects_path,
                    return_bytes=False,
                )
                self._fault("artifact.after_publish_verify")
            except OSError as error:
                if error.errno in {errno.ENOSPC, errno.EDQUOT, errno.EFBIG}:
                    raise MetOfficeRateArtifactCapacityError(
                        "artifactpublicatie heeft onvoldoende opslag"
                    ) from error
                raise MetOfficeRateArtifactError("artifactpublicatie faalde") from error
            finally:
                if temporary_path is not None and temporary_path.exists():
                    temporary_path.unlink()
                    _fsync_directory(self.staging_path)
        return self._reference(digest, len(payload))

    def read_bytes(
        self,
        reference: MetOfficeRateArtifactRef,
        *,
        checkpoint: Callable[[str], None],
    ) -> bytes:
        if not isinstance(reference, MetOfficeRateArtifactRef):
            raise TypeError("artifactreference heeft verkeerd type")
        match = _HANDLE_RE.fullmatch(reference.handle)
        if (
            match is None
            or match.group("sha") != reference.sha256
            or _SHA256_RE.fullmatch(reference.sha256) is None
            or not 0 < reference.size_bytes <= MAX_ARTIFACT_BYTES
        ):
            raise MetOfficeRateArtifactError("artifactreference is ongeldig")
        checkpoint("artifact_read_before")
        payload = self._verify_regular(
            self.objects_path / f"{reference.sha256}.morq",
            expected_size=reference.size_bytes,
            expected_sha256=reference.sha256,
            allowed_parent=self.objects_path,
            return_bytes=True,
        )
        checkpoint("artifact_read_after")
        assert payload is not None
        return payload

    def read_decoded(
        self,
        reference: MetOfficeRateArtifactRef,
        *,
        checkpoint: Callable[[str], None],
    ) -> MetOfficeRateRasterArtifact:
        return _parse_artifact(self.read_bytes(reference, checkpoint=checkpoint), checkpoint)


def _exact_keys(value: object, expected: set[str], context: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != expected:
        raise _RejectedDecode("unsupported", f"unsupported:{context}_schema")
    return value


def _finite_number(value: object, context: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float, np.number)):
        raise _RejectedDecode("unsupported", f"unsupported:{context}")
    result = float(value)
    if not math.isfinite(result):
        raise _RejectedDecode("unsupported", f"unsupported:{context}")
    return result


def _bounded_optional_text(value: object, context: str, maximum: int) -> str | None:
    if value is None:
        return None
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise _RejectedDecode("unsupported", f"unsupported:{context}")
    return value


def _validate_http_provenance(
    value: object,
    *,
    object_etag: str | None,
    object_last_modified: float | None,
) -> None:
    http = _exact_keys(
        value,
        {
            "etag",
            "last_modified",
            "request_id",
            "request_id_2",
            "server_side_encryption",
            "version_id",
        },
        "http_provenance",
    )
    etag = http["etag"]
    modified = http["last_modified"]
    if object_etag is not None:
        if etag != object_etag or _finite_number(modified, "http_last_modified") != object_last_modified:
            raise _RejectedDecode("unsupported", "unsupported:http_object_revision")
    else:
        if etag is not None and (
            not isinstance(etag, str) or _ETAG_RE.fullmatch(etag) is None
        ):
            raise _RejectedDecode("unsupported", "unsupported:http_listing_etag")
        if modified is not None:
            _finite_number(modified, "http_listing_last_modified")
    _bounded_optional_text(http["request_id"], "http_request_id", 256)
    _bounded_optional_text(http["request_id_2"], "http_request_id_2", 2_048)
    _bounded_optional_text(http["server_side_encryption"], "http_sse", 128)
    _bounded_optional_text(http["version_id"], "http_version_id", 1_024)


def _listing_receipt_contract(
    receipt: RawArtifactReceipt,
    *,
    role: str,
    job_id: str,
) -> str:
    if (
        receipt.provider != PROVIDER
        or receipt.product != LISTING_PRODUCT
        or receipt.fetch_id != f"{job_id}:{role}"
        or receipt.content_type not in {"application/xml", "text/xml"}
        or not 0 < receipt.size_bytes <= native.MAX_LISTING_BYTES
        or receipt.upstream_id == ""
    ):
        raise _RejectedDecode("unsupported", "unsupported:listing_receipt_contract")
    provenance = _exact_keys(
        receipt.provenance,
        {
            "representation",
            "capture_stage",
            "collector",
            "collector_version",
            "bucket",
            "operation",
            "role",
            "prefix",
            "list_type",
            "max_keys",
            "encoding_type",
            "http",
            "decoded_or_admitted",
        },
        "listing_provenance",
    )
    prefix = provenance["prefix"]
    if (
        provenance["representation"] != NATIVE_REPRESENTATION
        or provenance["capture_stage"] != NATIVE_CAPTURE_STAGE
        or provenance["collector"] != native.RAW_COLLECTOR
        or provenance["collector_version"] != native.RAW_COLLECTOR_VERSION
        or provenance["bucket"] != native.BUCKET_NAME
        or provenance["operation"] != "ListObjectsV2"
        or provenance["role"] != role
        or not isinstance(prefix, str)
        or _PREFIX_RE.fullmatch(prefix) is None
        or provenance["list_type"] != 2
        or provenance["max_keys"] != native.LIST_MAX_KEYS
        or provenance["encoding_type"] != "url"
        or provenance["decoded_or_admitted"] is not False
        or receipt.upstream_id
        != f"s3-list-v2:{native.BUCKET_NAME}:{prefix}:max-{native.LIST_MAX_KEYS}"
    ):
        raise _RejectedDecode("unsupported", "unsupported:listing_provenance")
    _validate_http_provenance(
        provenance["http"], object_etag=None, object_last_modified=None
    )
    return prefix


def _hdf_receipt_contract(
    receipt: RawArtifactReceipt,
    *,
    selected: _SelectedObject,
    job_id: str,
) -> None:
    if (
        receipt.provider != PROVIDER
        or receipt.product != HDF5_PRODUCT
        or receipt.fetch_id != f"{job_id}:odim_hdf5"
        or receipt.content_type
        not in {"application/octet-stream", "application/x-hdf5", "binary/octet-stream"}
        or receipt.size_bytes != selected.size_bytes
        or not 0 < receipt.size_bytes <= native.MAX_HDF5_BYTES
        or receipt.upstream_id
        != f"s3:{native.BUCKET_NAME}:{selected.key}:etag:{selected.etag}"
    ):
        raise _RejectedDecode("unsupported", "unsupported:hdf_receipt_contract")
    provenance = _exact_keys(
        receipt.provenance,
        {
            "representation",
            "capture_stage",
            "collector",
            "collector_version",
            "bucket",
            "operation",
            "upstream_key",
            "listing_etag",
            "listing_last_modified",
            "listing_size_bytes",
            "listing_storage_class",
            "listing_checksum_algorithm",
            "listing_checksum_type",
            "filename_product_timestamp",
            "http",
            "schema3_full_object_buffer_required",
            "operational_memory_certified",
            "decoded_or_admitted",
        },
        "hdf_provenance",
    )
    expected = {
        "representation": NATIVE_REPRESENTATION,
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": native.RAW_COLLECTOR,
        "collector_version": native.RAW_COLLECTOR_VERSION,
        "bucket": native.BUCKET_NAME,
        "operation": "GetObject",
        "upstream_key": selected.key,
        "listing_etag": selected.etag,
        "listing_last_modified": selected.last_modified,
        "listing_size_bytes": selected.size_bytes,
        "listing_storage_class": selected.storage_class,
        "listing_checksum_algorithm": selected.checksum_algorithm,
        "listing_checksum_type": selected.checksum_type,
        "filename_product_timestamp": selected.product_timestamp,
        "schema3_full_object_buffer_required": True,
        "operational_memory_certified": False,
        "decoded_or_admitted": False,
    }
    if any(provenance[key] != value for key, value in expected.items()):
        raise _RejectedDecode("unsupported", "unsupported:hdf_provenance")
    _validate_http_provenance(
        provenance["http"],
        object_etag=selected.etag,
        object_last_modified=selected.last_modified,
    )


def _selected_object(value: native.MetOfficeObject) -> _SelectedObject:
    return _SelectedObject(
        key=value.key,
        product_timestamp=value.product_timestamp,
        last_modified=value.last_modified,
        etag=value.etag,
        size_bytes=value.size_bytes,
        storage_class=value.storage_class,
        checksum_algorithm=value.checksum_algorithm,
        checksum_type=value.checksum_type,
    )


def _validate_raw_capture(
    capture: RawArtifactCaptureReceipt,
    *,
    receipts: Mapping[str, RawArtifactReceipt],
    payloads: Mapping[str, bytes],
    checkpoint: Callable[[str], None],
) -> _VerifiedSource:
    if (
        capture.raw_store_schema_version != RAW_ARTIFACT_SCHEMA_VERSION
        or capture.capture_schema != RAW_ARTIFACT_CAPTURE_SCHEMA
        or capture.capture_receipt_schema != RAW_ARTIFACT_CAPTURE_RECEIPT_SCHEMA
        or capture.complete is not True
        or capture.provider != PROVIDER
        or capture.product != CAPTURE_PRODUCT
        or capture.product_timestamp is None
        or not math.isfinite(float(capture.product_timestamp))
    ):
        raise _RejectedDecode("unsupported", "unsupported:capture_contract")
    fetch_match = _CAPTURE_FETCH_RE.fullmatch(capture.fetch_id)
    if fetch_match is None:
        raise _RejectedDecode("unsupported", "unsupported:capture_fetch_id")
    job_id = fetch_match.group("job")
    expected_roles = set(MEMBER_ROLES)
    if (
        len(capture.members) != 3
        or {member.role for member in capture.members} != expected_roles
        or set(receipts) != expected_roles
        or set(payloads) != expected_roles
    ):
        raise _RejectedDecode("incomplete", "incomplete:capture_roles")
    total_bytes = 0
    members = {member.role: member for member in capture.members}
    for role in MEMBER_ROLES:
        checkpoint(f"raw_member_contract_{role}")
        receipt = receipts[role]
        payload = payloads[role]
        member = members[role]
        if not isinstance(payload, bytes) or not payload:
            raise RawArtifactIntegrityError("Met Office raw capturelid bevat geen bytes")
        total_bytes += len(payload)
        if (
            len(payload) != receipt.size_bytes
            or member.member_id != role
            or member.receipt_id != receipt.receipt_id
            or member.content_sha256 != receipt.content_sha256
            or member.receipt_sha256 != receipt.receipt_sha256
            or member.received_at != receipt.received_at
        ):
            raise RawArtifactIntegrityError("Met Office capturelid en raw receipt verschillen")
    if total_bytes > MAX_CAPTURE_MEMBER_BYTES:
        raise _RejectedDecode("incomplete", "incomplete:capture_byte_capacity")

    prefixes = {
        role: _listing_receipt_contract(receipts[role], role=role, job_id=job_id)
        for role in ("listing_previous", "listing_current")
    }
    try:
        previous_date = datetime.strptime(
            prefixes["listing_previous"], "radar/%Y/%m/%d/"
        ).replace(tzinfo=timezone.utc)
        current_date = datetime.strptime(
            prefixes["listing_current"], "radar/%Y/%m/%d/"
        ).replace(tzinfo=timezone.utc)
    except ValueError as error:
        raise _RejectedDecode("unsupported", "unsupported:listing_prefix_time") from error
    if (current_date - previous_date).total_seconds() != 86_400:
        raise _RejectedDecode("unsupported", "unsupported:listing_prefix_pair")
    try:
        groups = tuple(
            native._parse_listing(  # noqa: SLF001 - same exact native capture contract
                payloads[role], expected_prefix=prefixes[role]
            )
            for role in ("listing_previous", "listing_current")
        )
        selected_native = native._select_latest(groups)  # noqa: SLF001
    except native.MetOfficeNativeError as error:
        raise _RejectedDecode("decode_error", "decode_error:listing_contract") from error
    selected = _selected_object(selected_native)
    _hdf_receipt_contract(receipts["odim_hdf5"], selected=selected, job_id=job_id)
    expected_capture_id = (
        f"{CAPTURE_PRODUCT}|key={selected.key}|etag={selected.etag}"
        f"|previous={receipts['listing_previous'].content_sha256}"
        f"|current={receipts['listing_current'].content_sha256}"
        f"|hdf5={receipts['odim_hdf5'].content_sha256}"
    )
    if (
        capture.product_timestamp != selected.product_timestamp
        or capture.upstream_capture_id != expected_capture_id
    ):
        raise _RejectedDecode("unsupported", "unsupported:capture_identity")
    checkpoint("raw_capture_contract_complete")
    return _VerifiedSource(
        capture=capture,
        receipts=dict(receipts),
        hdf5_payload=payloads["odim_hdf5"],
        selected=selected,
    )


def _decode_hdf(
    source: _VerifiedSource,
    *,
    checkpoint: Callable[[str], None],
) -> _DecodedFrame:
    selected = native.MetOfficeObject(
        key=source.selected.key,
        product_timestamp=source.selected.product_timestamp,
        last_modified=source.selected.last_modified,
        etag=source.selected.etag,
        size_bytes=source.selected.size_bytes,
        storage_class=source.selected.storage_class,
        checksum_algorithm=source.selected.checksum_algorithm,
        checksum_type=source.selected.checksum_type,
    )
    checkpoint("odim_metadata_before")
    try:
        identity = native._inspect_odim(  # noqa: SLF001 - reapply exact capture profile
            source.hdf5_payload, selected=selected
        )
    except (native.MetOfficeNativeError, *_HDF_PAYLOAD_ERRORS) as error:
        raise _RejectedDecode("decode_error", "decode_error:odim_contract") from error
    checkpoint("odim_metadata_after")
    try:
        with h5py.File(io.BytesIO(source.hdf5_payload), "r") as root:
            visited: list[str] = []

            def visitor(name: str) -> None:
                visited.append(name)
                if len(visited) > MAX_HDF_OBJECTS:
                    raise _RejectedDecode("decode_error", "decode_error:hdf_object_capacity")

            root.visit(visitor)
            if set(visited) != {
                "dataset1",
                "dataset1/data1",
                "dataset1/data1/data",
                "dataset1/data1/what",
                "dataset1/how",
                "dataset1/what",
                "how",
                "what",
                "where",
            }:
                raise _RejectedDecode("unsupported", "unsupported:odim_object_tree")
            dataset_group = native._hard_child(  # noqa: SLF001
                root, "dataset1", h5py.Group
            )
            data_group = native._hard_child(  # noqa: SLF001
                dataset_group, "data1", h5py.Group
            )
            raster = native._hard_child(  # noqa: SLF001
                data_group, "data", h5py.Dataset
            )
            if raster.dtype != np.dtype("float32"):
                raise _RejectedDecode("unsupported", "unsupported:rate_dtype")
            encoded = np.empty(
                (native.GRID_ROWS, native.GRID_COLUMNS), dtype=np.dtype("<f4")
            )
            for start in range(0, native.GRID_ROWS, identity.chunk_rows):
                stop = min(native.GRID_ROWS, start + identity.chunk_rows)
                checkpoint("rate_chunk_before")
                encoded[start:stop, :] = raster[start:stop, :]
                checkpoint("rate_chunk_after")
    except _RejectedDecode:
        raise
    except _HDF_PAYLOAD_ERRORS as error:
        raise _RejectedDecode("decode_error", "decode_error:raster_read") from error

    if encoded.nbytes > MAX_UNCOMPRESSED_ARRAY_BYTES:
        raise _RejectedDecode("decode_error", "decode_error:array_capacity")
    if not np.isfinite(encoded).all():
        raise _RejectedDecode("decode_error", "decode_error:rate_nonfinite")
    nodata = np.asarray(encoded == np.float32(RATE_NODATA), dtype=np.uint8)
    undetect = np.asarray(encoded == np.float32(RATE_UNDETECT), dtype=np.uint8)
    valid = np.asarray((nodata == 0) & (undetect == 0), dtype=np.uint8)
    if np.any(encoded[valid == 1] < np.float32(0.0)):
        raise _RejectedDecode("decode_error", "decode_error:rate_negative")
    rate = np.zeros(encoded.shape, dtype=np.dtype("<f4"))
    rate[valid == 1] = np.asarray(
        encoded[valid == 1] * np.float32(RATE_GAIN) + np.float32(RATE_OFFSET),
        dtype=np.dtype("<f4"),
    )
    if not np.isfinite(rate[valid == 1]).all() or np.any(rate[valid == 1] < 0.0):
        raise _RejectedDecode("decode_error", "decode_error:rate_calibration")
    arrays: dict[str, np.ndarray] = {
        "rate_encoded": np.ascontiguousarray(encoded, dtype=np.dtype("<f4")),
        "rate_mm_h": rate,
        "rate_valid_mask": valid,
        "rate_nodata_mask": nodata,
        "rate_undetect_mask": undetect,
    }
    if sum(array.nbytes for array in arrays.values()) > MAX_UNCOMPRESSED_ARRAY_BYTES:
        raise _RejectedDecode("decode_error", "decode_error:array_capacity")
    valid_values = rate[valid == 1]
    counts = {
        "pixels_total": int(encoded.size),
        "pixels_valid": int(np.count_nonzero(valid)),
        "pixels_nodata": int(np.count_nonzero(nodata)),
        "pixels_undetect": int(np.count_nonzero(undetect)),
    }
    for array in arrays.values():
        array.flags.writeable = False
    checkpoint("rate_semantics_complete")
    return _DecodedFrame(
        arrays=arrays,
        nominal_time=identity.product_timestamp,
        input_window_start=identity.input_window_start,
        input_window_end=identity.input_window_end,
        source_wmo_ids=identity.source_wmo_ids,
        source_nodes=identity.source_nodes,
        software=identity.software,
        software_version=identity.software_version,
        task=identity.task,
        zr_a=identity.zr_a,
        zr_b=identity.zr_b,
        counts=counts,
        detected_min_mm_h=(None if valid_values.size == 0 else float(valid_values.min())),
        detected_max_mm_h=(None if valid_values.size == 0 else float(valid_values.max())),
    )


def _capture_members(capture: RawArtifactCaptureReceipt) -> list[dict[str, Any]]:
    return [
        {
            "content_sha256": member.content_sha256,
            "member_id": member.member_id,
            "receipt_id": member.receipt_id,
            "receipt_sha256": member.receipt_sha256,
            "received_at": member.received_at,
            "role": member.role,
        }
        for member in capture.members
    ]


def _artifact_capture_metadata(capture: RawArtifactCaptureReceipt) -> dict[str, Any]:
    return {
        "capture_id": capture.capture_id,
        "capture_manifest_sha256": capture.manifest_sha256,
        "capture_receipt_sha256": capture.capture_receipt_sha256,
        "fetch_id": capture.fetch_id,
        "members": _capture_members(capture),
        "product": capture.product,
        "product_timestamp": capture.product_timestamp,
        "provider": capture.provider,
        "upstream_capture_id": capture.upstream_capture_id,
    }


def _artifact_source_metadata(source: _VerifiedSource) -> dict[str, Any]:
    hdf = source.receipts["odim_hdf5"]
    return {
        "bucket": native.BUCKET_NAME,
        "collector": native.RAW_COLLECTOR,
        "collector_version": native.RAW_COLLECTOR_VERSION,
        "hdf5": {
            "content_sha256": hdf.content_sha256,
            "content_type": hdf.content_type,
            "raw_receipt_id": hdf.receipt_id,
            "raw_receipt_sha256": hdf.receipt_sha256,
            "size_bytes": hdf.size_bytes,
        },
        "listings": {
            role: {
                "content_sha256": source.receipts[role].content_sha256,
                "raw_receipt_id": source.receipts[role].receipt_id,
                "raw_receipt_sha256": source.receipts[role].receipt_sha256,
                "size_bytes": source.receipts[role].size_bytes,
            }
            for role in ("listing_current", "listing_previous")
        },
        "selected_object": {
            "checksum_algorithm": source.selected.checksum_algorithm,
            "checksum_type": source.selected.checksum_type,
            "etag": source.selected.etag,
            "key": source.selected.key,
            "last_modified": source.selected.last_modified,
            "size_bytes": source.selected.size_bytes,
            "storage_class": source.selected.storage_class,
        },
        "structure_inspector_version": native.STRUCTURE_INSPECTOR_VERSION,
    }


def _artifact_product_metadata(frame: _DecodedFrame) -> dict[str, Any]:
    return {
        "gain": RATE_GAIN,
        "input_window_end_timestamp": frame.input_window_end,
        "input_window_is_uniform_observation_time": False,
        "input_window_start_timestamp": frame.input_window_start,
        "interpolation_applied": False,
        "nodata": RATE_NODATA,
        "nominal_timestamp": frame.nominal_time,
        "offset": RATE_OFFSET,
        "operational_admission_eligible": False,
        "quality_control": {
            "quality_dataset_present": False,
            "reason_code": QC_REASON_CODE,
            "state": QC_STATE,
        },
        "quantity": "surface_precipitation_rate_estimate",
        "reprojection_applied": False,
        "thresholding_applied": False,
        "undetect": RATE_UNDETECT,
        "unit": RATE_UNIT,
        "upstream_quantity": native.ODIM_QUANTITY,
        "wet_dry_classification_applied": False,
    }


def _artifact_grid_metadata() -> dict[str, Any]:
    return {
        "axis_order": "yx",
        "columns": native.GRID_COLUMNS,
        "coverage": dict(_COVERAGE_DEFINITION),
        "coverage_definition_sha256": COVERAGE_DEFINITION_SHA256,
        "grid_definition_sha256": GRID_DEFINITION_SHA256,
        "origin": "upper_left",
        "projection": native.GRID_PROJECTION,
        "rows": native.GRID_ROWS,
        "xscale_metres": native.GRID_SCALE_METRES,
        "yscale_metres": native.GRID_SCALE_METRES,
    }


def _artifact_processing_metadata(frame: _DecodedFrame) -> dict[str, Any]:
    return {
        "software": frame.software,
        "software_version": frame.software_version,
        "source_nodes": list(frame.source_nodes),
        "source_wmo_ids": list(frame.source_wmo_ids),
        "task": frame.task,
        "zr_a": frame.zr_a,
        "zr_b": frame.zr_b,
    }


def _artifact_metadata_core(
    source: _VerifiedSource,
    frame: _DecodedFrame,
) -> dict[str, Any]:
    return {
        "capture": _artifact_capture_metadata(source.capture),
        "decoder": {
            "config_sha256": DECODER_CONFIG_SHA256,
            "identity": DECODER_IDENTITY,
            "version": DECODER_VERSION,
        },
        "grid": _artifact_grid_metadata(),
        "processing": _artifact_processing_metadata(frame),
        "product": _artifact_product_metadata(frame),
        "schema": ARTIFACT_SCHEMA,
        "source": _artifact_source_metadata(source),
    }


def _artifact_bytes(
    source: _VerifiedSource,
    frame: _DecodedFrame,
    *,
    checkpoint: Callable[[str], None],
) -> tuple[bytes, str]:
    descriptors: list[dict[str, Any]] = []
    compressed_arrays: list[bytes] = []
    for name, dtype_text in _ARTIFACT_ARRAYS:
        checkpoint(f"artifact_encode_{name}")
        array = frame.arrays.get(name)
        if not isinstance(array, np.ndarray):
            raise MetOfficeRateArtifactError(f"artifactarray {name} ontbreekt")
        dtype = np.dtype(dtype_text)
        if array.shape != (native.GRID_ROWS, native.GRID_COLUMNS):
            raise MetOfficeRateArtifactError(f"artifactarray {name} heeft verkeerde vorm")
        normalized = np.ascontiguousarray(array, dtype=dtype)
        raw = normalized.tobytes(order="C")
        if not 0 < len(raw) <= MAX_UNCOMPRESSED_ARRAY_BYTES:
            raise MetOfficeRateArtifactCapacityError(
                f"artifactarray {name} overschrijdt bytegrens"
            )
        compressed = zlib.compress(raw, level=ZLIB_LEVEL)
        descriptors.append({
            "codec": f"zlib-{ZLIB_LEVEL}",
            "compressed_bytes": len(compressed),
            "compressed_sha256": hashlib.sha256(compressed).hexdigest(),
            "dtype": dtype_text,
            "name": name,
            "raw_bytes": len(raw),
            "raw_sha256": hashlib.sha256(raw).hexdigest(),
            "shape": [native.GRID_ROWS, native.GRID_COLUMNS],
        })
        compressed_arrays.append(compressed)
    metadata = {
        **_artifact_metadata_core(source, frame),
        "arrays": descriptors,
        "statistics": {
            **dict(frame.counts),
            "detected_max_mm_h": frame.detected_max_mm_h,
            "detected_min_mm_h": frame.detected_min_mm_h,
        },
    }
    header = _canonical_json(metadata)
    if not 0 < len(header) <= MAX_HEADER_BYTES:
        raise MetOfficeRateArtifactCapacityError("artifactheader overschrijdt bytegrens")
    payload = b"".join(
        (ARTIFACT_MAGIC, struct.pack(">Q", len(header)), header, *compressed_arrays)
    )
    if not 0 < len(payload) <= MAX_ARTIFACT_BYTES:
        raise MetOfficeRateArtifactCapacityError("artifact overschrijdt objectgrens")
    checkpoint("artifact_serialized")
    return payload, hashlib.sha256(header).hexdigest()


def _bounded_decompress(payload: bytes, expected_bytes: int) -> bytes:
    if (
        not isinstance(payload, bytes)
        or isinstance(expected_bytes, bool)
        or not isinstance(expected_bytes, int)
        or not 0 < expected_bytes <= MAX_UNCOMPRESSED_ARRAY_BYTES
    ):
        raise MetOfficeRateArtifactError("gecomprimeerde arraygrens is ongeldig")
    decompressor = zlib.decompressobj()
    try:
        decoded = decompressor.decompress(payload, expected_bytes + 1)
        if decompressor.unconsumed_tail:
            raise MetOfficeRateArtifactError("gecomprimeerde array overschrijdt raw grens")
        decoded += decompressor.flush()
    except zlib.error as error:
        raise MetOfficeRateArtifactError("gecomprimeerde array is ongeldig") from error
    if (
        len(decoded) != expected_bytes
        or not decompressor.eof
        or decompressor.unused_data
    ):
        raise MetOfficeRateArtifactError("gecomprimeerde arraylengte wijkt af")
    return decoded


def _artifact_exact_keys(
    value: object,
    expected: set[str],
    context: str,
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != expected:
        raise MetOfficeRateArtifactError(f"artifact {context}-schema wijkt af")
    return value


def _artifact_sha(value: object, context: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise MetOfficeRateArtifactError(f"artifact {context} is geen SHA-256")
    return value


def _artifact_arrays_valid(arrays: Mapping[str, np.ndarray]) -> bool:
    encoded = arrays["rate_encoded"]
    rate = arrays["rate_mm_h"]
    valid = arrays["rate_valid_mask"]
    nodata = arrays["rate_nodata_mask"]
    undetect = arrays["rate_undetect_mask"]
    if (
        not np.isfinite(encoded).all()
        or not np.all((valid == 0) | (valid == 1))
        or not np.all((nodata == 0) | (nodata == 1))
        or not np.all((undetect == 0) | (undetect == 1))
    ):
        return False
    expected_nodata = np.asarray(encoded == np.float32(RATE_NODATA), dtype=np.uint8)
    expected_undetect = np.asarray(encoded == np.float32(RATE_UNDETECT), dtype=np.uint8)
    expected_valid = np.asarray(
        (expected_nodata == 0) & (expected_undetect == 0), dtype=np.uint8
    )
    if (
        not np.array_equal(nodata, expected_nodata)
        or not np.array_equal(undetect, expected_undetect)
        or not np.array_equal(valid, expected_valid)
        or np.any(encoded[expected_valid == 1] < 0.0)
        or not np.all(rate[expected_valid == 0] == 0.0)
        or not np.isfinite(rate[expected_valid == 1]).all()
        or np.any(rate[expected_valid == 1] < 0.0)
    ):
        return False
    expected_rate = np.asarray(
        encoded[expected_valid == 1] * np.float32(RATE_GAIN)
        + np.float32(RATE_OFFSET),
        dtype=np.dtype("<f4"),
    )
    return np.array_equal(rate[expected_valid == 1], expected_rate)


def _parse_artifact(
    payload: bytes,
    checkpoint: Callable[[str], None],
) -> MetOfficeRateRasterArtifact:
    if not isinstance(payload, bytes) or not 0 < len(payload) <= MAX_ARTIFACT_BYTES:
        raise MetOfficeRateArtifactError("artifactpayload heeft ongeldige grootte")
    prefix_bytes = len(ARTIFACT_MAGIC) + 8
    if len(payload) < prefix_bytes or not payload.startswith(ARTIFACT_MAGIC):
        raise MetOfficeRateArtifactError("artifactmagic wijkt af")
    header_bytes = struct.unpack(">Q", payload[len(ARTIFACT_MAGIC):prefix_bytes])[0]
    if not 0 < header_bytes <= MAX_HEADER_BYTES:
        raise MetOfficeRateArtifactError("artifactheaderlengte is ongeldig")
    header_end = prefix_bytes + header_bytes
    if header_end > len(payload):
        raise MetOfficeRateArtifactError("artifactheader is afgekapt")
    header = payload[prefix_bytes:header_end]
    try:
        metadata = json.loads(header.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise MetOfficeRateArtifactError("artifactheader is geen JSON") from error
    if not isinstance(metadata, dict) or header != _canonical_json(metadata):
        raise MetOfficeRateArtifactError("artifactheader is niet canoniek")
    _artifact_exact_keys(
        metadata,
        {
            "arrays",
            "capture",
            "decoder",
            "grid",
            "processing",
            "product",
            "schema",
            "source",
            "statistics",
        },
        "root",
    )
    if metadata["schema"] != ARTIFACT_SCHEMA:
        raise MetOfficeRateArtifactError("artifactschema wijkt af")
    decoder = _artifact_exact_keys(
        metadata["decoder"], {"config_sha256", "identity", "version"}, "decoder"
    )
    if decoder != {
        "config_sha256": DECODER_CONFIG_SHA256,
        "identity": DECODER_IDENTITY,
        "version": DECODER_VERSION,
    }:
        raise MetOfficeRateArtifactError("artifactdecoder wijkt af")
    grid = _artifact_exact_keys(
        metadata["grid"],
        {
            "axis_order",
            "columns",
            "coverage",
            "coverage_definition_sha256",
            "grid_definition_sha256",
            "origin",
            "projection",
            "rows",
            "xscale_metres",
            "yscale_metres",
        },
        "grid",
    )
    if grid != _artifact_grid_metadata():
        raise MetOfficeRateArtifactError("artifactgrid wijkt af")
    product = _artifact_exact_keys(
        metadata["product"],
        {
            "gain",
            "input_window_end_timestamp",
            "input_window_is_uniform_observation_time",
            "input_window_start_timestamp",
            "interpolation_applied",
            "nodata",
            "nominal_timestamp",
            "offset",
            "operational_admission_eligible",
            "quality_control",
            "quantity",
            "reprojection_applied",
            "thresholding_applied",
            "undetect",
            "unit",
            "upstream_quantity",
            "wet_dry_classification_applied",
        },
        "product",
    )
    quality = _artifact_exact_keys(
        product["quality_control"],
        {"quality_dataset_present", "reason_code", "state"},
        "quality_control",
    )
    if (
        product["gain"] != RATE_GAIN
        or product["offset"] != RATE_OFFSET
        or product["nodata"] != RATE_NODATA
        or product["undetect"] != RATE_UNDETECT
        or product["unit"] != RATE_UNIT
        or product["upstream_quantity"] != native.ODIM_QUANTITY
        or product["quantity"] != "surface_precipitation_rate_estimate"
        or product["input_window_is_uniform_observation_time"] is not False
        or product["operational_admission_eligible"] is not False
        or product["interpolation_applied"] is not False
        or product["reprojection_applied"] is not False
        or product["thresholding_applied"] is not False
        or product["wet_dry_classification_applied"] is not False
        or quality
        != {
            "quality_dataset_present": False,
            "reason_code": QC_REASON_CODE,
            "state": QC_STATE,
        }
    ):
        raise MetOfficeRateArtifactError("artifactproductsemantiek wijkt af")
    for field in (
        "nominal_timestamp",
        "input_window_start_timestamp",
        "input_window_end_timestamp",
    ):
        value = product[field]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
            raise MetOfficeRateArtifactError("artifacttijd is ongeldig")
    if (
        float(product["input_window_start_timestamp"])
        > float(product["input_window_end_timestamp"])
        or abs(float(product["nominal_timestamp"]) - float(product["input_window_start_timestamp"])) > 900
        or abs(float(product["nominal_timestamp"]) - float(product["input_window_end_timestamp"])) > 900
    ):
        raise MetOfficeRateArtifactError("artifacttijden wijken af")
    capture = _artifact_exact_keys(
        metadata["capture"],
        {
            "capture_id",
            "capture_manifest_sha256",
            "capture_receipt_sha256",
            "fetch_id",
            "members",
            "product",
            "product_timestamp",
            "provider",
            "upstream_capture_id",
        },
        "capture",
    )
    if (
        not isinstance(capture["capture_id"], str)
        or _CAPTURE_ID_RE.fullmatch(capture["capture_id"]) is None
        or capture["provider"] != PROVIDER
        or capture["product"] != CAPTURE_PRODUCT
        or capture["product_timestamp"] != product["nominal_timestamp"]
        or _CAPTURE_FETCH_RE.fullmatch(str(capture["fetch_id"])) is None
    ):
        raise MetOfficeRateArtifactError("artifactcapture wijkt af")
    _artifact_sha(capture["capture_manifest_sha256"], "capture_manifest_sha256")
    _artifact_sha(capture["capture_receipt_sha256"], "capture_receipt_sha256")
    members = capture["members"]
    if not isinstance(members, list) or len(members) != 3:
        raise MetOfficeRateArtifactError("artifactcaptureleden wijken af")
    member_roles: set[str] = set()
    members_by_role: dict[str, Mapping[str, Any]] = {}
    for member in members:
        parsed_member = _artifact_exact_keys(
            member,
            {
                "content_sha256",
                "member_id",
                "receipt_id",
                "receipt_sha256",
                "received_at",
                "role",
            },
            "capture_member",
        )
        role = parsed_member["role"]
        if role not in set(MEMBER_ROLES) or parsed_member["member_id"] != role:
            raise MetOfficeRateArtifactError("artifactcapturelidrol wijkt af")
        member_roles.add(str(role))
        members_by_role[str(role)] = parsed_member
        _artifact_sha(parsed_member["content_sha256"], "member_content_sha256")
        _artifact_sha(parsed_member["receipt_sha256"], "member_receipt_sha256")
        if (
            not isinstance(parsed_member["receipt_id"], str)
            or _RAW_RECEIPT_RE.fullmatch(parsed_member["receipt_id"]) is None
            or isinstance(parsed_member["received_at"], bool)
            or not isinstance(parsed_member["received_at"], (int, float))
            or not math.isfinite(float(parsed_member["received_at"]))
        ):
            raise MetOfficeRateArtifactError("artifactcapturelidbinding is ongeldig")
    if member_roles != set(MEMBER_ROLES):
        raise MetOfficeRateArtifactError("artifactcaptureleden zijn niet compleet")
    source = _artifact_exact_keys(
        metadata["source"],
        {
            "bucket",
            "collector",
            "collector_version",
            "hdf5",
            "listings",
            "selected_object",
            "structure_inspector_version",
        },
        "source",
    )
    if (
        source["bucket"] != native.BUCKET_NAME
        or source["collector"] != native.RAW_COLLECTOR
        or source["collector_version"] != native.RAW_COLLECTOR_VERSION
        or source["structure_inspector_version"] != native.STRUCTURE_INSPECTOR_VERSION
    ):
        raise MetOfficeRateArtifactError("artifactbroncontract wijkt af")
    hdf = _artifact_exact_keys(
        source["hdf5"],
        {
            "content_sha256",
            "content_type",
            "raw_receipt_id",
            "raw_receipt_sha256",
            "size_bytes",
        },
        "hdf_source",
    )
    _artifact_sha(hdf["content_sha256"], "hdf_content_sha256")
    _artifact_sha(hdf["raw_receipt_sha256"], "hdf_receipt_sha256")
    if (
        not isinstance(hdf["raw_receipt_id"], str)
        or _RAW_RECEIPT_RE.fullmatch(hdf["raw_receipt_id"]) is None
        or hdf["content_type"]
        not in {"application/octet-stream", "application/x-hdf5", "binary/octet-stream"}
        or isinstance(hdf["size_bytes"], bool)
        or not isinstance(hdf["size_bytes"], int)
        or not 0 < hdf["size_bytes"] <= native.MAX_HDF5_BYTES
    ):
        raise MetOfficeRateArtifactError("artifact HDF-brongrootte wijkt af")
    selected = _artifact_exact_keys(
        source["selected_object"],
        {
            "checksum_algorithm",
            "checksum_type",
            "etag",
            "key",
            "last_modified",
            "size_bytes",
            "storage_class",
        },
        "selected_object",
    )
    if (
        not isinstance(selected["key"], str)
        or _KEY_RE.fullmatch(selected["key"]) is None
        or not isinstance(selected["etag"], str)
        or _ETAG_RE.fullmatch(selected["etag"]) is None
        or selected["size_bytes"] != hdf["size_bytes"]
        or selected["storage_class"] not in {"STANDARD", "INTELLIGENT_TIERING"}
        or (selected["checksum_algorithm"], selected["checksum_type"])
        not in {(None, None), ("SHA256", "FULL_OBJECT")}
        or isinstance(selected["last_modified"], bool)
        or not isinstance(selected["last_modified"], (int, float))
        or not math.isfinite(float(selected["last_modified"]))
    ):
        raise MetOfficeRateArtifactError("artifact geselecteerd object wijkt af")
    key_match = _KEY_RE.fullmatch(str(selected["key"]))
    assert key_match is not None
    try:
        selected_time = datetime.strptime(
            key_match.group("stamp"), "%Y%m%d%H%M"
        ).replace(tzinfo=timezone.utc).timestamp()
    except ValueError as error:
        raise MetOfficeRateArtifactError(
            "artifact geselecteerd object bevat ongeldige tijd"
        ) from error
    if selected_time != capture["product_timestamp"]:
        raise MetOfficeRateArtifactError("artifact key- en capturetijd verschillen")
    listings = _artifact_exact_keys(
        source["listings"], {"listing_current", "listing_previous"}, "listings"
    )
    for role in ("listing_current", "listing_previous"):
        listing = _artifact_exact_keys(
            listings[role],
            {
                "content_sha256",
                "raw_receipt_id",
                "raw_receipt_sha256",
                "size_bytes",
            },
            role,
        )
        _artifact_sha(listing["content_sha256"], f"{role}_content_sha256")
        _artifact_sha(listing["raw_receipt_sha256"], f"{role}_receipt_sha256")
        if (
            not isinstance(listing["raw_receipt_id"], str)
            or _RAW_RECEIPT_RE.fullmatch(listing["raw_receipt_id"]) is None
            or isinstance(listing["size_bytes"], bool)
            or not isinstance(listing["size_bytes"], int)
            or not 0 < listing["size_bytes"] <= native.MAX_LISTING_BYTES
        ):
            raise MetOfficeRateArtifactError("artifact listingbron wijkt af")
    source_bindings = {
        "listing_current": listings["listing_current"],
        "listing_previous": listings["listing_previous"],
        "odim_hdf5": hdf,
    }
    for role, source_binding in source_bindings.items():
        member_binding = members_by_role[role]
        if (
            member_binding["content_sha256"] != source_binding["content_sha256"]
            or member_binding["receipt_id"] != source_binding["raw_receipt_id"]
            or member_binding["receipt_sha256"]
            != source_binding["raw_receipt_sha256"]
        ):
            raise MetOfficeRateArtifactError(
                "artifact capturelid en bronbinding verschillen"
            )
    expected_upstream_capture_id = (
        f"{CAPTURE_PRODUCT}|key={selected['key']}|etag={selected['etag']}"
        f"|previous={listings['listing_previous']['content_sha256']}"
        f"|current={listings['listing_current']['content_sha256']}"
        f"|hdf5={hdf['content_sha256']}"
    )
    if capture["upstream_capture_id"] != expected_upstream_capture_id:
        raise MetOfficeRateArtifactError("artifact upstream-capturebinding wijkt af")
    processing = _artifact_exact_keys(
        metadata["processing"],
        {
            "software",
            "software_version",
            "source_nodes",
            "source_wmo_ids",
            "task",
            "zr_a",
            "zr_b",
        },
        "processing",
    )
    if (
        not isinstance(processing["source_nodes"], list)
        or not isinstance(processing["source_wmo_ids"], list)
        or not 1 <= len(processing["source_nodes"]) <= 64
        or not 1 <= len(processing["source_wmo_ids"]) <= 64
        or any(not isinstance(value, str) or _NODE_RE.fullmatch(value) is None for value in processing["source_nodes"])
        or any(not isinstance(value, str) or _WMO_RE.fullmatch(value) is None for value in processing["source_wmo_ids"])
        or len(set(processing["source_nodes"])) != len(processing["source_nodes"])
        or len(set(processing["source_wmo_ids"])) != len(processing["source_wmo_ids"])
        or any(
            not isinstance(processing[field], str)
            or not processing[field]
            or len(processing[field]) > 8_192
            for field in ("software", "software_version", "task")
        )
        or any(
            isinstance(processing[field], bool)
            or not isinstance(processing[field], (int, float))
            or not math.isfinite(float(processing[field]))
            for field in ("zr_a", "zr_b")
        )
    ):
        raise MetOfficeRateArtifactError("artifact processingbronnen wijken af")
    descriptors = metadata["arrays"]
    if not isinstance(descriptors, list) or len(descriptors) != len(_ARTIFACT_ARRAYS):
        raise MetOfficeRateArtifactError("artifactarraycatalogus wijkt af")
    cursor = header_end
    arrays: dict[str, np.ndarray] = {}
    for descriptor, (expected_name, expected_dtype) in zip(
        descriptors, _ARTIFACT_ARRAYS, strict=True
    ):
        checkpoint(f"artifact_parse_{expected_name}")
        item = _artifact_exact_keys(
            descriptor,
            {
                "codec",
                "compressed_bytes",
                "compressed_sha256",
                "dtype",
                "name",
                "raw_bytes",
                "raw_sha256",
                "shape",
            },
            "array_descriptor",
        )
        dtype = np.dtype(expected_dtype)
        raw_bytes = native.GRID_ROWS * native.GRID_COLUMNS * dtype.itemsize
        compressed_bytes = item["compressed_bytes"]
        if (
            item["name"] != expected_name
            or item["dtype"] != expected_dtype
            or item["codec"] != f"zlib-{ZLIB_LEVEL}"
            or item["shape"] != [native.GRID_ROWS, native.GRID_COLUMNS]
            or item["raw_bytes"] != raw_bytes
            or isinstance(compressed_bytes, bool)
            or not isinstance(compressed_bytes, int)
            or not 0 < compressed_bytes <= MAX_ARTIFACT_BYTES
        ):
            raise MetOfficeRateArtifactError("artifactarraydescriptor wijkt af")
        end = cursor + compressed_bytes
        if end > len(payload):
            raise MetOfficeRateArtifactError("artifactarray is afgekapt")
        compressed = payload[cursor:end]
        if not hmac.compare_digest(
            hashlib.sha256(compressed).hexdigest(),
            _artifact_sha(item["compressed_sha256"], "compressed_sha256"),
        ):
            raise MetOfficeRateArtifactError("artifactarraycompressiehash wijkt af")
        raw = _bounded_decompress(compressed, raw_bytes)
        if not hmac.compare_digest(
            hashlib.sha256(raw).hexdigest(),
            _artifact_sha(item["raw_sha256"], "raw_sha256"),
        ):
            raise MetOfficeRateArtifactError("artifactarray raw hash wijkt af")
        array = np.frombuffer(raw, dtype=dtype).reshape(
            (native.GRID_ROWS, native.GRID_COLUMNS)
        )
        array.flags.writeable = False
        arrays[expected_name] = array
        cursor = end
    if cursor != len(payload):
        raise MetOfficeRateArtifactError("artifact bevat bytes na laatste array")
    if not _artifact_arrays_valid(arrays):
        raise MetOfficeRateArtifactError("artifactmaskers/kalibratie wijken af")
    statistics = _artifact_exact_keys(
        metadata["statistics"],
        {
            "detected_max_mm_h",
            "detected_min_mm_h",
            "pixels_nodata",
            "pixels_total",
            "pixels_undetect",
            "pixels_valid",
        },
        "statistics",
    )
    valid_values = arrays["rate_mm_h"][arrays["rate_valid_mask"] == 1]
    expected_statistics = {
        "detected_max_mm_h": None if valid_values.size == 0 else float(valid_values.max()),
        "detected_min_mm_h": None if valid_values.size == 0 else float(valid_values.min()),
        "pixels_nodata": int(np.count_nonzero(arrays["rate_nodata_mask"])),
        "pixels_total": int(arrays["rate_encoded"].size),
        "pixels_undetect": int(np.count_nonzero(arrays["rate_undetect_mask"])),
        "pixels_valid": int(np.count_nonzero(arrays["rate_valid_mask"])),
    }
    if statistics != expected_statistics:
        raise MetOfficeRateArtifactError("artifactstatistieken wijken af")
    checkpoint("artifact_parse_complete")
    return MetOfficeRateRasterArtifact(metadata=metadata, **arrays)


def _run_mapping(
    capture: RawArtifactCaptureReceipt,
    *,
    decoder_job_id: str,
    attempt_id: str,
    evaluated_at: float,
    outcome: str,
    reason_code: str,
    artifact: MetOfficeRateArtifactRef | None = None,
) -> dict[str, Any]:
    complete = outcome == "decoded_complete"
    if complete != (artifact is not None):
        raise RuntimeError("decoderresultaat schendt fail-closed invariant")
    native_grid = None
    coverage = None
    if complete:
        native_grid = {
            "axis_order": "yx",
            "definition_sha256": GRID_DEFINITION_SHA256,
            "height": native.GRID_ROWS,
            "kind": "regular",
            "width": native.GRID_COLUMNS,
        }
        coverage = {
            "definition_sha256": COVERAGE_DEFINITION_SHA256,
            "kind": "native_bbox",
            "maximum_x": NATIVE_MAXIMUM_X_METRES,
            "maximum_y": NATIVE_MAXIMUM_Y_METRES,
            "minimum_x": NATIVE_MINIMUM_X_METRES,
            "minimum_y": NATIVE_MINIMUM_Y_METRES,
        }
    return {
        "attempt_id": attempt_id,
        "capture": {
            "capture_id": capture.capture_id,
            "capture_receipt_sha256": capture.capture_receipt_sha256,
            "members": _capture_members(capture),
        },
        "contract": EVIDENCE_CONTRACT,
        "coverage": coverage,
        "decoder": {
            "config_sha256": DECODER_CONFIG_SHA256,
            "identity": DECODER_IDENTITY,
            "version": DECODER_VERSION,
        },
        "decoder_job_id": decoder_job_id,
        "evaluated_at": evaluated_at,
        "native_crs": native.GRID_PROJECTION if complete else None,
        "native_grid": native_grid,
        "non_operational_candidate_artifact": (
            None
            if artifact is None
            else {"handle": artifact.handle, "sha256": artifact.sha256}
        ),
        "outcome": outcome,
        "product": capture.product,
        "product_time": capture.product_timestamp,
        "provider": capture.provider,
        "quantity": "surface_precipitation_rate_estimate" if complete else None,
        "reason_code": reason_code,
        "received_at": max(member.received_at for member in capture.members),
        "unit": RATE_UNIT if complete else None,
    }


def _verified_published_artifact(
    artifact_store: MetOfficeRateDerivedArtifactStore,
    artifact: MetOfficeRateArtifactRef,
    *,
    source: _VerifiedSource,
    frame: _DecodedFrame,
    expected_metadata_sha256: str,
    checkpoint: Callable[[str], None],
) -> None:
    decoded = artifact_store.read_decoded(artifact, checkpoint=checkpoint)
    if (
        hashlib.sha256(_canonical_json(decoded.metadata)).hexdigest()
        != expected_metadata_sha256
        or any(
            decoded.metadata.get(key) != value
            for key, value in _artifact_metadata_core(source, frame).items()
        )
    ):
        raise MetOfficeRateArtifactError(
            "gepubliceerd artifact verschilt van geverifieerde raw bron"
        )


def decode_met_office_rate_capture(
    *,
    capture_id: str,
    decoder_job_id: str,
    attempt_id: str,
    raw_authority: MetOfficeRateRawAuthority,
    artifact_store: MetOfficeRateDerivedArtifactStore,
    evidence_store: CanonicalEvidenceStore,
    evaluated_at: float,
    monotonic_deadline: float | None = None,
    monotonic_clock: Callable[[], float] = time.monotonic,
) -> MetOfficeRateDecodeResult:
    """Decode one complete exact capture into an unwired native RATE candidate."""

    if not isinstance(raw_authority, MetOfficeRateRawAuthority):
        raise TypeError("raw_authority heeft verkeerd type")
    if not isinstance(artifact_store, MetOfficeRateDerivedArtifactStore):
        raise TypeError("artifact_store heeft verkeerd type")
    if not isinstance(evidence_store, CanonicalEvidenceStore):
        raise TypeError("evidence_store heeft verkeerd type")
    if getattr(evidence_store, "_raw_artifact_reader", None) is not raw_authority:
        raise TypeError("evidence_store moet dezelfde single-read raw authority delen")
    if not isinstance(capture_id, str) or _CAPTURE_ID_RE.fullmatch(capture_id) is None:
        raise ValueError("capture_id is geen schema-3 capture-identiteit")
    if not isinstance(decoder_job_id, str) or _DECODER_JOB_RE.fullmatch(decoder_job_id) is None:
        raise ValueError("decoder_job_id is geen vooraf toegewezen identiteit")
    if not isinstance(attempt_id, str) or _ATTEMPT_RE.fullmatch(attempt_id) is None:
        raise ValueError("attempt_id is geen vooraf toegewezen identiteit")
    if (
        isinstance(evaluated_at, bool)
        or not isinstance(evaluated_at, (int, float))
        or not math.isfinite(float(evaluated_at))
    ):
        raise ValueError("evaluated_at moet een eindige tijd zijn")
    initial = float(monotonic_clock())
    if not math.isfinite(initial):
        raise ValueError("monotone klok moet eindig zijn")
    if monotonic_deadline is None:
        deadline = initial + DECODE_TOTAL_DEADLINE_SECONDS
    else:
        if isinstance(monotonic_deadline, bool) or not isinstance(
            monotonic_deadline, (int, float)
        ):
            raise ValueError("decoderdeadline moet eindig zijn")
        deadline = float(monotonic_deadline)
        if not math.isfinite(deadline):
            raise ValueError("decoderdeadline moet eindig zijn")

    def checkpoint(stage: str) -> None:
        current = float(monotonic_clock())
        if not math.isfinite(current) or current >= deadline:
            raise MetOfficeRateDecodeDeadlineError(
                f"Met Office RATE decoderdeadline verstreek bij {stage}"
            )

    checkpoint("decode_start")
    bundle = raw_authority.load_capture_bundle_bytes(capture_id)
    if bundle is None:
        raise MetOfficeRateCaptureMissingError(
            "schema-3 Met Office RATE capture ontbreekt"
        )
    checkpoint("raw_bundle_verified")
    capture, receipts, payloads = bundle
    if not capture.members:
        raise RawArtifactIntegrityError("Met Office capture heeft geen leden")
    received_at = max(member.received_at for member in capture.members)
    evaluation = float(evaluated_at)
    if evaluation < received_at:
        raise ValueError("evaluated_at mag raw ontvangst niet voorafgaan")

    try:
        source = _validate_raw_capture(
            capture,
            receipts=receipts,
            payloads=payloads,
            checkpoint=checkpoint,
        )
        checkpoint("raw_capture_validated")
        frame = _decode_hdf(source, checkpoint=checkpoint)
        checkpoint("hdf_decoded")
        if (
            frame.nominal_time != capture.product_timestamp
            or frame.nominal_time != source.selected.product_timestamp
        ):
            raise _RejectedDecode("unsupported", "unsupported:time_contract")
        artifact_payload, metadata_sha256 = _artifact_bytes(
            source, frame, checkpoint=checkpoint
        )
        artifact = artifact_store.publish(artifact_payload, checkpoint=checkpoint)
        checkpoint("artifact_published")
        _verified_published_artifact(
            artifact_store,
            artifact,
            source=source,
            frame=frame,
            expected_metadata_sha256=metadata_sha256,
            checkpoint=checkpoint,
        )
        checkpoint("canonical_success_before")
        evidence = evidence_store.commit(
            _run_mapping(
                capture,
                decoder_job_id=decoder_job_id,
                attempt_id=attempt_id,
                evaluated_at=evaluation,
                outcome="decoded_complete",
                reason_code="decoded_complete",
                artifact=artifact,
            )
        )
        checkpoint("decode_complete")
        return MetOfficeRateDecodeResult(
            outcome="decoded_complete",
            reason_code="decoded_complete",
            evidence_receipt=evidence,
            artifact=artifact,
        )
    except _RejectedDecode as rejected:
        checkpoint("canonical_rejection_before")
        evidence = evidence_store.commit(
            _run_mapping(
                capture,
                decoder_job_id=decoder_job_id,
                attempt_id=attempt_id,
                evaluated_at=evaluation,
                outcome=rejected.outcome,
                reason_code=rejected.reason_code,
            )
        )
        checkpoint("decode_rejection_complete")
        return MetOfficeRateDecodeResult(
            outcome=rejected.outcome,
            reason_code=rejected.reason_code,
            evidence_receipt=evidence,
            artifact=None,
        )


__all__ = [
    "ARTIFACT_SCHEMA",
    "COVERAGE_DEFINITION_SHA256",
    "DECODER_CONFIG_SHA256",
    "DECODER_IDENTITY",
    "DECODER_VERSION",
    "GRID_DEFINITION_SHA256",
    "MetOfficeRateArtifactCapacityError",
    "MetOfficeRateArtifactError",
    "MetOfficeRateArtifactRef",
    "MetOfficeRateCaptureMissingError",
    "MetOfficeRateDecodeDeadlineError",
    "MetOfficeRateDecodeResult",
    "MetOfficeRateDerivedArtifactStore",
    "MetOfficeRateQuantitativeError",
    "MetOfficeRateRasterArtifact",
    "MetOfficeRateRawAuthority",
    "decode_met_office_rate_capture",
    "open_met_office_rate_evidence_store",
]
