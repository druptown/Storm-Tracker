"""Fail-closed quantitative decoder for one exact KNMI RTCOR capture.

This module is intentionally not registered with the worker service.  It consumes
one schema-3 ``knmi_nl_rdr_data_rtcor_5m_capture`` containing exactly one
``hdf5`` role, verifies its raw authority, decodes the three product-native
rasters, publishes an immutable content-addressed candidate, verifies that
candidate, and only then appends canonical ``decoded_complete`` evidence.

This boundary makes no rain/dry decision, applies no threshold, extracts no
cells, invents no quality, and does not apply the delivered adjustment field a
second time.  A rejected decode exposes neither an artifact nor meteorological
fields.
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
import zlib

import h5py
import numpy as np

from .canonical_evidence_store import (
    CanonicalEvidenceStore,
    DecoderRunReceipt,
    EVIDENCE_CONTRACT,
)
from .knmi_native import (
    KNMI_CAPTURE_PRODUCT,
    KNMI_DATASET_LICENSE,
    KNMI_DATASET_NAME,
    KNMI_DATASET_VERSION,
    KNMI_HDF5_PRODUCT,
    KNMI_INTERNAL_PRODUCT_NAME,
    KNMI_RAW_COLLECTOR,
    KNMI_RAW_COLLECTOR_VERSION,
    KNMI_STRUCTURE_INSPECTOR_VERSION,
)
from .raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RawArtifactCaptureReceipt,
    RawArtifactIntegrityError,
    RawArtifactReceipt,
    RawArtifactStore,
)


KNMI_PROVIDER = "knmi"
KNMI_ROLE = "hdf5"
SUPPORTED_HDF_TAG = "3.6"
DECODER_IDENTITY = "knmi-nl25-rtcor-hdf5-quantitative"
DECODER_VERSION = "knmi-rtcor-quantitative-v1"
ARTIFACT_SCHEMA = "stv3-worker-knmi-rtcor-quantitative-raster-v1"
ARTIFACT_MAGIC = b"STV3-KNMI-RTCOR-Q1\n"
ARTIFACT_HANDLE_PREFIX = "artifact:knmi-rtcor-quantitative:"

EXPECTED_ROWS = 765
EXPECTED_COLUMNS = 700
EXPECTED_ACCUMULATION_SECONDS = 300
EXPECTED_PROJECTION = (
    "+proj=stere +x_0=0 +y_0=0 +lat_0=90 +lon_0=0 "
    "+lat_ts=60 +a=6378.388 +b=6356.906"
)
EXPECTED_CORNERS = (
    0.0, 49.362064,
    0.0, 55.973602,
    10.856453, 55.388973,
    9.009301, 48.895306,
)
EXPECTED_COLUMN_OFFSET_KM = 0.0
EXPECTED_ROW_OFFSET_KM = 3649.9814
EXPECTED_PIXEL_SIZE_X_KM = 1.0000035
EXPECTED_PIXEL_SIZE_Y_KM = -1.0000049

PRECIP_PARAMETER = "PRECIP_[MM]"
PRECIP_FORMULA = "GEO=0.010000*PV+0"
PRECIP_GAIN = 0.01
PRECIP_OFFSET = 0.0
PRECIP_MISSING = 65534
PRECIP_OUT_OF_IMAGE = 65535

QUALITY_PARAMETER = "QUALITY_[-]"
QUALITY_FORMULA = "GEO=0.004*PV+0"
QUALITY_GAIN = 0.004
QUALITY_OFFSET = 0.0
QUALITY_MISSING = 254
QUALITY_OUT_OF_IMAGE = 255

ADJUSTMENT_PARAMETER = "ADJUSTMENT_FACTOR_[DB]"
ADJUSTMENT_FORMULA = "GEO=0.16*PV-20.24"
ADJUSTMENT_GAIN = 0.16
ADJUSTMENT_OFFSET = -20.24
ADJUSTMENT_MISSING = 254
ADJUSTMENT_OUT_OF_IMAGE = 255

PRECIPITATION_UPSTREAM_ADJUSTMENT_STATE = "gauge_adjusted_by_knmi_before_delivery"
ADJUSTMENT_REAPPLICATION_POLICY = "never_reapply_image1_already_gauge_adjusted"

MAX_HDF5_BYTES = 40 * 1024 * 1024
MAX_HDF_OBJECTS = 4096
MAX_HEADER_BYTES = 128 * 1024
MAX_UNCOMPRESSED_ARRAY_BYTES = 32 * 1024 * 1024
MAX_ARTIFACT_BYTES = 32 * 1024 * 1024
DEFAULT_MAX_DERIVED_BYTES = 2 * 1024 * 1024 * 1024
DEFAULT_MAX_DERIVED_OBJECTS = 4096
DEFAULT_MAX_STAGING_OBJECTS = 8
DECODE_TOTAL_DEADLINE_SECONDS = 60.0
LOCK_POLL_SECONDS = 0.01
STORE_PROFILE_FILENAME = ".store-profile.json"
STORE_PROFILE_SCHEMA = "stv3-worker-knmi-rtcor-derived-store-profile-v1"

_RTCOR_FILENAME_RE = re.compile(
    r"^RAD_NL25_RAC_RT_(?P<timestamp>\d{12})\.h5$"
)
_HDF_TIMESTAMP_RE = re.compile(
    r"^(?P<day>\d{2})-(?P<month>[A-Z]{3})-(?P<year>\d{4});"
    r"(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})"
    r"(?:\.(?P<fraction>\d{1,6}))?$"
)
_MONTHS = {
    month.upper(): index
    for index, month in enumerate(
        ("", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
    )
    if month
}
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_HANDLE_RE = re.compile(
    rf"^{re.escape(ARTIFACT_HANDLE_PREFIX)}(?P<sha>[0-9a-f]{{64}})$"
)
_ARTIFACT_FILENAME_RE = re.compile(r"^(?P<sha>[0-9a-f]{64})\.krtq$")
_STAGING_FILENAME_RE = re.compile(
    r"^\.(?P<sha>[0-9a-f]{64})\.[A-Za-z0-9_-]{1,128}$"
)
_DECODER_JOB_RE = re.compile(r"^decoder-job-[0-9a-f]{64}$")
_ATTEMPT_RE = re.compile(r"^decoder-attempt-[0-9a-f]{64}$")
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
_STORE_THREAD_LOCKS: dict[str, threading.RLock] = {}


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


_DECODER_CONFIG = {
    "accumulation_seconds": EXPECTED_ACCUMULATION_SECONDS,
    "artifact_schema": ARTIFACT_SCHEMA,
    "axis_order": "yx",
    "capture_product": KNMI_CAPTURE_PRODUCT,
    "decoder_identity": DECODER_IDENTITY,
    "decoder_version": DECODER_VERSION,
    "grid": {
        "columns": EXPECTED_COLUMNS,
        "pixel_definition": "LU",
        "pixel_size_x_km": EXPECTED_PIXEL_SIZE_X_KM,
        "pixel_size_y_km": EXPECTED_PIXEL_SIZE_Y_KM,
        "rows": EXPECTED_ROWS,
    },
    "layers": (
        (PRECIP_PARAMETER, PRECIP_FORMULA, PRECIP_MISSING, PRECIP_OUT_OF_IMAGE),
        (QUALITY_PARAMETER, QUALITY_FORMULA, QUALITY_MISSING, QUALITY_OUT_OF_IMAGE),
        (ADJUSTMENT_PARAMETER, ADJUSTMENT_FORMULA, ADJUSTMENT_MISSING, ADJUSTMENT_OUT_OF_IMAGE),
    ),
    "precipitation_upstream_adjustment_state": PRECIPITATION_UPSTREAM_ADJUSTMENT_STATE,
    "adjustment_reapplication_policy": ADJUSTMENT_REAPPLICATION_POLICY,
    "member_role": KNMI_ROLE,
    "product": KNMI_INTERNAL_PRODUCT_NAME,
    "provider": KNMI_PROVIDER,
    "resource_contract": {
        "cooperative_deadline_seconds": DECODE_TOTAL_DEADLINE_SECONDS,
        "hard_c_or_rss_isolation": False,
        "max_artifact_bytes": MAX_ARTIFACT_BYTES,
        "max_artifact_objects": DEFAULT_MAX_DERIVED_OBJECTS,
        "max_staging_objects": DEFAULT_MAX_STAGING_OBJECTS,
        "max_uncompressed_array_bytes": MAX_UNCOMPRESSED_ARRAY_BYTES,
    },
    "supported_hdf_tag": SUPPORTED_HDF_TAG,
}
DECODER_CONFIG_SHA256 = hashlib.sha256(_canonical_json(_DECODER_CONFIG)).hexdigest()


class KnmiRtcorQuantitativeError(RuntimeError):
    """Base failure for this isolated candidate boundary."""


class KnmiRtcorCaptureMissingError(KnmiRtcorQuantitativeError):
    """The requested schema-3 KNMI capture does not exist."""


class KnmiRtcorArtifactError(KnmiRtcorQuantitativeError):
    """The immutable derived store rejected an object."""


class KnmiRtcorArtifactCapacityError(KnmiRtcorArtifactError):
    """Publishing would exceed an object or total byte bound."""


class KnmiRtcorDecodeDeadlineError(KnmiRtcorQuantitativeError):
    """The cooperative decoder deadline expired at a bounded checkpoint."""


class _RejectedDecode(Exception):
    def __init__(self, outcome: str, reason_code: str) -> None:
        self.outcome = outcome
        self.reason_code = reason_code
        super().__init__(reason_code)


@dataclass(frozen=True, slots=True)
class KnmiRtcorArtifactRef:
    handle: str
    sha256: str
    size_bytes: int


@dataclass(frozen=True, slots=True)
class KnmiRtcorRasterArtifact:
    metadata: Mapping[str, Any]
    precipitation_raw: np.ndarray
    precipitation_mm: np.ndarray
    precipitation_valid_mask: np.ndarray
    precipitation_missing_mask: np.ndarray
    precipitation_out_of_image_mask: np.ndarray
    quality_raw: np.ndarray
    quality_index: np.ndarray
    quality_valid_mask: np.ndarray
    quality_missing_mask: np.ndarray
    quality_out_of_image_mask: np.ndarray
    adjustment_raw: np.ndarray
    adjustment_db: np.ndarray
    adjustment_valid_mask: np.ndarray
    adjustment_missing_mask: np.ndarray
    adjustment_out_of_image_mask: np.ndarray


@dataclass(frozen=True, slots=True)
class KnmiRtcorDecodeResult:
    outcome: str
    reason_code: str
    evidence_receipt: DecoderRunReceipt
    artifact: KnmiRtcorArtifactRef | None


@dataclass(frozen=True, slots=True)
class _DecodedFrame:
    arrays: Mapping[str, np.ndarray]
    product_start: float
    product_end: float
    bbox_km: tuple[float, float, float, float]
    grid_definition_sha256: str
    coverage_definition_sha256: str


def _thread_store_lock(root: Path) -> threading.RLock:
    identity = os.path.normcase(str(root.resolve()))
    with _STORE_LOCKS_GUARD:
        lock = _STORE_THREAD_LOCKS.get(identity)
        if lock is None:
            lock = threading.RLock()
            _STORE_THREAD_LOCKS[identity] = lock
        return lock


@contextmanager
def _exclusive_store_lock(
    root: Path,
    *,
    checkpoint: Callable[[str], None] | None = None,
):
    """Serialize store mutations without waiting beyond the active deadline."""

    if checkpoint is None:
        initial_monotonic = time.monotonic()
        if not math.isfinite(initial_monotonic):
            raise KnmiRtcorDecodeDeadlineError(
                "KNMI RTCOR decoder clock is invalid before artifact store lock"
            )
        default_deadline = initial_monotonic + DECODE_TOTAL_DEADLINE_SECONDS

        def active_checkpoint(stage: str) -> None:
            current = time.monotonic()
            if not math.isfinite(current) or current >= default_deadline:
                raise KnmiRtcorDecodeDeadlineError(
                    f"KNMI RTCOR decoder deadline expired at {stage}"
                )
    elif callable(checkpoint):
        active_checkpoint = checkpoint
    else:
        raise TypeError("derived artifact lock checkpoint must be callable")

    thread_lock = _thread_store_lock(root)
    thread_acquired = False
    descriptor: int | None = None
    file_acquired = False
    try:
        while not thread_acquired:
            active_checkpoint("artifact_store_thread_lock_wait")
            thread_acquired = thread_lock.acquire(blocking=False)
            if not thread_acquired:
                time.sleep(LOCK_POLL_SECONDS)

        lock_path = root / ".store.lock"
        flags = (
            os.O_CREAT
            | os.O_RDWR
            | getattr(os, "O_BINARY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        try:
            descriptor = os.open(lock_path, flags, 0o600)
            opened = os.fstat(descriptor)
            current = lock_path.lstat()
            if (
                lock_path.is_symlink()
                or not stat.S_ISREG(opened.st_mode)
                or not stat.S_ISREG(current.st_mode)
                or (opened.st_dev, opened.st_ino)
                != (current.st_dev, current.st_ino)
            ):
                raise KnmiRtcorArtifactError("derived artifact store lock is unsafe")
            if opened.st_size == 0:
                os.ftruncate(descriptor, 1)
                os.fsync(descriptor)
            while not file_acquired:
                active_checkpoint("artifact_store_process_lock_wait")
                os.lseek(descriptor, 0, os.SEEK_SET)
                try:
                    if os.name == "nt":  # pragma: no cover - platform selected
                        import msvcrt

                        msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
                    else:  # pragma: no branch - platform selected
                        import fcntl

                        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    file_acquired = True
                except OSError as exc:
                    if exc.errno not in {
                        errno.EACCES,
                        errno.EAGAIN,
                        errno.EDEADLK,
                        errno.EWOULDBLOCK,
                    }:
                        raise
                    time.sleep(LOCK_POLL_SECONDS)
            active_checkpoint("artifact_store_process_lock_acquired")
        except (KnmiRtcorArtifactError, KnmiRtcorDecodeDeadlineError, TimeoutError):
            raise
        except OSError as exc:
            raise KnmiRtcorArtifactError("derived artifact store lock failed") from exc
        yield
    finally:
        if descriptor is not None:
            try:
                if file_acquired:
                    os.lseek(descriptor, 0, os.SEEK_SET)
                    if os.name == "nt":  # pragma: no cover - platform selected
                        import msvcrt

                        msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
                    else:  # pragma: no branch - platform selected
                        import fcntl

                        fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)
        if thread_acquired:
            thread_lock.release()


class KnmiRtcorDerivedArtifactStore:
    """Bounded immutable object store for deterministic native RTCOR rasters."""

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
            raise ValueError("derived artifact root must be absolute")
        if (
            isinstance(max_total_bytes, bool)
            or not isinstance(max_total_bytes, int)
            or max_total_bytes < MAX_ARTIFACT_BYTES
        ):
            raise ValueError("derived artifact total bound is invalid")
        if (
            isinstance(max_objects, bool)
            or not isinstance(max_objects, int)
            or max_objects < 1
        ):
            raise ValueError("derived artifact object bound is invalid")
        if (
            isinstance(max_staging_objects, bool)
            or not isinstance(max_staging_objects, int)
            or max_staging_objects < 1
        ):
            raise ValueError("derived artifact staging bound is invalid")
        self.root = supplied
        self.objects_path = supplied / "objects"
        self.staging_path = supplied / "staging"
        self.profile_path = supplied / STORE_PROFILE_FILENAME
        self.max_total_bytes = max_total_bytes
        self.max_objects = max_objects
        self.max_staging_objects = max_staging_objects
        self._fault_injector = fault_injector
        self.initialize()

    def initialize(self) -> None:
        root_existed = self.root.exists()
        objects_existed = self.objects_path.exists()
        staging_existed = self.staging_path.exists()
        if root_existed or self.root.is_symlink():
            root_info = self.root.lstat()
            if self.root.is_symlink() or not stat.S_ISDIR(root_info.st_mode):
                raise KnmiRtcorArtifactError("derived artifact layout is unsafe")
        for path in (self.objects_path, self.staging_path):
            if path.exists() or path.is_symlink():
                info = path.lstat()
                if path.is_symlink() or not stat.S_ISDIR(info.st_mode):
                    raise KnmiRtcorArtifactError("derived artifact layout is unsafe")
        self.root.mkdir(parents=True, exist_ok=True)
        self.objects_path.mkdir(mode=0o700, exist_ok=True)
        self.staging_path.mkdir(mode=0o700, exist_ok=True)
        for path in (self.root, self.objects_path, self.staging_path):
            info = path.lstat()
            if path.is_symlink() or not stat.S_ISDIR(info.st_mode):
                raise KnmiRtcorArtifactError("derived artifact layout is unsafe")

        if not objects_existed or not staging_existed:
            self._fsync_directory(self.root)
        if not root_existed:
            self._fsync_directory(self.root.parent)
        if self.profile_path.exists() or self.profile_path.is_symlink():
            self._validate_store_profile()
        else:
            with _exclusive_store_lock(self.root):
                self._ensure_store_profile()

    def _store_profile_bytes(self) -> bytes:
        return _canonical_json({
            "artifact_schema": ARTIFACT_SCHEMA,
            "filename_suffix": ".krtq",
            "limits": {
                "max_object_bytes": MAX_ARTIFACT_BYTES,
                "max_objects": self.max_objects,
                "max_staging_objects": self.max_staging_objects,
                "max_total_bytes": self.max_total_bytes,
            },
            "schema": STORE_PROFILE_SCHEMA,
        })

    def _ensure_store_profile(self) -> None:
        expected = self._store_profile_bytes()
        if self.profile_path.exists() or self.profile_path.is_symlink():
            self._validate_store_profile(expected=expected)
            return
        total, _object_count, staging_count = self._scan_layout()
        if staging_count >= self.max_staging_objects:
            raise KnmiRtcorArtifactCapacityError(
                "derived artifact staging count blocks store profile publication"
            )
        if total + len(expected) > self.max_total_bytes:
            raise KnmiRtcorArtifactCapacityError(
                "derived artifact bytes block store profile publication"
            )
        digest = hashlib.sha256(expected).hexdigest()
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                dir=self.staging_path,
                prefix=f".{digest}.",
                delete=False,
            ) as temporary:
                temporary.write(expected)
                temporary.flush()
                os.fsync(temporary.fileno())
                temporary_path = Path(temporary.name)
            self._fsync_directory(self.staging_path)
            self._fault("profile.after_stage")
            try:
                os.link(temporary_path, self.profile_path)
            except FileExistsError:
                pass
            self._fsync_directory(self.root)
            self._fault("profile.after_link")
            self._validate_store_profile(expected=expected)
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)
                self._fsync_directory(self.staging_path)

    def _validate_store_profile(self, *, expected: bytes | None = None) -> None:
        expected = self._store_profile_bytes() if expected is None else expected
        try:
            actual = self._verify_regular(
                self.profile_path,
                expected_size=len(expected),
                expected_sha256=hashlib.sha256(expected).hexdigest(),
                allowed_parent=self.root,
                return_bytes=True,
            )
        except (KnmiRtcorArtifactError, FileNotFoundError, OSError) as exc:
            raise KnmiRtcorArtifactError(
                "derived artifact store profile is missing, unsafe, or differs "
                "from the immutable limits"
            ) from exc
        if actual != expected:
            raise KnmiRtcorArtifactError(
                "derived artifact store was opened with a different immutable profile"
            )

    def publish(
        self,
        payload: bytes,
        *,
        checkpoint: Callable[[str], None] | None = None,
    ) -> KnmiRtcorArtifactRef:
        if not isinstance(payload, bytes):
            raise TypeError("derived artifact payload must be exact bytes")
        if not 0 < len(payload) <= MAX_ARTIFACT_BYTES:
            raise KnmiRtcorArtifactCapacityError("derived artifact exceeds object bound")
        digest = hashlib.sha256(payload).hexdigest()
        reference = KnmiRtcorArtifactRef(
            handle=f"{ARTIFACT_HANDLE_PREFIX}{digest}",
            sha256=digest,
            size_bytes=len(payload),
        )
        with _exclusive_store_lock(self.root, checkpoint=checkpoint):
            self._validate_store_profile()
            return self._publish_locked(payload, reference)

    def _publish_locked(
        self,
        payload: bytes,
        reference: KnmiRtcorArtifactRef,
    ) -> KnmiRtcorArtifactRef:
        digest = reference.sha256
        target = self.objects_path / f"{digest}.krtq"
        total, object_count, staging_count = self._scan_layout()
        if total > self.max_total_bytes:
            raise KnmiRtcorArtifactCapacityError(
                "derived artifact store capacity is already exceeded"
            )
        if target.exists():
            self._verify_path(target, reference)
            return reference
        if object_count >= self.max_objects:
            raise KnmiRtcorArtifactCapacityError(
                "derived artifact object count would be exceeded"
            )
        if staging_count >= self.max_staging_objects:
            raise KnmiRtcorArtifactCapacityError(
                "derived artifact staging count would be exceeded"
            )
        if total + len(payload) > self.max_total_bytes:
            raise KnmiRtcorArtifactCapacityError("derived artifact store capacity exceeded")
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                dir=self.staging_path,
                prefix=f".{digest}.",
                delete=False,
            ) as temporary:
                temporary.write(payload)
                temporary.flush()
                os.fsync(temporary.fileno())
                temporary_path = Path(temporary.name)
            self._fsync_directory(self.staging_path)
            self._fault("publish.after_stage")
            self._verify_regular(
                temporary_path,
                expected_size=len(payload),
                expected_sha256=digest,
                allowed_parent=self.staging_path,
            )
            try:
                os.link(temporary_path, target)
            except FileExistsError:
                pass
            self._fault("publish.after_link")
            self._verify_path(target, reference)
            self._fsync_directory(self.objects_path)
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)
                self._fsync_directory(self.staging_path)
        return reference

    def read(self, reference: KnmiRtcorArtifactRef) -> bytes:
        if not isinstance(reference, KnmiRtcorArtifactRef):
            raise TypeError("artifact reference has wrong type")
        match = _HANDLE_RE.fullmatch(reference.handle)
        if (
            match is None
            or match.group("sha") != reference.sha256
            or not _SHA256_RE.fullmatch(reference.sha256)
        ):
            raise KnmiRtcorArtifactError("artifact reference is invalid")
        self._validate_store_profile()
        return self._verify_path(
            self.objects_path / f"{reference.sha256}.krtq",
            reference,
            return_bytes=True,
        )

    def read_decoded(self, reference: KnmiRtcorArtifactRef) -> KnmiRtcorRasterArtifact:
        return _parse_artifact(self.read(reference))

    def object_count(self) -> int:
        with _exclusive_store_lock(self.root):
            self._validate_store_profile()
            _total, object_count, _staging_count = self._scan_layout()
            return object_count

    def _verify_path(
        self,
        path: Path,
        reference: KnmiRtcorArtifactRef,
        *,
        return_bytes: bool = False,
    ) -> bytes:
        result = self._verify_regular(
            path,
            expected_size=reference.size_bytes,
            expected_sha256=reference.sha256,
            allowed_parent=self.objects_path,
            return_bytes=return_bytes,
        )
        return result or b""

    @staticmethod
    def _verify_regular(
        path: Path,
        *,
        expected_size: int,
        expected_sha256: str,
        allowed_parent: Path,
        return_bytes: bool = False,
    ) -> bytes | None:
        if path.parent.resolve() != allowed_parent.resolve():
            raise KnmiRtcorArtifactError("artifact path escapes store")
        try:
            info = path.lstat()
        except OSError as exc:
            raise KnmiRtcorArtifactError("artifact object is missing") from exc
        if path.is_symlink() or not stat.S_ISREG(info.st_mode):
            raise KnmiRtcorArtifactError("artifact object is not regular")
        if info.st_size != expected_size or not 0 < expected_size <= MAX_ARTIFACT_BYTES:
            raise KnmiRtcorArtifactError("artifact object size changed")
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
        chunks: list[bytes] | None = [] if return_bytes else None
        digest = hashlib.sha256()
        total = 0
        try:
            opened = os.fstat(descriptor)
            if not stat.S_ISREG(opened.st_mode) or opened.st_size != expected_size:
                raise KnmiRtcorArtifactError("artifact changed while opening")
            while total < expected_size:
                chunk = os.read(descriptor, min(1024 * 1024, expected_size - total))
                if not chunk:
                    break
                total += len(chunk)
                digest.update(chunk)
                if chunks is not None:
                    chunks.append(chunk)
            if os.read(descriptor, 1):
                raise KnmiRtcorArtifactError("artifact contains trailing bytes")
        finally:
            os.close(descriptor)
        if total != expected_size or not hmac.compare_digest(digest.hexdigest(), expected_sha256):
            raise KnmiRtcorArtifactError("artifact checksum does not match")
        after = path.lstat()
        if after.st_size != opened.st_size or (after.st_dev, after.st_ino) != (opened.st_dev, opened.st_ino):
            raise KnmiRtcorArtifactError("artifact path changed while reading")
        return b"".join(chunks) if chunks is not None else None

    def _stored_bytes(self) -> int:
        with _exclusive_store_lock(self.root):
            self._validate_store_profile()
            total, _object_count, _staging_count = self._scan_layout()
            return total

    def _scan_layout(self) -> tuple[int, int, int]:
        total = 0
        seen: set[tuple[int, int] | tuple[str, str]] = set()
        object_count = 0
        staging_count = 0
        layouts = (
            (
                self.objects_path,
                _ARTIFACT_FILENAME_RE,
                self.max_objects,
                "object",
            ),
            (
                self.staging_path,
                _STAGING_FILENAME_RE,
                self.max_staging_objects,
                "staging",
            ),
        )
        for directory, filename_pattern, entry_limit, entry_kind in layouts:
            entry_count = 0
            for path in directory.iterdir():
                entry_count += 1
                if entry_count > entry_limit:
                    raise KnmiRtcorArtifactCapacityError(
                        f"derived artifact {entry_kind} count exceeds its bound"
                    )
                if filename_pattern.fullmatch(path.name) is None:
                    raise KnmiRtcorArtifactError(
                        f"derived artifact store contains unknown {entry_kind} entry"
                    )
                info = path.lstat()
                if path.is_symlink() or not stat.S_ISREG(info.st_mode):
                    raise KnmiRtcorArtifactError("artifact store contains unsafe entry")
                identity: tuple[int, int] | tuple[str, str] = (
                    (int(info.st_dev), int(info.st_ino))
                    if info.st_ino
                    else ("path", str(path.resolve()))
                )
                if identity not in seen:
                    seen.add(identity)
                    total += int(info.st_size)
                if entry_kind == "object":
                    object_count += 1
                else:
                    staging_count += 1
        return total, object_count, staging_count

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        try:
            descriptor = os.open(path, flags)
        except OSError:
            if os.name == "nt":  # Windows development hosts cannot fsync dirs.
                return
            raise
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _fault(self, checkpoint: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(checkpoint)


def _required_attr(attributes: Any, name: str) -> object:
    if name not in attributes:
        raise _RejectedDecode("incomplete", "incomplete:hdf5_metadata")
    return attributes[name]


def _ascii(value: object, *, maximum: int = 512) -> str:
    if isinstance(value, np.bytes_):
        value = bytes(value)
    if isinstance(value, bytes):
        try:
            result = value.decode("ascii")
        except UnicodeDecodeError as exc:
            raise _RejectedDecode("unsupported", "unsupported:hdf5_text") from exc
    elif isinstance(value, str):
        result = value
    else:
        raise _RejectedDecode("incomplete", "incomplete:hdf5_metadata")
    if (
        not result
        or result != result.strip()
        or len(result) > maximum
        or any(ord(character) < 32 for character in result)
    ):
        raise _RejectedDecode("unsupported", "unsupported:hdf5_text")
    return result


def _number(value: object) -> float:
    array = np.asarray(value)
    if array.shape != () or array.dtype.kind not in "iuf":
        raise _RejectedDecode("incomplete", "incomplete:hdf5_metadata")
    result = float(array.item())
    if not math.isfinite(result):
        raise _RejectedDecode("unsupported", "unsupported:numeric_metadata")
    return result


def _integer(value: object) -> int:
    number = _number(value)
    if not number.is_integer():
        raise _RejectedDecode("unsupported", "unsupported:numeric_metadata")
    return int(number)


def _parse_timestamp(text: str) -> float:
    match = _HDF_TIMESTAMP_RE.fullmatch(text)
    if match is None:
        raise _RejectedDecode("unsupported", "unsupported:time_contract")
    month = _MONTHS.get(match.group("month"))
    if month is None:
        raise _RejectedDecode("unsupported", "unsupported:time_contract")
    fraction = (match.group("fraction") or "").ljust(6, "0")
    try:
        result = datetime(
            int(match.group("year")),
            month,
            int(match.group("day")),
            int(match.group("hour")),
            int(match.group("minute")),
            int(match.group("second")),
            int(fraction or "0"),
            tzinfo=timezone.utc,
        )
    except ValueError as exc:
        raise _RejectedDecode("unsupported", "unsupported:time_contract") from exc
    return result.timestamp()


def _hard_group(root: h5py.File | h5py.Group, path: str) -> h5py.Group:
    link = root.get(path, getlink=True)
    if link is None:
        raise _RejectedDecode("incomplete", "incomplete:hdf5_structure")
    if not isinstance(link, h5py.HardLink):
        raise _RejectedDecode("unsupported", "unsupported:hdf5_links")
    value = root.get(path)
    if not isinstance(value, h5py.Group):
        raise _RejectedDecode("incomplete", "incomplete:hdf5_structure")
    return value


def _hard_dataset(root: h5py.File | h5py.Group, path: str) -> h5py.Dataset:
    link = root.get(path, getlink=True)
    if link is None:
        raise _RejectedDecode("incomplete", "incomplete:hdf5_structure")
    if not isinstance(link, h5py.HardLink):
        raise _RejectedDecode("unsupported", "unsupported:hdf5_links")
    value = root.get(path)
    if not isinstance(value, h5py.Dataset):
        raise _RejectedDecode("incomplete", "incomplete:hdf5_structure")
    return value


def _validate_self_contained(root: h5py.File) -> None:
    seen_groups: set[int] = set()
    object_count = 0

    def walk(group: h5py.Group, depth: int) -> None:
        nonlocal object_count
        if depth > 16:
            raise _RejectedDecode("unsupported", "unsupported:hdf5_resource_graph")
        identity = hash(group.id)
        if identity in seen_groups:
            return
        seen_groups.add(identity)
        if len(group) > 1024:
            raise _RejectedDecode("unsupported", "unsupported:hdf5_resource_graph")
        for name in group.keys():
            object_count += 1
            if object_count > MAX_HDF_OBJECTS:
                raise _RejectedDecode("unsupported", "unsupported:hdf5_resource_graph")
            link = group.get(name, getlink=True)
            if not isinstance(link, h5py.HardLink):
                raise _RejectedDecode("unsupported", "unsupported:hdf5_links")
            value = group.get(name)
            if isinstance(value, h5py.Group):
                walk(value, depth + 1)
            elif isinstance(value, h5py.Dataset):
                if bool(value.is_virtual) or bool(value.external):
                    raise _RejectedDecode("unsupported", "unsupported:hdf5_storage")
            else:
                raise _RejectedDecode("unsupported", "unsupported:hdf5_resource_graph")

    walk(root, 0)


def _exact_float(
    attributes: Any,
    name: str,
    expected: float,
    *,
    tolerance: float,
) -> float:
    value = _number(_required_attr(attributes, name))
    if not math.isclose(value, expected, rel_tol=0.0, abs_tol=tolerance):
        raise _RejectedDecode("unsupported", "unsupported:grid_contract")
    return value


def _validate_geography(root: h5py.File) -> tuple[tuple[float, float, float, float], str, str]:
    geographic = _hard_group(root, "geographic")
    attributes = geographic.attrs
    if (
        _integer(_required_attr(attributes, "geo_number_columns")) != EXPECTED_COLUMNS
        or _integer(_required_attr(attributes, "geo_number_rows")) != EXPECTED_ROWS
        or _ascii(_required_attr(attributes, "geo_par_pixel")) != "X,Y"
        or _ascii(_required_attr(attributes, "geo_dim_pixel")) != "KM,KM"
        or _ascii(_required_attr(attributes, "geo_pixel_def")) != "LU"
    ):
        raise _RejectedDecode("unsupported", "unsupported:grid_contract")
    column_offset = _exact_float(
        attributes, "geo_column_offset", EXPECTED_COLUMN_OFFSET_KM, tolerance=1e-9
    )
    row_offset = _exact_float(
        attributes, "geo_row_offset", EXPECTED_ROW_OFFSET_KM, tolerance=5e-4
    )
    pixel_x = _exact_float(
        attributes, "geo_pixel_size_x", EXPECTED_PIXEL_SIZE_X_KM, tolerance=1e-6
    )
    pixel_y = _exact_float(
        attributes, "geo_pixel_size_y", EXPECTED_PIXEL_SIZE_Y_KM, tolerance=1e-6
    )
    corners = np.asarray(_required_attr(attributes, "geo_product_corners"))
    if corners.shape != (8,) or corners.dtype.kind not in "iuf" or not np.all(np.isfinite(corners)):
        raise _RejectedDecode("unsupported", "unsupported:grid_corners")
    if not np.allclose(
        corners.astype(np.float64),
        np.asarray(EXPECTED_CORNERS, dtype=np.float64),
        rtol=0.0,
        atol=5e-6,
    ):
        raise _RejectedDecode("unsupported", "unsupported:grid_corners")
    projection = _hard_group(geographic, "map_projection").attrs
    if (
        _ascii(_required_attr(projection, "projection_indication")) != "Y"
        or _ascii(_required_attr(projection, "projection_name")) != "STEREOGRAPHIC"
        or _ascii(_required_attr(projection, "projection_proj4_params"))
        != EXPECTED_PROJECTION
    ):
        raise _RejectedDecode("unsupported", "unsupported:crs_profile")

    # Attribute storage may be IEEE float32 or float64.  Once the encoded
    # value has passed the precision-bounded product check, bind the published
    # artifact to the normative NL25 decimals instead of retaining a storage-
    # dtype rounding accident as a different grid identity.
    column_offset = EXPECTED_COLUMN_OFFSET_KM
    row_offset = EXPECTED_ROW_OFFSET_KM
    pixel_x = EXPECTED_PIXEL_SIZE_X_KM
    pixel_y = EXPECTED_PIXEL_SIZE_Y_KM

    x0 = column_offset
    x1 = column_offset + EXPECTED_COLUMNS * pixel_x
    y0 = row_offset
    y1 = row_offset + EXPECTED_ROWS * pixel_y
    bbox = (min(x0, x1), min(y0, y1), max(x0, x1), max(y0, y1))
    grid_definition = {
        "axis_order": "yx",
        "column_offset_km": column_offset,
        "columns": EXPECTED_COLUMNS,
        "corner_order": "southwest_northwest_northeast_southeast",
        "corners_wgs84": list(EXPECTED_CORNERS),
        "pixel_definition": "LU",
        "pixel_parameter": "X,Y",
        "pixel_size_x_km": pixel_x,
        "pixel_size_y_km": pixel_y,
        "projection": EXPECTED_PROJECTION,
        "row_offset_km": row_offset,
        "rows": EXPECTED_ROWS,
    }
    coverage_definition = {
        "coordinate_unit": "km",
        "kind": "native_bbox",
        "maximum_x": bbox[2],
        "maximum_y": bbox[3],
        "minimum_x": bbox[0],
        "minimum_y": bbox[1],
    }
    return (
        bbox,
        hashlib.sha256(_canonical_json(grid_definition)).hexdigest(),
        hashlib.sha256(_canonical_json(coverage_definition)).hexdigest(),
    )


@dataclass(frozen=True, slots=True)
class _LayerContract:
    group_name: str
    parameter: str
    formula: str
    dtype: np.dtype
    declared_bytes_per_pixel: int
    gain: float
    offset: float
    missing: int
    out_of_image: int
    physical_name: str


_LAYERS = (
    _LayerContract(
        "image1", PRECIP_PARAMETER, PRECIP_FORMULA, np.dtype("<u2"), 2,
        PRECIP_GAIN, PRECIP_OFFSET, PRECIP_MISSING, PRECIP_OUT_OF_IMAGE,
        "precipitation_mm",
    ),
    _LayerContract(
        "image2", QUALITY_PARAMETER, QUALITY_FORMULA, np.dtype("|u1"), 2,
        QUALITY_GAIN, QUALITY_OFFSET, QUALITY_MISSING, QUALITY_OUT_OF_IMAGE,
        "quality_index",
    ),
    _LayerContract(
        "image3", ADJUSTMENT_PARAMETER, ADJUSTMENT_FORMULA, np.dtype("|u1"), 2,
        ADJUSTMENT_GAIN, ADJUSTMENT_OFFSET, ADJUSTMENT_MISSING,
        ADJUSTMENT_OUT_OF_IMAGE, "adjustment_db",
    ),
)


def _dataset_object_identity(dataset: h5py.Dataset) -> tuple[int, int]:
    information = h5py.h5o.get_info(dataset.id)
    identity = (int(information.fileno), int(information.addr))
    if identity[0] < 0 or identity[1] < 0:
        raise _RejectedDecode("decode_error", "decode_error:hdf5_container")
    return identity


def _validate_filter_pipeline_and_allocation(dataset: h5py.Dataset) -> None:
    creation = dataset.id.get_create_plist()
    if creation.get_layout() != h5py.h5d.CHUNKED or creation.get_nfilters() != 1:
        raise _RejectedDecode("unsupported", "unsupported:hdf5_filter_pipeline")
    filter_id, flags, client_data, name = creation.get_filter(0)
    if (
        int(filter_id) != int(h5py.h5z.FILTER_DEFLATE)
        or int(flags) not in {
            int(h5py.h5z.FLAG_MANDATORY),
            int(h5py.h5z.FLAG_OPTIONAL),
        }
        or tuple(client_data) != (dataset.compression_opts,)
        or not isinstance(dataset.compression_opts, int)
        or not 0 <= dataset.compression_opts <= 9
        or bytes(name) != b"deflate"
    ):
        raise _RejectedDecode("unsupported", "unsupported:hdf5_filter_pipeline")
    if (
        dataset.id.get_space_status() != h5py.h5d.SPACE_STATUS_ALLOCATED
        or dataset.id.get_num_chunks() != 1
    ):
        raise _RejectedDecode("unsupported", "unsupported:hdf5_chunk_allocation")
    chunk = dataset.id.get_chunk_info_by_coord((0, 0))
    if int(chunk.filter_mask) != 0:
        raise _RejectedDecode("unsupported", "unsupported:hdf5_filter_pipeline")
    storage_size = int(dataset.id.get_storage_size())
    if (
        tuple(int(value) for value in chunk.chunk_offset) != (0, 0)
        or not isinstance(chunk.byte_offset, int)
        or int(chunk.byte_offset) < 0
        or int(chunk.size) <= 0
        or storage_size <= 0
        or storage_size != int(chunk.size)
    ):
        raise _RejectedDecode("unsupported", "unsupported:hdf5_chunk_allocation")


def _decode_layer(
    root: h5py.File,
    contract: _LayerContract,
    seen_dataset_objects: set[tuple[int, int]],
) -> dict[str, np.ndarray]:
    group = _hard_group(root, contract.group_name)
    if (
        _ascii(_required_attr(group.attrs, "image_product_name"))
        != KNMI_INTERNAL_PRODUCT_NAME
        or _ascii(_required_attr(group.attrs, "image_geo_parameter"))
        != contract.parameter
        or _integer(_required_attr(group.attrs, "image_bytes_per_pixel"))
        != contract.declared_bytes_per_pixel
        or _integer(_required_attr(group.attrs, "image_size"))
        != EXPECTED_ROWS * EXPECTED_COLUMNS * contract.declared_bytes_per_pixel
    ):
        raise _RejectedDecode("unsupported", "unsupported:layer_contract")
    calibration = _hard_group(group, "calibration").attrs
    if (
        _ascii(_required_attr(calibration, "calibration_flag")) != "Y"
        or _ascii(_required_attr(calibration, "calibration_formulas"))
        != contract.formula
        or _integer(_required_attr(calibration, "calibration_missing_data"))
        != contract.missing
        or _integer(_required_attr(calibration, "calibration_out_of_image"))
        != contract.out_of_image
    ):
        raise _RejectedDecode("unsupported", "unsupported:calibration_contract")
    dataset = _hard_dataset(group, "image_data")
    if (
        dataset.ndim != 2
        or tuple(dataset.shape) != (EXPECTED_ROWS, EXPECTED_COLUMNS)
        or dataset.dtype.kind != "u"
        or dataset.dtype.itemsize != contract.dtype.itemsize
        or (dataset.dtype.itemsize > 1 and dataset.dtype.byteorder not in {"<", "="})
        or dataset.chunks != (EXPECTED_ROWS, EXPECTED_COLUMNS)
        or bool(dataset.is_virtual)
        or bool(dataset.external)
    ):
        raise _RejectedDecode("unsupported", "unsupported:layer_storage")
    object_identity = _dataset_object_identity(dataset)
    if object_identity in seen_dataset_objects:
        raise _RejectedDecode("unsupported", "unsupported:hdf5_dataset_alias")
    seen_dataset_objects.add(object_identity)
    _validate_filter_pipeline_and_allocation(dataset)
    if (
        _ascii(_required_attr(dataset.attrs, "CLASS")) != "IMAGE"
        or _ascii(_required_attr(dataset.attrs, "IMAGE_VERSION")) != "1.2"
    ):
        raise _RejectedDecode("unsupported", "unsupported:image_literal")
    try:
        raw = np.asarray(dataset[...], dtype=contract.dtype, order="C")
    except (OSError, RuntimeError, ValueError) as exc:
        raise _RejectedDecode("decode_error", "decode_error:raster_read") from exc
    if raw.shape != (EXPECTED_ROWS, EXPECTED_COLUMNS):
        raise _RejectedDecode("decode_error", "decode_error:raster_read")
    missing = np.asarray(raw == contract.missing, dtype=np.uint8)
    out_of_image = np.asarray(raw == contract.out_of_image, dtype=np.uint8)
    valid = np.asarray((missing == 0) & (out_of_image == 0), dtype=np.uint8)
    physical = np.zeros(raw.shape, dtype=np.dtype("<f8"))
    physical[valid == 1] = (
        raw[valid == 1].astype(np.float64) * contract.gain + contract.offset
    )
    prefix = contract.group_name.replace("image1", "precipitation").replace(
        "image2", "quality"
    ).replace("image3", "adjustment")
    result = {
        f"{prefix}_raw": raw,
        contract.physical_name: physical,
        f"{prefix}_valid_mask": valid,
        f"{prefix}_missing_mask": missing,
        f"{prefix}_out_of_image_mask": out_of_image,
    }
    for array in result.values():
        array.setflags(write=False)
    return result


def _decode_hdf(payload: bytes) -> _DecodedFrame:
    if not isinstance(payload, bytes) or not 0 < len(payload) <= MAX_HDF5_BYTES:
        raise _RejectedDecode("decode_error", "decode_error:hdf5_container")
    try:
        with h5py.File(io.BytesIO(payload), "r") as root:
            _validate_self_contained(root)
            overview = _hard_group(root, "overview").attrs
            if (
                _ascii(_required_attr(overview, "product_group_name"))
                != KNMI_INTERNAL_PRODUCT_NAME
                or _ascii(_required_attr(overview, "hdftag_version_number"))
                != SUPPORTED_HDF_TAG
                or _integer(_required_attr(overview, "number_image_groups")) != 3
            ):
                raise _RejectedDecode("unsupported", "unsupported:rtcor_profile")
            product_start = _parse_timestamp(
                _ascii(_required_attr(overview, "product_datetime_start"))
            )
            product_end = _parse_timestamp(
                _ascii(_required_attr(overview, "product_datetime_end"))
            )
            if not math.isclose(
                product_end - product_start,
                EXPECTED_ACCUMULATION_SECONDS,
                rel_tol=0.0,
                abs_tol=1e-6,
            ):
                raise _RejectedDecode("unsupported", "unsupported:time_contract")
            bbox, grid_hash, coverage_hash = _validate_geography(root)
            arrays: dict[str, np.ndarray] = {}
            seen_dataset_objects: set[tuple[int, int]] = set()
            for contract in _LAYERS:
                arrays.update(_decode_layer(root, contract, seen_dataset_objects))
    except _RejectedDecode:
        raise
    except _HDF_PAYLOAD_ERRORS as exc:
        raise _RejectedDecode("decode_error", "decode_error:hdf5_container") from exc
    return _DecodedFrame(
        arrays=arrays,
        product_start=product_start,
        product_end=product_end,
        bbox_km=bbox,
        grid_definition_sha256=grid_hash,
        coverage_definition_sha256=coverage_hash,
    )


_ARRAY_LAYOUT = (
    ("precipitation_raw", "<u2", "native_uint16_with_sentinels"),
    ("precipitation_mm", "<f8", "valid_pixels_only_invalid_fill_zero"),
    ("precipitation_valid_mask", "|u1", "one_is_valid_native_pixel"),
    ("precipitation_missing_mask", "|u1", "one_is_missing_data"),
    ("precipitation_out_of_image_mask", "|u1", "one_is_outside_product_domain"),
    ("quality_raw", "|u1", "native_uint8_with_sentinels"),
    ("quality_index", "<f8", "native_quality_index_valid_pixels_only"),
    ("quality_valid_mask", "|u1", "one_is_valid_quality_pixel"),
    ("quality_missing_mask", "|u1", "one_is_missing_quality"),
    ("quality_out_of_image_mask", "|u1", "one_is_outside_quality_domain"),
    ("adjustment_raw", "|u1", "native_uint8_with_sentinels"),
    ("adjustment_db", "<f8", "delivered_adjustment_field_valid_pixels_only"),
    ("adjustment_valid_mask", "|u1", "one_is_valid_adjustment_pixel"),
    ("adjustment_missing_mask", "|u1", "one_is_missing_adjustment"),
    ("adjustment_out_of_image_mask", "|u1", "one_is_outside_adjustment_domain"),
)


def _artifact_bytes(
    capture: RawArtifactCaptureReceipt,
    raw_receipt: RawArtifactReceipt,
    filename: str,
    frame: _DecodedFrame,
) -> bytes:
    sections: list[dict[str, Any]] = []
    chunks: list[bytes] = []
    offset = 0
    for name, dtype_text, semantics in _ARRAY_LAYOUT:
        array = frame.arrays[name]
        dtype = np.dtype(dtype_text)
        if array.dtype != dtype or array.shape != (EXPECTED_ROWS, EXPECTED_COLUMNS):
            raise KnmiRtcorArtifactError("decoded array violates fixed layout")
        chunk = array.tobytes(order="C")
        sections.append({
            "dtype": dtype.str,
            "name": name,
            "nbytes": len(chunk),
            "offset": offset,
            "semantics": semantics,
            "sha256": hashlib.sha256(chunk).hexdigest(),
            "shape": [EXPECTED_ROWS, EXPECTED_COLUMNS],
        })
        chunks.append(chunk)
        offset += len(chunk)
    uncompressed = b"".join(chunks)
    if not 0 < len(uncompressed) <= MAX_UNCOMPRESSED_ARRAY_BYTES:
        raise KnmiRtcorArtifactCapacityError("decoded arrays exceed their byte bound")
    compressed = zlib.compress(uncompressed, level=6)
    minimum_x, minimum_y, maximum_x, maximum_y = frame.bbox_km
    metadata = {
        "arrays": sections,
        "capture": {
            "capture_id": capture.capture_id,
            "capture_receipt_sha256": capture.capture_receipt_sha256,
            "manifest_sha256": capture.manifest_sha256,
        },
        "decoder": {
            "config_sha256": DECODER_CONFIG_SHA256,
            "identity": DECODER_IDENTITY,
            "version": DECODER_VERSION,
        },
        "grid": {
            "axis_order": "yx",
            "column_direction": "west_to_east",
            "column_offset_km": EXPECTED_COLUMN_OFFSET_KM,
            "coordinate_unit": "km",
            "corner_coordinates_wgs84": {
                "northeast_latitude": EXPECTED_CORNERS[5],
                "northeast_longitude": EXPECTED_CORNERS[4],
                "northwest_latitude": EXPECTED_CORNERS[3],
                "northwest_longitude": EXPECTED_CORNERS[2],
                "southeast_latitude": EXPECTED_CORNERS[7],
                "southeast_longitude": EXPECTED_CORNERS[6],
                "southwest_latitude": EXPECTED_CORNERS[1],
                "southwest_longitude": EXPECTED_CORNERS[0],
            },
            "coverage": {
                "kind": "native_bbox",
                "maximum_x": maximum_x,
                "maximum_y": maximum_y,
                "minimum_x": minimum_x,
                "minimum_y": minimum_y,
            },
            "coverage_definition_sha256": frame.coverage_definition_sha256,
            "definition_sha256": frame.grid_definition_sha256,
            "height": EXPECTED_ROWS,
            "pixel_definition": "LU",
            "pixel_size_x_km": EXPECTED_PIXEL_SIZE_X_KM,
            "pixel_size_y_km": EXPECTED_PIXEL_SIZE_Y_KM,
            "proj4": EXPECTED_PROJECTION,
            "row_direction": "north_to_south",
            "row_offset_km": EXPECTED_ROW_OFFSET_KM,
            "width": EXPECTED_COLUMNS,
        },
        "layers": {
            "adjustment": {
                "application": "diagnostic_delivered_field_not_applied_by_decoder",
                "formula": ADJUSTMENT_FORMULA,
                "may_reapply_to_image1": False,
                "missing": ADJUSTMENT_MISSING,
                "out_of_image": ADJUSTMENT_OUT_OF_IMAGE,
                "parameter": ADJUSTMENT_PARAMETER,
                "reapplication_policy": ADJUSTMENT_REAPPLICATION_POLICY,
                "unit": "dB",
            },
            "precipitation": {
                "accumulation_seconds": EXPECTED_ACCUMULATION_SECONDS,
                "formula": PRECIP_FORMULA,
                "missing": PRECIP_MISSING,
                "out_of_image": PRECIP_OUT_OF_IMAGE,
                "parameter": PRECIP_PARAMETER,
                "unit": "mm",
                "upstream_adjustment_state": PRECIPITATION_UPSTREAM_ADJUSTMENT_STATE,
                "upstream_gauge_adjusted": True,
            },
            "quality": {
                "formula": QUALITY_FORMULA,
                "meaning": "upstream_native_index_not_a_decision_metric",
                "missing": QUALITY_MISSING,
                "out_of_image": QUALITY_OUT_OF_IMAGE,
                "parameter": QUALITY_PARAMETER,
                "unit": "1",
            },
        },
        "payload": {
            "codec": "zlib",
            "compressed_bytes": len(compressed),
            "compressed_sha256": hashlib.sha256(compressed).hexdigest(),
            "level": 6,
            "uncompressed_bytes": len(uncompressed),
            "uncompressed_sha256": hashlib.sha256(uncompressed).hexdigest(),
        },
        "product": {
            "hdf_tag_version": SUPPORTED_HDF_TAG,
            "interval_end": frame.product_end,
            "interval_start": frame.product_start,
            "product_group_name": KNMI_INTERNAL_PRODUCT_NAME,
            "product_time": frame.product_end,
            "semantic_quantity": "precipitation_accumulation",
        },
        "schema": ARTIFACT_SCHEMA,
        "source": {
            "hdf5_content_sha256": raw_receipt.content_sha256,
            "hdf5_content_type": raw_receipt.content_type,
            "hdf5_provenance_sha256": raw_receipt.provenance_sha256,
            "hdf5_received_at": raw_receipt.received_at,
            "hdf5_receipt_id": raw_receipt.receipt_id,
            "hdf5_receipt_sha256": raw_receipt.receipt_sha256,
            "hdf5_size_bytes": raw_receipt.size_bytes,
            "hdf5_upstream_id": raw_receipt.upstream_id,
            "role": KNMI_ROLE,
            "upstream_filename": filename,
        },
    }
    header = _canonical_json(metadata)
    if len(header) > MAX_HEADER_BYTES:
        raise KnmiRtcorArtifactCapacityError("derived artifact header exceeds bound")
    result = ARTIFACT_MAGIC + struct.pack(">I", len(header)) + header + compressed
    if len(result) > MAX_ARTIFACT_BYTES:
        raise KnmiRtcorArtifactCapacityError("derived artifact exceeds object bound")
    return result


def _bounded_decompress(payload: bytes, expected_bytes: int) -> bytes:
    if not 0 < expected_bytes <= MAX_UNCOMPRESSED_ARRAY_BYTES:
        raise KnmiRtcorArtifactError("artifact uncompressed size is invalid")
    decompressor = zlib.decompressobj()
    try:
        decoded = decompressor.decompress(payload, expected_bytes + 1)
        if len(decoded) > expected_bytes or decompressor.unconsumed_tail:
            raise KnmiRtcorArtifactError("artifact expands beyond bound")
        decoded += decompressor.flush(expected_bytes + 1 - len(decoded))
    except zlib.error as exc:
        raise KnmiRtcorArtifactError("artifact compression stream is invalid") from exc
    if len(decoded) != expected_bytes or not decompressor.eof or decompressor.unused_data:
        raise KnmiRtcorArtifactError("artifact compression length differs")
    return decoded


def _layer_arrays_valid(
    arrays: Mapping[str, np.ndarray],
    prefix: str,
    physical_name: str,
    *,
    gain: float,
    offset: float,
    missing_code: int,
    out_code: int,
) -> bool:
    raw = arrays[f"{prefix}_raw"]
    physical = arrays[physical_name]
    valid = arrays[f"{prefix}_valid_mask"]
    missing = arrays[f"{prefix}_missing_mask"]
    out = arrays[f"{prefix}_out_of_image_mask"]
    expected_missing = np.asarray(raw == missing_code, dtype=np.uint8)
    expected_out = np.asarray(raw == out_code, dtype=np.uint8)
    expected_valid = np.asarray((expected_missing == 0) & (expected_out == 0), dtype=np.uint8)
    if (
        not np.array_equal(missing, expected_missing)
        or not np.array_equal(out, expected_out)
        or not np.array_equal(valid, expected_valid)
        or np.any(physical[valid == 0] != 0.0)
    ):
        return False
    expected_physical = raw[valid == 1].astype(np.float64) * gain + offset
    return np.array_equal(physical[valid == 1], expected_physical)


def _parse_artifact(payload: bytes) -> KnmiRtcorRasterArtifact:
    if not isinstance(payload, bytes) or not 0 < len(payload) <= MAX_ARTIFACT_BYTES:
        raise KnmiRtcorArtifactError("artifact payload is outside bounds")
    prefix_size = len(ARTIFACT_MAGIC) + 4
    if len(payload) < prefix_size or not payload.startswith(ARTIFACT_MAGIC):
        raise KnmiRtcorArtifactError("artifact magic is invalid")
    header_size = struct.unpack(">I", payload[len(ARTIFACT_MAGIC):prefix_size])[0]
    if not 0 < header_size <= MAX_HEADER_BYTES or prefix_size + header_size >= len(payload):
        raise KnmiRtcorArtifactError("artifact header size is invalid")
    header_bytes = payload[prefix_size:prefix_size + header_size]
    try:
        metadata = json.loads(header_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise KnmiRtcorArtifactError("artifact header is invalid JSON") from exc
    try:
        canonical_header = _canonical_json(metadata)
    except (TypeError, ValueError) as exc:
        raise KnmiRtcorArtifactError("artifact header contains invalid JSON values") from exc
    if not isinstance(metadata, dict) or canonical_header != header_bytes:
        raise KnmiRtcorArtifactError("artifact header is not canonical")
    if set(metadata) != {
        "arrays", "capture", "decoder", "grid", "layers", "payload",
        "product", "schema", "source",
    } or metadata.get("schema") != ARTIFACT_SCHEMA:
        raise KnmiRtcorArtifactError("artifact schema is unsupported")
    expected_layers = {
        "adjustment": {
            "application": "diagnostic_delivered_field_not_applied_by_decoder",
            "formula": ADJUSTMENT_FORMULA,
            "may_reapply_to_image1": False,
            "missing": ADJUSTMENT_MISSING,
            "out_of_image": ADJUSTMENT_OUT_OF_IMAGE,
            "parameter": ADJUSTMENT_PARAMETER,
            "reapplication_policy": ADJUSTMENT_REAPPLICATION_POLICY,
            "unit": "dB",
        },
        "precipitation": {
            "accumulation_seconds": EXPECTED_ACCUMULATION_SECONDS,
            "formula": PRECIP_FORMULA,
            "missing": PRECIP_MISSING,
            "out_of_image": PRECIP_OUT_OF_IMAGE,
            "parameter": PRECIP_PARAMETER,
            "unit": "mm",
            "upstream_adjustment_state": PRECIPITATION_UPSTREAM_ADJUSTMENT_STATE,
            "upstream_gauge_adjusted": True,
        },
        "quality": {
            "formula": QUALITY_FORMULA,
            "meaning": "upstream_native_index_not_a_decision_metric",
            "missing": QUALITY_MISSING,
            "out_of_image": QUALITY_OUT_OF_IMAGE,
            "parameter": QUALITY_PARAMETER,
            "unit": "1",
        },
    }
    if metadata.get("decoder") != {
        "config_sha256": DECODER_CONFIG_SHA256,
        "identity": DECODER_IDENTITY,
        "version": DECODER_VERSION,
    } or metadata.get("layers") != expected_layers:
        raise KnmiRtcorArtifactError("artifact decoder contract differs")
    product = metadata.get("product")
    if not isinstance(product, dict):
        raise KnmiRtcorArtifactError("artifact product metadata is invalid")
    try:
        interval_start = float(product["interval_start"])
        interval_end = float(product["interval_end"])
        product_time = float(product["product_time"])
    except (KeyError, TypeError, ValueError) as exc:
        raise KnmiRtcorArtifactError("artifact product time is invalid") from exc
    if (
        set(product) != {
            "hdf_tag_version", "interval_end", "interval_start",
            "product_group_name", "product_time", "semantic_quantity",
        }
        or product.get("hdf_tag_version") != SUPPORTED_HDF_TAG
        or product.get("product_group_name") != KNMI_INTERNAL_PRODUCT_NAME
        or product.get("semantic_quantity") != "precipitation_accumulation"
        or not all(math.isfinite(value) for value in (interval_start, interval_end, product_time))
        or interval_end - interval_start != EXPECTED_ACCUMULATION_SECONDS
        or product_time != interval_end
    ):
        raise KnmiRtcorArtifactError("artifact product contract differs")
    minimum_x = EXPECTED_COLUMN_OFFSET_KM
    maximum_x = minimum_x + EXPECTED_COLUMNS * EXPECTED_PIXEL_SIZE_X_KM
    maximum_y = EXPECTED_ROW_OFFSET_KM
    minimum_y = maximum_y + EXPECTED_ROWS * EXPECTED_PIXEL_SIZE_Y_KM
    grid_definition = {
        "axis_order": "yx",
        "column_offset_km": EXPECTED_COLUMN_OFFSET_KM,
        "columns": EXPECTED_COLUMNS,
        "corner_order": "southwest_northwest_northeast_southeast",
        "corners_wgs84": list(EXPECTED_CORNERS),
        "pixel_definition": "LU",
        "pixel_parameter": "X,Y",
        "pixel_size_x_km": EXPECTED_PIXEL_SIZE_X_KM,
        "pixel_size_y_km": EXPECTED_PIXEL_SIZE_Y_KM,
        "projection": EXPECTED_PROJECTION,
        "row_offset_km": EXPECTED_ROW_OFFSET_KM,
        "rows": EXPECTED_ROWS,
    }
    coverage_definition = {
        "coordinate_unit": "km",
        "kind": "native_bbox",
        "maximum_x": maximum_x,
        "maximum_y": maximum_y,
        "minimum_x": minimum_x,
        "minimum_y": minimum_y,
    }
    expected_grid = {
        "axis_order": "yx",
        "column_direction": "west_to_east",
        "column_offset_km": EXPECTED_COLUMN_OFFSET_KM,
        "coordinate_unit": "km",
        "corner_coordinates_wgs84": {
            "northeast_latitude": EXPECTED_CORNERS[5],
            "northeast_longitude": EXPECTED_CORNERS[4],
            "northwest_latitude": EXPECTED_CORNERS[3],
            "northwest_longitude": EXPECTED_CORNERS[2],
            "southeast_latitude": EXPECTED_CORNERS[7],
            "southeast_longitude": EXPECTED_CORNERS[6],
            "southwest_latitude": EXPECTED_CORNERS[1],
            "southwest_longitude": EXPECTED_CORNERS[0],
        },
        "coverage": {
            "kind": "native_bbox",
            "maximum_x": maximum_x,
            "maximum_y": maximum_y,
            "minimum_x": minimum_x,
            "minimum_y": minimum_y,
        },
        "coverage_definition_sha256": hashlib.sha256(
            _canonical_json(coverage_definition)
        ).hexdigest(),
        "definition_sha256": hashlib.sha256(
            _canonical_json(grid_definition)
        ).hexdigest(),
        "height": EXPECTED_ROWS,
        "pixel_definition": "LU",
        "pixel_size_x_km": EXPECTED_PIXEL_SIZE_X_KM,
        "pixel_size_y_km": EXPECTED_PIXEL_SIZE_Y_KM,
        "proj4": EXPECTED_PROJECTION,
        "row_direction": "north_to_south",
        "row_offset_km": EXPECTED_ROW_OFFSET_KM,
        "width": EXPECTED_COLUMNS,
    }
    grid = metadata.get("grid")
    if grid != expected_grid:
        raise KnmiRtcorArtifactError("artifact grid contract differs")
    capture = metadata.get("capture")
    source = metadata.get("source")
    if (
        not isinstance(capture, dict)
        or set(capture) != {"capture_id", "capture_receipt_sha256", "manifest_sha256"}
        or re.fullmatch(r"capture-[0-9a-f]{64}", str(capture.get("capture_id"))) is None
        or not isinstance(capture.get("capture_receipt_sha256"), str)
        or not _SHA256_RE.fullmatch(str(capture.get("capture_receipt_sha256")))
        or not _SHA256_RE.fullmatch(str(capture.get("manifest_sha256")))
        or not isinstance(source, dict)
        or set(source) != {
            "hdf5_content_sha256", "hdf5_content_type",
            "hdf5_provenance_sha256", "hdf5_received_at",
            "hdf5_receipt_id", "hdf5_receipt_sha256", "hdf5_size_bytes",
            "hdf5_upstream_id", "role", "upstream_filename",
        }
        or not _SHA256_RE.fullmatch(str(source.get("hdf5_content_sha256")))
        or source.get("hdf5_content_type") not in {"application/octet-stream", "application/x-hdf5"}
        or not _SHA256_RE.fullmatch(str(source.get("hdf5_provenance_sha256")))
        or not re.fullmatch(r"raw-[0-9a-f]{64}", str(source.get("hdf5_receipt_id")))
        or not _SHA256_RE.fullmatch(str(source.get("hdf5_receipt_sha256")))
        or isinstance(source.get("hdf5_size_bytes"), bool)
        or not isinstance(source.get("hdf5_size_bytes"), int)
        or not 0 < source["hdf5_size_bytes"] <= MAX_HDF5_BYTES
        or isinstance(source.get("hdf5_received_at"), bool)
        or not isinstance(source.get("hdf5_received_at"), (int, float))
        or not math.isfinite(float(source["hdf5_received_at"]))
        or float(source["hdf5_received_at"]) < product_time
        or not isinstance(source.get("hdf5_upstream_id"), str)
        or len(source["hdf5_upstream_id"]) > 1024
        or source.get("role") != KNMI_ROLE
        or _RTCOR_FILENAME_RE.fullmatch(str(source.get("upstream_filename"))) is None
    ):
        raise KnmiRtcorArtifactError("artifact source binding is invalid")
    source_filename = str(source["upstream_filename"])
    filename_match = _RTCOR_FILENAME_RE.fullmatch(source_filename)
    try:
        source_product_time = datetime.strptime(
            filename_match.group("timestamp"), "%Y%m%d%H%M"  # type: ignore[union-attr]
        ).replace(tzinfo=timezone.utc).timestamp()
    except (AttributeError, ValueError) as exc:
        raise KnmiRtcorArtifactError("artifact source time is invalid") from exc
    if (
        source_product_time != product_time
        or source["hdf5_upstream_id"]
        != f"{KNMI_DATASET_NAME}/{KNMI_DATASET_VERSION}|filename={source_filename}"
    ):
        raise KnmiRtcorArtifactError("artifact source identity differs")
    compressed = payload[prefix_size + header_size:]
    payload_meta = metadata.get("payload")
    if (
        not isinstance(payload_meta, dict)
        or set(payload_meta) != {
            "codec", "compressed_bytes", "compressed_sha256", "level",
            "uncompressed_bytes", "uncompressed_sha256",
        }
        or payload_meta.get("codec") != "zlib"
        or payload_meta.get("level") != 6
        or payload_meta.get("compressed_bytes") != len(compressed)
        or payload_meta.get("compressed_sha256") != hashlib.sha256(compressed).hexdigest()
    ):
        raise KnmiRtcorArtifactError("artifact compressed metadata differs")
    expected_uncompressed = payload_meta.get("uncompressed_bytes")
    if isinstance(expected_uncompressed, bool) or not isinstance(expected_uncompressed, int):
        raise KnmiRtcorArtifactError("artifact uncompressed size is invalid")
    uncompressed = _bounded_decompress(compressed, expected_uncompressed)
    if payload_meta.get("uncompressed_sha256") != hashlib.sha256(uncompressed).hexdigest():
        raise KnmiRtcorArtifactError("artifact array payload checksum differs")
    section_values = metadata.get("arrays")
    if not isinstance(section_values, list) or len(section_values) != len(_ARRAY_LAYOUT):
        raise KnmiRtcorArtifactError("artifact array manifest is invalid")
    arrays: dict[str, np.ndarray] = {}
    expected_offset = 0
    for section, (name, dtype_text, semantics) in zip(section_values, _ARRAY_LAYOUT, strict=True):
        if not isinstance(section, dict):
            raise KnmiRtcorArtifactError("artifact array section is invalid")
        dtype = np.dtype(dtype_text)
        expected_nbytes = EXPECTED_ROWS * EXPECTED_COLUMNS * dtype.itemsize
        if (
            section.get("name") != name
            or section.get("dtype") != dtype.str
            or section.get("shape") != [EXPECTED_ROWS, EXPECTED_COLUMNS]
            or section.get("offset") != expected_offset
            or section.get("nbytes") != expected_nbytes
            or section.get("semantics") != semantics
            or set(section) != {"dtype", "name", "nbytes", "offset", "semantics", "sha256", "shape"}
        ):
            raise KnmiRtcorArtifactError("artifact array layout differs")
        chunk = uncompressed[expected_offset:expected_offset + expected_nbytes]
        if len(chunk) != expected_nbytes or section.get("sha256") != hashlib.sha256(chunk).hexdigest():
            raise KnmiRtcorArtifactError("artifact array checksum differs")
        array = np.frombuffer(chunk, dtype=dtype).reshape(EXPECTED_ROWS, EXPECTED_COLUMNS)
        array.setflags(write=False)
        arrays[name] = array
        expected_offset += expected_nbytes
    if expected_offset != len(uncompressed):
        raise KnmiRtcorArtifactError("artifact contains unbound array bytes")
    if not (
        _layer_arrays_valid(
            arrays, "precipitation", "precipitation_mm",
            gain=PRECIP_GAIN, offset=PRECIP_OFFSET,
            missing_code=PRECIP_MISSING, out_code=PRECIP_OUT_OF_IMAGE,
        )
        and _layer_arrays_valid(
            arrays, "quality", "quality_index",
            gain=QUALITY_GAIN, offset=QUALITY_OFFSET,
            missing_code=QUALITY_MISSING, out_code=QUALITY_OUT_OF_IMAGE,
        )
        and _layer_arrays_valid(
            arrays, "adjustment", "adjustment_db",
            gain=ADJUSTMENT_GAIN, offset=ADJUSTMENT_OFFSET,
            missing_code=ADJUSTMENT_MISSING, out_code=ADJUSTMENT_OUT_OF_IMAGE,
        )
    ):
        raise KnmiRtcorArtifactError("artifact masks or calibration differ")
    return KnmiRtcorRasterArtifact(metadata=metadata, **arrays)


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


def _run_mapping(
    capture: RawArtifactCaptureReceipt,
    *,
    decoder_job_id: str,
    attempt_id: str,
    evaluated_at: float,
    outcome: str,
    reason_code: str,
    artifact: KnmiRtcorArtifactRef | None = None,
    frame: _DecodedFrame | None = None,
) -> dict[str, Any]:
    complete = outcome == "decoded_complete"
    if complete != (artifact is not None and frame is not None):
        raise RuntimeError("decoder result fields violate fail-closed invariant")
    native_grid = None
    coverage = None
    if frame is not None:
        minimum_x, minimum_y, maximum_x, maximum_y = frame.bbox_km
        native_grid = {
            "axis_order": "yx",
            "definition_sha256": frame.grid_definition_sha256,
            "height": EXPECTED_ROWS,
            "kind": "regular",
            "width": EXPECTED_COLUMNS,
        }
        coverage = {
            "definition_sha256": frame.coverage_definition_sha256,
            "kind": "native_bbox",
            "maximum_x": maximum_x,
            "maximum_y": maximum_y,
            "minimum_x": minimum_x,
            "minimum_y": minimum_y,
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
        "native_crs": EXPECTED_PROJECTION if complete else None,
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
        "quantity": "precipitation_accumulation_5_minute" if complete else None,
        "reason_code": reason_code,
        "received_at": max(member.received_at for member in capture.members),
        "unit": "mm" if complete else None,
    }


def _filename_time(filename: str) -> float:
    match = _RTCOR_FILENAME_RE.fullmatch(filename)
    if match is None:
        raise _RejectedDecode("unsupported", "unsupported:raw_member_identity")
    try:
        moment = datetime.strptime(match.group("timestamp"), "%Y%m%d%H%M")
    except ValueError as exc:
        raise _RejectedDecode("unsupported", "unsupported:time_contract") from exc
    return moment.replace(tzinfo=timezone.utc).timestamp()


def _validate_raw_capture(
    capture: RawArtifactCaptureReceipt,
    receipts_by_role: Mapping[str, RawArtifactReceipt],
    payloads_by_role: Mapping[str, bytes],
) -> tuple[RawArtifactReceipt, bytes, str, str]:
    if capture.provider != KNMI_PROVIDER or capture.product != KNMI_CAPTURE_PRODUCT:
        raise _RejectedDecode("unsupported", "unsupported:capture_contract")
    if capture.product_timestamp is None:
        raise _RejectedDecode("incomplete", "incomplete:product_time")
    if len(capture.members) != 1 or capture.members[0].role != KNMI_ROLE:
        raise _RejectedDecode("incomplete", "incomplete:capture_roles")
    member = capture.members[0]
    receipt = receipts_by_role.get(KNMI_ROLE)
    payload = payloads_by_role.get(KNMI_ROLE)
    if receipt is None or payload is None:
        raise RawArtifactIntegrityError("KNMI RTCOR member receipt is missing")
    if (
        receipt.provider != KNMI_PROVIDER
        or receipt.product != KNMI_HDF5_PRODUCT
        or receipt.receipt_id != member.receipt_id
        or receipt.content_sha256 != member.content_sha256
        or receipt.receipt_sha256 != member.receipt_sha256
        or receipt.received_at != member.received_at
        or receipt.size_bytes <= 0
        or receipt.size_bytes > MAX_HDF5_BYTES
        or receipt.content_type not in {"application/octet-stream", "application/x-hdf5"}
    ):
        raise _RejectedDecode("unsupported", "unsupported:raw_member_contract")
    provenance = receipt.provenance
    filename_value = provenance.get("upstream_filename")
    if not isinstance(filename_value, str):
        raise _RejectedDecode("incomplete", "incomplete:raw_provenance")
    filename = filename_value
    if (
        provenance.get("representation") != NATIVE_REPRESENTATION
        or provenance.get("capture_stage") != NATIVE_CAPTURE_STAGE
        or provenance.get("collector") != KNMI_RAW_COLLECTOR
        or provenance.get("collector_version") != KNMI_RAW_COLLECTOR_VERSION
        or provenance.get("structure_inspector_version")
        != KNMI_STRUCTURE_INSPECTOR_VERSION
        or provenance.get("dataset_name") != KNMI_DATASET_NAME
        or provenance.get("dataset_version") != KNMI_DATASET_VERSION
        or provenance.get("dataset_license") != KNMI_DATASET_LICENSE
        or provenance.get("selection_order") != "created_descending"
        or provenance.get("selection_limit") != 1
        or provenance.get("listing_size_bytes") != receipt.size_bytes
        or provenance.get("download_size_bytes") != receipt.size_bytes
        or provenance.get("declared_semantic_class")
        != "quantitative_precipitation_accumulation"
        or provenance.get("declared_quantity") != "precipitation_accumulation"
        or provenance.get("declared_unit") != "mm"
        or provenance.get("declared_accumulation_minutes") != 5.0
        or provenance.get("quantitative_semantics_verified_after_decode") is not False
    ):
        raise _RejectedDecode("unsupported", "unsupported:raw_provenance")
    filename_time = _filename_time(filename)
    if filename_time != capture.product_timestamp:
        raise _RejectedDecode("unsupported", "unsupported:time_contract")
    digest = receipt.content_sha256
    expected_upstream_id = (
        f"{KNMI_DATASET_NAME}/{KNMI_DATASET_VERSION}|filename={filename}"
    )
    expected_capture_upstream_id = (
        f"{KNMI_CAPTURE_PRODUCT}|filename={filename}"
        f"|product_timestamp={capture.product_timestamp:.6f}"
        f"|content_sha256={digest}"
    )
    if receipt.upstream_id != expected_upstream_id:
        raise _RejectedDecode("unsupported", "unsupported:raw_member_identity")
    if capture.upstream_capture_id != expected_capture_upstream_id:
        raise _RejectedDecode("unsupported", "unsupported:capture_identity")
    return receipt, payload, digest, filename


def decode_knmi_rtcor_capture(
    *,
    capture_id: str,
    decoder_job_id: str,
    attempt_id: str,
    raw_store: RawArtifactStore,
    artifact_store: KnmiRtcorDerivedArtifactStore,
    evidence_store: CanonicalEvidenceStore,
    evaluated_at: float | None = None,
    clock: Callable[[], float] | None = None,
    monotonic_deadline: float | None = None,
    monotonic_clock: Callable[[], float] | None = None,
) -> KnmiRtcorDecodeResult:
    """Decode one verified RTCOR capture; never grants operational admission."""
    if not isinstance(raw_store, RawArtifactStore):
        raise TypeError("raw_store must be RawArtifactStore")
    if not isinstance(artifact_store, KnmiRtcorDerivedArtifactStore):
        raise TypeError("artifact_store has wrong type")
    if not isinstance(evidence_store, CanonicalEvidenceStore):
        raise TypeError("evidence_store has wrong type")
    if not isinstance(decoder_job_id, str) or _DECODER_JOB_RE.fullmatch(decoder_job_id) is None:
        raise ValueError("decoder_job_id must be a stable SHA-256 identity")
    if not isinstance(attempt_id, str) or _ATTEMPT_RE.fullmatch(attempt_id) is None:
        raise ValueError("attempt_id must be a stable SHA-256 identity")
    active_monotonic_clock = monotonic_clock or time.monotonic
    monotonic_now = float(active_monotonic_clock())
    if not math.isfinite(monotonic_now):
        raise ValueError("monotonic decoder clock must be finite")
    if monotonic_deadline is None:
        active_deadline = monotonic_now + DECODE_TOTAL_DEADLINE_SECONDS
    else:
        if isinstance(monotonic_deadline, bool) or not isinstance(
            monotonic_deadline, (int, float)
        ):
            raise ValueError("monotonic decoder deadline must be finite")
        active_deadline = float(monotonic_deadline)
        if not math.isfinite(active_deadline):
            raise ValueError("monotonic decoder deadline must be finite")

    def checkpoint(stage: str) -> None:
        current = float(active_monotonic_clock())
        if not math.isfinite(current) or current >= active_deadline:
            raise KnmiRtcorDecodeDeadlineError(
                f"KNMI RTCOR decoder deadline expired at {stage}"
            )

    checkpoint("decode_start")
    capture_bundle = raw_store.load_capture_bundle_bytes(capture_id)
    if capture_bundle is None:
        raise KnmiRtcorCaptureMissingError("schema-3 KNMI RTCOR capture is missing")
    checkpoint("raw_capture_loaded")
    capture, receipts_by_role, payloads_by_role = capture_bundle
    received_at = max(member.received_at for member in capture.members)
    if evaluated_at is None:
        evaluation = float((clock or time.time)())
    else:
        if isinstance(evaluated_at, bool) or not isinstance(evaluated_at, (int, float)):
            raise ValueError("evaluated_at must be a finite timestamp")
        evaluation = float(evaluated_at)
    if not math.isfinite(evaluation) or evaluation < received_at:
        raise ValueError("evaluated_at must be finite and not predate receipt")

    try:
        receipt, payload, raw_sha256, filename = _validate_raw_capture(
            capture,
            receipts_by_role,
            payloads_by_role,
        )
        checkpoint("raw_capture_validated")
        frame = _decode_hdf(payload)
        checkpoint("hdf_decoded")
        if frame.product_end != capture.product_timestamp:
            raise _RejectedDecode("unsupported", "unsupported:time_contract")
        artifact_payload = _artifact_bytes(
            capture,
            receipt,
            filename,
            frame,
        )
        checkpoint("artifact_encoded")
        artifact = artifact_store.publish(
            artifact_payload,
            checkpoint=checkpoint,
        )
        checkpoint("artifact_published")
        verified = artifact_store.read_decoded(artifact)
        checkpoint("artifact_verified")
        if (
            verified.metadata.get("capture", {}).get("capture_id") != capture.capture_id
            or verified.metadata.get("source", {}).get("hdf5_content_sha256")
            != raw_sha256
        ):
            raise KnmiRtcorArtifactError("published artifact binding differs")
        checkpoint("canonical_commit_before")
        evidence_receipt = evidence_store.commit(_run_mapping(
            capture,
            decoder_job_id=decoder_job_id,
            attempt_id=attempt_id,
            evaluated_at=evaluation,
            outcome="decoded_complete",
            reason_code="decoded_complete",
            artifact=artifact,
            frame=frame,
        ))
        return KnmiRtcorDecodeResult(
            outcome="decoded_complete",
            reason_code="decoded_complete",
            evidence_receipt=evidence_receipt,
            artifact=artifact,
        )
    except _RejectedDecode as rejected:
        checkpoint("canonical_rejection_commit_before")
        evidence_receipt = evidence_store.commit(_run_mapping(
            capture,
            decoder_job_id=decoder_job_id,
            attempt_id=attempt_id,
            evaluated_at=evaluation,
            outcome=rejected.outcome,
            reason_code=rejected.reason_code,
        ))
        return KnmiRtcorDecodeResult(
            outcome=rejected.outcome,
            reason_code=rejected.reason_code,
            evidence_receipt=evidence_receipt,
            artifact=None,
        )


__all__ = [
    "ADJUSTMENT_REAPPLICATION_POLICY",
    "ADJUSTMENT_FORMULA",
    "ARTIFACT_SCHEMA",
    "DECODER_CONFIG_SHA256",
    "DECODER_IDENTITY",
    "DECODER_VERSION",
    "EXPECTED_COLUMNS",
    "EXPECTED_ROWS",
    "KnmiRtcorArtifactCapacityError",
    "KnmiRtcorArtifactError",
    "KnmiRtcorArtifactRef",
    "KnmiRtcorCaptureMissingError",
    "KnmiRtcorDecodeResult",
    "KnmiRtcorDecodeDeadlineError",
    "KnmiRtcorDerivedArtifactStore",
    "KnmiRtcorQuantitativeError",
    "KnmiRtcorRasterArtifact",
    "PRECIP_FORMULA",
    "PRECIPITATION_UPSTREAM_ADJUSTMENT_STATE",
    "QUALITY_FORMULA",
    "decode_knmi_rtcor_capture",
]
