"""Fail-closed quantitative decoder for one exact DWD RV schema-3 capture.

This module is deliberately isolated from the legacy component decoder and from
operational admission.  It reads one ``rv_tar`` member from
:class:`~storm_tracker_worker.raw_artifacts.RawArtifactStore`, validates the
documented ODIM/RV contract, publishes one immutable native raster candidate,
and only then appends a ``decoded_complete`` canonical evidence receipt.

No threshold, cell extraction, categorical intensity, or invented quality is
part of this boundary.  A rejected capture can only produce a payload-free,
non-meteorological canonical outcome.
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
import tarfile
import tempfile
import threading
import time
import zlib
from typing import Any, Callable, Mapping

import h5py
import numpy as np
from pyproj import CRS, Transformer
from pyproj.exceptions import CRSError, ProjError

from .canonical_evidence_store import (
    CanonicalEvidenceStore,
    DecoderRunReceipt,
    EVIDENCE_CONTRACT,
)
from .dwd_rv_capture_handoff import (
    DwdRvCaptureHandoffError,
    HANDOFF_ADAPTER_ID,
    HANDOFF_ADAPTER_VERSION,
    HANDOFF_CONFIG_SHA256,
    HANDOFF_TARGET_COLLECTOR_VERSION,
    validate_decoder_handoff_capture,
)
from .raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RawArtifactCaptureReceipt,
    RawArtifactIntegrityError,
    RawArtifactReceipt,
    RawArtifactStore,
)


DWD_RV_PROVIDER = "dwd_radolan"
DWD_RV_ARCHIVE_PRODUCT = "dwd_open_data_composite_rv_tar"
DWD_RV_CAPTURE_PRODUCT = "dwd_open_data_composite_rv_capture"
DWD_RV_ROLE = "rv_tar"
DWD_RV_COLLECTOR = "dwd_open_data_composite_rv"
DWD_RV_COLLECTOR_VERSION = "worker-dwd-rv-exact-raw-v1"

DECODER_IDENTITY = "dwd-rv-odim-hdf5-quantitative"
DECODER_VERSION = "dwd-rv-quantitative-v1"
ARTIFACT_SCHEMA = "stv3-worker-dwd-rv-quantitative-raster-v1"
ARTIFACT_MAGIC = b"STV3-DWD-RV-Q1\n"
ARTIFACT_HANDLE_PREFIX = "artifact:dwd-rv-quantitative:"

MAX_ARCHIVE_BYTES = 8 * 1024 * 1024
MAX_ARCHIVE_MEMBERS = 64
MAX_HDF_BYTES = 40 * 1024 * 1024
MAX_HDF_OBJECTS = 32
MAX_HEADER_BYTES = 64 * 1024
MAX_UNCOMPRESSED_ARRAY_BYTES = 24 * 1024 * 1024
MAX_ARTIFACT_BYTES = 24 * 1024 * 1024
DEFAULT_MAX_DERIVED_BYTES = 2 * 1024 * 1024 * 1024
DEFAULT_MAX_DERIVED_OBJECTS = 4096
DEFAULT_MAX_STAGING_OBJECTS = 8
DECODE_TOTAL_DEADLINE_SECONDS = 60.0
LOCK_POLL_SECONDS = 0.01
STORE_PROFILE_FILENAME = ".store-profile.json"
STORE_PROFILE_SCHEMA = "stv3-worker-dwd-rv-derived-store-profile-v1"

EXPECTED_WIDTH = 1100
EXPECTED_HEIGHT = 1200
EXPECTED_SCALE_METRES = 1000.0
EXPECTED_ACCUMULATION_SECONDS = 300
EXPECTED_GAIN = 0.001
EXPECTED_NODATA = 2**32 - 1
EXPECTED_UNDETECT = 0
EXPECTED_VALUE_MAX_MM = 4.3e6

_ARCHIVE_MEMBER_RE = re.compile(
    r"^composite_rv_(?P<date>\d{8})_(?P<time>\d{4})_"
    r"(?P<lead>\d{3})-hd5$"
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_HANDLE_RE = re.compile(
    rf"^{re.escape(ARTIFACT_HANDLE_PREFIX)}(?P<sha>[0-9a-f]{{64}})$"
)
_ARTIFACT_FILENAME_RE = re.compile(r"^(?P<sha>[0-9a-f]{64})\.drvq$")
_STAGING_FILENAME_RE = re.compile(
    r"^\.(?P<sha>[0-9a-f]{64})\.[A-Za-z0-9_-]{1,128}$"
)
_DECODER_JOB_RE = re.compile(r"^decoder-job-[0-9a-f]{64}$")
_ATTEMPT_RE = re.compile(r"^decoder-attempt-[0-9a-f]{64}$")
_STORE_LOCKS_GUARD = threading.Lock()
_STORE_THREAD_LOCKS: dict[str, threading.RLock] = {}

_EXPECTED_PROJECTION_NUMERIC = {
    "lat_ts": 60.0,
    "lat_0": 90.0,
    "lon_0": 10.0,
    "x_0": 543196.835217764,
    "y_0": 3622588.861931002,
    "a": 6378137.0,
    "b": 6356752.3142451802,
}
_EXPECTED_PROJECTION_TEXT = {"proj": "stere", "units": "m"}
_EXPECTED_PROJECTION_FLAGS = {"no_defs"}

_DECODER_CONFIG = {
    "accumulation_seconds": EXPECTED_ACCUMULATION_SECONDS,
    "artifact_schema": ARTIFACT_SCHEMA,
    "axis_order": "yx",
    "capture_product": DWD_RV_CAPTURE_PRODUCT,
    "capture_profiles": {
        "legacy_exact_raw": DWD_RV_COLLECTOR_VERSION,
        "raw_v2_observation_handoff": {
            "adapter_id": HANDOFF_ADAPTER_ID,
            "adapter_version": HANDOFF_ADAPTER_VERSION,
            "collector_version": HANDOFF_TARGET_COLLECTOR_VERSION,
            "config_sha256": HANDOFF_CONFIG_SHA256,
        },
    },
    "conventions": "ODIM_H5/V2_3",
    "data_dtype": "<u4",
    "dataset_product": "MAX",
    "decoder_identity": DECODER_IDENTITY,
    "decoder_version": DECODER_VERSION,
    "grid": {
        "height": EXPECTED_HEIGHT,
        "width": EXPECTED_WIDTH,
        "xscale_m": EXPECTED_SCALE_METRES,
        "yscale_m": EXPECTED_SCALE_METRES,
    },
    "member_role": DWD_RV_ROLE,
    "object": "COMP",
    "prodname": "RV_top_view",
    "provider": DWD_RV_PROVIDER,
    "quantity": "ACRR",
    "resource_contract": {
        "cooperative_deadline_seconds": DECODE_TOTAL_DEADLINE_SECONDS,
        "hard_c_or_rss_isolation": False,
        "max_artifact_bytes": MAX_ARTIFACT_BYTES,
        "max_artifact_objects": DEFAULT_MAX_DERIVED_OBJECTS,
        "max_staging_objects": DEFAULT_MAX_STAGING_OBJECTS,
        "max_uncompressed_array_bytes": MAX_UNCOMPRESSED_ARRAY_BYTES,
    },
    "unit": "mm",
}


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


DECODER_CONFIG_SHA256 = hashlib.sha256(
    _canonical_json(_DECODER_CONFIG)
).hexdigest()


class DwdRvQuantitativeError(RuntimeError):
    """Base failure for the isolated quantitative boundary."""


class DwdRvCaptureMissingError(DwdRvQuantitativeError):
    """The requested schema-3 capture does not exist."""


class DwdRvArtifactError(DwdRvQuantitativeError):
    """The derived artifact store rejected or failed an operation."""


class DwdRvArtifactCapacityError(DwdRvArtifactError):
    """Publishing would cross a hard derived-artifact byte boundary."""


class DwdRvDecodeDeadlineError(DwdRvQuantitativeError):
    """The cooperative decoder deadline expired at a bounded checkpoint."""


class _RejectedDecode(Exception):
    def __init__(self, outcome: str, reason_code: str) -> None:
        self.outcome = outcome
        self.reason_code = reason_code
        super().__init__(reason_code)


@dataclass(frozen=True, slots=True)
class DwdRvArtifactRef:
    """Opaque, content-addressed reference without a filesystem path."""

    handle: str
    sha256: str
    size_bytes: int


@dataclass(frozen=True, slots=True)
class DwdRvRasterArtifact:
    """Verified decoded arrays returned only for a successful artifact."""

    metadata: Mapping[str, Any]
    raw_values: np.ndarray
    accumulation_mm: np.ndarray
    valid_mask: np.ndarray
    nodata_mask: np.ndarray
    undetect_mask: np.ndarray


@dataclass(frozen=True, slots=True)
class DwdRvDecodeResult:
    """Canonical outcome; ``artifact`` is absent for every rejection."""

    outcome: str
    reason_code: str
    evidence_receipt: DecoderRunReceipt
    artifact: DwdRvArtifactRef | None


@dataclass(frozen=True, slots=True)
class _SelectedMember:
    name: str
    payload: bytes
    content_sha256: str
    product_time: float


@dataclass(frozen=True, slots=True)
class _DecodedFrame:
    raw_values: np.ndarray
    accumulation_mm: np.ndarray
    valid_mask: np.ndarray
    nodata_mask: np.ndarray
    undetect_mask: np.ndarray
    product_time: float
    interval_start: float
    interval_end: float
    gain: float
    offset: float
    nodata: int
    undetect: int
    projdef: str
    xscale: float
    yscale: float
    corners: Mapping[str, float]
    bbox: tuple[float, float, float, float]
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
            raise DwdRvDecodeDeadlineError(
                "DWD RV decoder clock is invalid before artifact store lock"
            )
        default_deadline = initial_monotonic + DECODE_TOTAL_DEADLINE_SECONDS

        def active_checkpoint(stage: str) -> None:
            current = time.monotonic()
            if not math.isfinite(current) or current >= default_deadline:
                raise DwdRvDecodeDeadlineError(
                    f"DWD RV decoder deadline expired at {stage}"
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
                raise DwdRvArtifactError("derived artifact store lock is unsafe")
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
        except (DwdRvArtifactError, DwdRvDecodeDeadlineError, TimeoutError):
            raise
        except OSError as exc:
            raise DwdRvArtifactError("derived artifact store lock failed") from exc
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


class DwdRvDerivedArtifactStore:
    """Bounded immutable object store for deterministic DWD RV rasters."""

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
        self.objects_path = self.root / "objects"
        self.staging_path = self.root / "staging"
        self.profile_path = self.root / STORE_PROFILE_FILENAME
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
                raise DwdRvArtifactError("derived artifact layout is unsafe")
        for path in (self.objects_path, self.staging_path):
            if path.exists() or path.is_symlink():
                info = path.lstat()
                if path.is_symlink() or not stat.S_ISDIR(info.st_mode):
                    raise DwdRvArtifactError("derived artifact layout is unsafe")
        self.root.mkdir(parents=True, exist_ok=True)
        self.objects_path.mkdir(mode=0o700, exist_ok=True)
        self.staging_path.mkdir(mode=0o700, exist_ok=True)
        for path in (self.root, self.objects_path, self.staging_path):
            info = path.lstat()
            if path.is_symlink() or not stat.S_ISDIR(info.st_mode):
                raise DwdRvArtifactError("derived artifact layout is unsafe")

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
            "filename_suffix": ".drvq",
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
            raise DwdRvArtifactCapacityError(
                "derived artifact staging count blocks store profile publication"
            )
        if total + len(expected) > self.max_total_bytes:
            raise DwdRvArtifactCapacityError(
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
            actual = self._verify_regular_bytes(
                self.profile_path,
                len(expected),
                hashlib.sha256(expected).hexdigest(),
                allowed_parent=self.root,
                return_bytes=True,
            )
        except (DwdRvArtifactError, FileNotFoundError, OSError) as exc:
            raise DwdRvArtifactError(
                "derived artifact store profile is missing, unsafe, or differs "
                "from the immutable limits"
            ) from exc
        if actual != expected:
            raise DwdRvArtifactError(
                "derived artifact store was opened with a different immutable profile"
            )

    def publish(
        self,
        payload: bytes,
        *,
        checkpoint: Callable[[str], None] | None = None,
    ) -> DwdRvArtifactRef:
        if not isinstance(payload, bytes):
            raise TypeError("derived artifact payload must be exact bytes")
        if not 0 < len(payload) <= MAX_ARTIFACT_BYTES:
            raise DwdRvArtifactCapacityError("derived artifact exceeds object bound")
        digest = hashlib.sha256(payload).hexdigest()
        reference = DwdRvArtifactRef(
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
        reference: DwdRvArtifactRef,
    ) -> DwdRvArtifactRef:
        digest = reference.sha256
        target = self.objects_path / f"{digest}.drvq"
        total, object_count, staging_count = self._scan_layout()
        if total > self.max_total_bytes:
            raise DwdRvArtifactCapacityError(
                "derived artifact store capacity is already exceeded"
            )
        if target.exists():
            self._verify_path(target, reference)
            return reference
        if object_count >= self.max_objects:
            raise DwdRvArtifactCapacityError(
                "derived artifact object count would be exceeded"
            )
        if staging_count >= self.max_staging_objects:
            raise DwdRvArtifactCapacityError(
                "derived artifact staging count would be exceeded"
            )
        if total + len(payload) > self.max_total_bytes:
            raise DwdRvArtifactCapacityError(
                "derived artifact store capacity would be exceeded"
            )
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
            self._verify_regular_bytes(
                temporary_path,
                len(payload),
                digest,
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

    def read(self, reference: DwdRvArtifactRef) -> bytes:
        if not isinstance(reference, DwdRvArtifactRef):
            raise TypeError("artifact reference has the wrong type")
        match = _HANDLE_RE.fullmatch(reference.handle)
        if (
            match is None
            or match.group("sha") != reference.sha256
            or not _SHA256_RE.fullmatch(reference.sha256)
        ):
            raise DwdRvArtifactError("artifact reference is invalid")
        self._validate_store_profile()
        target = self.objects_path / f"{reference.sha256}.drvq"
        return self._verify_path(target, reference, return_bytes=True)

    def read_decoded(self, reference: DwdRvArtifactRef) -> DwdRvRasterArtifact:
        return _parse_artifact(self.read(reference))

    def object_count(self) -> int:
        with _exclusive_store_lock(self.root):
            self._validate_store_profile()
            _total, object_count, _staging_count = self._scan_layout()
            return object_count

    def _verify_path(
        self,
        path: Path,
        reference: DwdRvArtifactRef,
        *,
        return_bytes: bool = False,
    ) -> bytes:
        result = self._verify_regular_bytes(
            path,
            reference.size_bytes,
            reference.sha256,
            allowed_parent=self.objects_path,
            return_bytes=return_bytes,
        )
        return result or b""

    @staticmethod
    def _verify_regular_bytes(
        path: Path,
        expected_size: int,
        expected_sha256: str,
        *,
        allowed_parent: Path,
        return_bytes: bool = False,
    ) -> bytes | None:
        if path.parent.resolve() != allowed_parent.resolve():
            raise DwdRvArtifactError("artifact path escapes its store")
        info = path.lstat()
        if path.is_symlink() or not stat.S_ISREG(info.st_mode):
            raise DwdRvArtifactError("artifact object is not a regular file")
        if info.st_size != expected_size or not 0 < expected_size <= MAX_ARTIFACT_BYTES:
            raise DwdRvArtifactError("artifact object size changed")
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
        chunks: list[bytes] | None = [] if return_bytes else None
        digest = hashlib.sha256()
        total = 0
        try:
            opened = os.fstat(descriptor)
            if not stat.S_ISREG(opened.st_mode) or opened.st_size != expected_size:
                raise DwdRvArtifactError("artifact changed while opening")
            while total < expected_size:
                chunk = os.read(descriptor, min(1024 * 1024, expected_size - total))
                if not chunk:
                    break
                total += len(chunk)
                digest.update(chunk)
                if chunks is not None:
                    chunks.append(chunk)
            if os.read(descriptor, 1):
                raise DwdRvArtifactError("artifact contains trailing bytes")
        finally:
            os.close(descriptor)
        if total != expected_size or not hmac.compare_digest(
            digest.hexdigest(), expected_sha256
        ):
            raise DwdRvArtifactError("artifact checksum does not match")
        after = path.lstat()
        if (
            after.st_size != opened.st_size
            or (after.st_dev, after.st_ino) != (opened.st_dev, opened.st_ino)
        ):
            raise DwdRvArtifactError("artifact path changed while reading")
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
                    raise DwdRvArtifactCapacityError(
                        f"derived artifact {entry_kind} count exceeds its bound"
                    )
                if filename_pattern.fullmatch(path.name) is None:
                    raise DwdRvArtifactError(
                        f"derived artifact store contains unknown {entry_kind} entry"
                    )
                info = path.lstat()
                if path.is_symlink() or not stat.S_ISREG(info.st_mode):
                    raise DwdRvArtifactError("artifact store contains unsafe entry")
                identity: tuple[int, int] | tuple[str, str]
                identity = (
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


def _ascii(value: object, field: str, maximum: int = 512) -> str:
    if isinstance(value, np.bytes_):
        value = bytes(value)
    if isinstance(value, bytes):
        try:
            result = value.decode("ascii")
        except UnicodeDecodeError as exc:
            raise _RejectedDecode("unsupported", "unsupported:odim_text") from exc
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
        raise _RejectedDecode("unsupported", "unsupported:odim_text")
    return result


def _finite_number(value: object, field: str) -> float:
    array = np.asarray(value)
    if array.shape != () or array.dtype.kind not in "iuf":
        raise _RejectedDecode("incomplete", "incomplete:hdf5_metadata")
    result = float(array.item())
    if not math.isfinite(result):
        raise _RejectedDecode("unsupported", "unsupported:numeric_metadata")
    return result


def _integer(value: object, field: str) -> int:
    number = _finite_number(value, field)
    if not number.is_integer():
        raise _RejectedDecode("unsupported", "unsupported:grid_contract")
    return int(number)


def _required_attr(attributes: Any, key: str, field: str) -> object:
    if key not in attributes:
        raise _RejectedDecode("incomplete", "incomplete:hdf5_metadata")
    return attributes[key]


def _utc_timestamp(date_text: str, time_text: str) -> float:
    try:
        parsed = datetime.strptime(date_text + time_text, "%Y%m%d%H%M%S")
    except ValueError as exc:
        raise _RejectedDecode("unsupported", "unsupported:time_contract") from exc
    return parsed.replace(tzinfo=timezone.utc).timestamp()


def _parse_projection(projdef: str) -> CRS:
    tokens: dict[str, str | None] = {}
    for token in projdef.split():
        if not token.startswith("+") or len(token) < 2:
            raise _RejectedDecode("unsupported", "unsupported:crs_profile")
        raw = token[1:]
        key, separator, value = raw.partition("=")
        if not key or key in tokens:
            raise _RejectedDecode("unsupported", "unsupported:crs_profile")
        tokens[key] = value if separator else None
    if set(tokens) != (
        set(_EXPECTED_PROJECTION_NUMERIC)
        | set(_EXPECTED_PROJECTION_TEXT)
        | _EXPECTED_PROJECTION_FLAGS
    ):
        raise _RejectedDecode("unsupported", "unsupported:crs_profile")
    for key, expected in _EXPECTED_PROJECTION_TEXT.items():
        if tokens[key] != expected:
            raise _RejectedDecode("unsupported", "unsupported:crs_profile")
    for key in _EXPECTED_PROJECTION_FLAGS:
        if tokens[key] is not None:
            raise _RejectedDecode("unsupported", "unsupported:crs_profile")
    for key, expected in _EXPECTED_PROJECTION_NUMERIC.items():
        try:
            actual = float(str(tokens[key]))
        except ValueError as exc:
            raise _RejectedDecode("unsupported", "unsupported:crs_profile") from exc
        tolerance = 1e-6 if key in {"x_0", "y_0", "b"} else 1e-9
        if not math.isfinite(actual) or not math.isclose(
            actual, expected, rel_tol=0.0, abs_tol=tolerance
        ):
            raise _RejectedDecode("unsupported", "unsupported:crs_profile")
    try:
        crs = CRS.from_user_input(projdef)
    except CRSError as exc:
        raise _RejectedDecode("unsupported", "unsupported:crs_profile") from exc
    if not crs.is_projected or any(
        axis.unit_name.lower() not in {"metre", "meter"} for axis in crs.axis_info
    ):
        raise _RejectedDecode("unsupported", "unsupported:crs_profile")
    return crs


def _validate_grid(
    where: Any,
    shape: tuple[int, int],
) -> tuple[
    str,
    float,
    float,
    dict[str, float],
    tuple[float, float, float, float],
    str,
    str,
]:
    width = _integer(_required_attr(where, "xsize", "where.xsize"), "xsize")
    height = _integer(_required_attr(where, "ysize", "where.ysize"), "ysize")
    xscale = _finite_number(
        _required_attr(where, "xscale", "where.xscale"), "xscale"
    )
    yscale = _finite_number(
        _required_attr(where, "yscale", "where.yscale"), "yscale"
    )
    if (
        (width, height) != (EXPECTED_WIDTH, EXPECTED_HEIGHT)
        or shape != (EXPECTED_HEIGHT, EXPECTED_WIDTH)
        or xscale != EXPECTED_SCALE_METRES
        or yscale != EXPECTED_SCALE_METRES
    ):
        raise _RejectedDecode("unsupported", "unsupported:grid_contract")
    projdef = _ascii(
        _required_attr(where, "projdef", "where.projdef"), "where.projdef"
    )
    crs = _parse_projection(projdef)
    corners = {
        key: _finite_number(_required_attr(where, key, f"where.{key}"), key)
        for key in (
            "LL_lon", "LL_lat", "LR_lon", "LR_lat",
            "UL_lon", "UL_lat", "UR_lon", "UR_lat",
        )
    }
    for key, value in corners.items():
        if key.endswith("_lon") and not -180.0 <= value <= 180.0:
            raise _RejectedDecode("unsupported", "unsupported:grid_corners")
        if key.endswith("_lat") and not -90.0 <= value <= 90.0:
            raise _RejectedDecode("unsupported", "unsupported:grid_corners")
    try:
        transform = Transformer.from_crs("EPSG:4326", crs, always_xy=True)
        projected = {
            name: transform.transform(corners[f"{name}_lon"], corners[f"{name}_lat"])
            for name in ("LL", "LR", "UL", "UR")
        }
    except (CRSError, ProjError) as exc:
        raise _RejectedDecode("unsupported", "unsupported:grid_corners") from exc
    if any(not math.isfinite(value) for point in projected.values() for value in point):
        raise _RejectedDecode("unsupported", "unsupported:grid_corners")
    left = (projected["LL"][0] + projected["UL"][0]) / 2.0
    right = (projected["LR"][0] + projected["UR"][0]) / 2.0
    bottom = (projected["LL"][1] + projected["LR"][1]) / 2.0
    top = (projected["UL"][1] + projected["UR"][1]) / 2.0
    rectangular_errors = (
        abs(projected["LL"][0] - projected["UL"][0]),
        abs(projected["LR"][0] - projected["UR"][0]),
        abs(projected["LL"][1] - projected["LR"][1]),
        abs(projected["UL"][1] - projected["UR"][1]),
        abs((right - left) - width * xscale),
        abs((top - bottom) - height * yscale),
    )
    if max(rectangular_errors) > 0.25:
        raise _RejectedDecode("unsupported", "unsupported:grid_corners")
    minimum_x = round(left, 6)
    maximum_x = round(right, 6)
    minimum_y = round(bottom, 6)
    maximum_y = round(top, 6)
    grid_definition = {
        "axis_order": "yx",
        "corner_coordinates_wgs84": corners,
        "height": height,
        "projdef": projdef,
        "row_direction": "north_to_south",
        "width": width,
        "xscale_m": xscale,
        "yscale_m": yscale,
    }
    coverage_definition = {
        "kind": "native_bbox",
        "maximum_x": maximum_x,
        "maximum_y": maximum_y,
        "minimum_x": minimum_x,
        "minimum_y": minimum_y,
        "projdef_sha256": hashlib.sha256(projdef.encode("ascii")).hexdigest(),
    }
    return (
        projdef,
        xscale,
        yscale,
        corners,
        (minimum_x, minimum_y, maximum_x, maximum_y),
        hashlib.sha256(_canonical_json(grid_definition)).hexdigest(),
        hashlib.sha256(_canonical_json(coverage_definition)).hexdigest(),
    )


def _source_identifiers(value: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for item in value.split(","):
        key, separator, member = item.partition(":")
        if not separator or not key or not member or key in result:
            raise _RejectedDecode("unsupported", "unsupported:source_contract")
        result[key] = member
    if result.get("ORG") != "78" or result.get("CTY") != "616":
        raise _RejectedDecode("unsupported", "unsupported:source_contract")
    return result


def _required_group(root: Any, path: str) -> h5py.Group:
    try:
        link = root.get(path, getlink=True)
        value = root[path]
    except (KeyError, OSError) as exc:
        raise _RejectedDecode("incomplete", "incomplete:hdf5_structure") from exc
    if not isinstance(link, h5py.HardLink) or not isinstance(value, h5py.Group):
        raise _RejectedDecode("unsupported", "unsupported:hdf5_links")
    return value


def _required_dataset(root: Any, path: str) -> h5py.Dataset:
    try:
        link = root.get(path, getlink=True)
        value = root[path]
    except (KeyError, OSError) as exc:
        raise _RejectedDecode("incomplete", "incomplete:hdf5_structure") from exc
    if not isinstance(link, h5py.HardLink) or not isinstance(value, h5py.Dataset):
        raise _RejectedDecode("unsupported", "unsupported:hdf5_links")
    return value


def _decode_hdf(member: _SelectedMember) -> _DecodedFrame:
    try:
        dataset = h5py.File(io.BytesIO(member.payload), "r")
    except OSError as exc:
        raise _RejectedDecode("decode_error", "decode_error:hdf5") from exc
    with dataset:
        object_count = 0

        def count_object(_name: str, _value: Any) -> None:
            nonlocal object_count
            object_count += 1
            if object_count > MAX_HDF_OBJECTS:
                raise _RejectedDecode("unsupported", "unsupported:hdf5_object_count")

        dataset.visititems(count_object)
        conventions = _ascii(
            _required_attr(dataset.attrs, "Conventions", "Conventions"),
            "Conventions",
        )
        top_what = _required_group(dataset, "what")
        where = _required_group(dataset, "where")
        frame_what = _required_group(dataset, "dataset1/what")
        data_what = _required_group(dataset, "dataset1/data1/what")
        data = _required_dataset(dataset, "dataset1/data1/data")
        dataset_groups = sorted(
            key for key in dataset.keys() if re.fullmatch(r"dataset\d+", key)
        )
        data_groups = sorted(
            key for key in dataset["dataset1"].keys() if re.fullmatch(r"data\d+", key)
        )
        if dataset_groups != ["dataset1"] or data_groups != ["data1"]:
            raise _RejectedDecode("unsupported", "unsupported:hdf5_multiplicity")
        if (
            data.external is not None
            or data.is_virtual
            or data.ndim != 2
            or data.dtype != np.dtype("uint32")
            or data.size != EXPECTED_WIDTH * EXPECTED_HEIGHT
            or data.compression not in {None, "gzip"}
            or data.scaleoffset is not None
        ):
            raise _RejectedDecode("unsupported", "unsupported:data_encoding")
        object_value = _ascii(
            _required_attr(top_what.attrs, "object", "what.object"), "what.object"
        )
        version = _ascii(
            _required_attr(top_what.attrs, "version", "what.version"), "what.version"
        )
        source = _ascii(
            _required_attr(top_what.attrs, "source", "what.source"), "what.source"
        )
        top_date = _ascii(
            _required_attr(top_what.attrs, "date", "what.date"), "what.date"
        )
        top_time = _ascii(
            _required_attr(top_what.attrs, "time", "what.time"), "what.time"
        )
        product = _ascii(
            _required_attr(frame_what.attrs, "product", "dataset1.what.product"),
            "dataset1.what.product",
        )
        prodname = _ascii(
            _required_attr(frame_what.attrs, "prodname", "dataset1.what.prodname"),
            "dataset1.what.prodname",
        )
        quantity = _ascii(
            _required_attr(data_what.attrs, "quantity", "data1.what.quantity"),
            "data1.what.quantity",
        )
        if (
            conventions != "ODIM_H5/V2_3"
            or object_value != "COMP"
            or version != "H5rad 2.3"
            or product != "MAX"
            or prodname != "RV_top_view"
            or quantity != "ACRR"
        ):
            raise _RejectedDecode("unsupported", "unsupported:rv_odim_profile")
        _source_identifiers(source)
        if "how" in dataset:
            how = _required_group(dataset, "how")
            if "simulated" in how.attrs and _ascii(
                how.attrs["simulated"], "how.simulated"
            ) != "False":
                raise _RejectedDecode("unsupported", "unsupported:simulated_product")
        start_date = _ascii(
            _required_attr(frame_what.attrs, "startdate", "startdate"), "startdate"
        )
        start_time = _ascii(
            _required_attr(frame_what.attrs, "starttime", "starttime"), "starttime"
        )
        end_date = _ascii(
            _required_attr(frame_what.attrs, "enddate", "enddate"), "enddate"
        )
        end_time = _ascii(
            _required_attr(frame_what.attrs, "endtime", "endtime"), "endtime"
        )
        interval_start = _utc_timestamp(start_date, start_time)
        interval_end = _utc_timestamp(end_date, end_time)
        top_timestamp = _utc_timestamp(top_date, top_time)
        if (
            interval_end - interval_start != EXPECTED_ACCUMULATION_SECONDS
            or interval_end != top_timestamp
            or interval_end != member.product_time
        ):
            raise _RejectedDecode("unsupported", "unsupported:time_contract")
        gain = _finite_number(
            _required_attr(data_what.attrs, "gain", "data1.what.gain"), "gain"
        )
        offset = _finite_number(
            _required_attr(data_what.attrs, "offset", "data1.what.offset"), "offset"
        )
        nodata_number = _finite_number(
            _required_attr(data_what.attrs, "nodata", "data1.what.nodata"), "nodata"
        )
        undetect_number = _finite_number(
            _required_attr(data_what.attrs, "undetect", "data1.what.undetect"),
            "undetect",
        )
        if (
            not math.isclose(gain, EXPECTED_GAIN, rel_tol=0.0, abs_tol=1e-9)
            or not math.isclose(offset, -gain, rel_tol=0.0, abs_tol=1e-12)
            or not nodata_number.is_integer()
            or int(nodata_number) != EXPECTED_NODATA
            or not undetect_number.is_integer()
            or int(undetect_number) != EXPECTED_UNDETECT
        ):
            raise _RejectedDecode("unsupported", "unsupported:value_encoding")
        grid = _validate_grid(where.attrs, tuple(data.shape))
        try:
            raw = np.asarray(data[...], dtype=np.dtype("<u4"), order="C")
        except (OSError, ValueError) as exc:
            raise _RejectedDecode("decode_error", "decode_error:data_read") from exc
        raw = np.ascontiguousarray(raw, dtype=np.dtype("<u4"))
        nodata_mask = np.ascontiguousarray(raw == EXPECTED_NODATA, dtype=np.uint8)
        undetect_mask = np.ascontiguousarray(raw == EXPECTED_UNDETECT, dtype=np.uint8)
        valid_mask = np.ascontiguousarray(
            (nodata_mask == 0) & (undetect_mask == 0), dtype=np.uint8
        )
        accumulation = np.zeros(raw.shape, dtype=np.dtype("<f8"))
        valid = valid_mask.astype(bool)
        accumulation[valid] = raw[valid].astype(np.float64) * gain + offset
        if (
            not np.isfinite(accumulation[valid]).all()
            or (accumulation[valid] < -1e-12).any()
            or (accumulation[valid] > EXPECTED_VALUE_MAX_MM + EXPECTED_GAIN).any()
            or np.any(nodata_mask & undetect_mask)
            or not np.all((valid_mask + nodata_mask + undetect_mask) == 1)
        ):
            raise _RejectedDecode("unsupported", "unsupported:decoded_values")
        return _DecodedFrame(
            raw_values=raw,
            accumulation_mm=accumulation,
            valid_mask=valid_mask,
            nodata_mask=nodata_mask,
            undetect_mask=undetect_mask,
            product_time=interval_end,
            interval_start=interval_start,
            interval_end=interval_end,
            gain=gain,
            offset=offset,
            nodata=EXPECTED_NODATA,
            undetect=EXPECTED_UNDETECT,
            projdef=grid[0],
            xscale=grid[1],
            yscale=grid[2],
            corners=grid[3],
            bbox=grid[4],
            grid_definition_sha256=grid[5],
            coverage_definition_sha256=grid[6],
        )


def _select_archive_member(archive_payload: bytes) -> _SelectedMember:
    if not 0 < len(archive_payload) <= MAX_ARCHIVE_BYTES:
        raise _RejectedDecode("unsupported", "unsupported:archive_size")
    try:
        archive = tarfile.open(fileobj=io.BytesIO(archive_payload), mode="r:")
    except tarfile.TarError as exc:
        raise _RejectedDecode("decode_error", "decode_error:archive") from exc
    with archive:
        try:
            members = archive.getmembers()
        except tarfile.TarError as exc:
            raise _RejectedDecode("decode_error", "decode_error:archive") from exc
        if not 1 <= len(members) <= MAX_ARCHIVE_MEMBERS:
            raise _RejectedDecode("unsupported", "unsupported:archive_members")
        candidates: list[tuple[tarfile.TarInfo, re.Match[str]]] = []
        seen_names: set[str] = set()
        base_stamp: tuple[str, str] | None = None
        for member in members:
            name = str(member.name)
            match = _ARCHIVE_MEMBER_RE.fullmatch(name)
            if (
                match is None
                or name in seen_names
                or "/" in name
                or "\\" in name
                or not member.isfile()
                or member.size <= 0
                or member.size > MAX_HDF_BYTES
            ):
                raise _RejectedDecode("unsupported", "unsupported:archive_members")
            seen_names.add(name)
            stamp = (match.group("date"), match.group("time"))
            if base_stamp is None:
                base_stamp = stamp
            if stamp != base_stamp:
                raise _RejectedDecode("unsupported", "unsupported:archive_members")
            lead = int(match.group("lead"))
            if lead > 120 or lead % 5:
                raise _RejectedDecode("unsupported", "unsupported:archive_members")
            if lead == 0:
                candidates.append((member, match))
        if len(candidates) != 1:
            raise _RejectedDecode("incomplete", "incomplete:plus000_member")
        member, match = candidates[0]
        try:
            stream = archive.extractfile(member)
            if stream is None:
                raise tarfile.ExtractError("member has no stream")
            payload = stream.read(MAX_HDF_BYTES + 1)
            trailing = stream.read(1)
        except (OSError, tarfile.TarError) as exc:
            raise _RejectedDecode("decode_error", "decode_error:archive_read") from exc
        if trailing or len(payload) != member.size or len(payload) > MAX_HDF_BYTES:
            raise _RejectedDecode("decode_error", "decode_error:archive_read")
        product_time = _utc_timestamp(
            match.group("date"), match.group("time") + "00"
        )
        return _SelectedMember(
            name=member.name,
            payload=payload,
            content_sha256=hashlib.sha256(payload).hexdigest(),
            product_time=product_time,
        )


def _artifact_bytes(
    capture: RawArtifactCaptureReceipt,
    archive_sha256: str,
    selected: _SelectedMember,
    frame: _DecodedFrame,
) -> bytes:
    arrays = (
        ("raw_values", frame.raw_values, "native_uint32_with_sentinels"),
        ("accumulation_mm", frame.accumulation_mm, "valid_pixels_only_invalid_fill_zero"),
        ("valid_mask", frame.valid_mask, "one_is_valid_measurement"),
        ("nodata_mask", frame.nodata_mask, "one_is_never_radiated"),
        ("undetect_mask", frame.undetect_mask, "one_is_below_detection_threshold"),
    )
    sections: list[dict[str, Any]] = []
    chunks: list[bytes] = []
    offset = 0
    for name, array, semantics in arrays:
        chunk = array.tobytes(order="C")
        sections.append({
            "dtype": array.dtype.str,
            "name": name,
            "nbytes": len(chunk),
            "offset": offset,
            "semantics": semantics,
            "sha256": hashlib.sha256(chunk).hexdigest(),
            "shape": [EXPECTED_HEIGHT, EXPECTED_WIDTH],
        })
        chunks.append(chunk)
        offset += len(chunk)
    uncompressed = b"".join(chunks)
    if not 0 < len(uncompressed) <= MAX_UNCOMPRESSED_ARRAY_BYTES:
        raise DwdRvArtifactCapacityError("decoded arrays exceed their byte bound")
    compressed = zlib.compress(uncompressed, level=6)
    minimum_x, minimum_y, maximum_x, maximum_y = frame.bbox
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
            "corner_coordinates_wgs84": dict(frame.corners),
            "coverage": {
                "kind": "native_bbox",
                "maximum_x": maximum_x,
                "maximum_y": maximum_y,
                "minimum_x": minimum_x,
                "minimum_y": minimum_y,
            },
            "coverage_definition_sha256": frame.coverage_definition_sha256,
            "definition_sha256": frame.grid_definition_sha256,
            "height": EXPECTED_HEIGHT,
            "projdef": frame.projdef,
            "row_direction": "north_to_south",
            "width": EXPECTED_WIDTH,
            "xscale_m": frame.xscale,
            "yscale_m": frame.yscale,
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
            "accumulation_seconds": EXPECTED_ACCUMULATION_SECONDS,
            "forecast_lead_minutes": 0,
            "gain": frame.gain,
            "interval_end": frame.interval_end,
            "interval_start": frame.interval_start,
            "nodata": frame.nodata,
            "offset": frame.offset,
            "product_time": frame.product_time,
            "quantity": "ACRR",
            "semantic_quantity": "precipitation_accumulation",
            "undetect": frame.undetect,
            "unit": "mm",
        },
        "schema": ARTIFACT_SCHEMA,
        "source": {
            "archive_content_sha256": archive_sha256,
            "hdf5_member_content_sha256": selected.content_sha256,
            "hdf5_member_name": selected.name,
            "role": DWD_RV_ROLE,
        },
    }
    header = _canonical_json(metadata)
    if len(header) > MAX_HEADER_BYTES:
        raise DwdRvArtifactCapacityError("derived artifact header exceeds its bound")
    result = ARTIFACT_MAGIC + struct.pack(">I", len(header)) + header + compressed
    if len(result) > MAX_ARTIFACT_BYTES:
        raise DwdRvArtifactCapacityError("derived artifact exceeds its byte bound")
    return result


def _bounded_decompress(payload: bytes, expected_bytes: int) -> bytes:
    if not 0 < expected_bytes <= MAX_UNCOMPRESSED_ARRAY_BYTES:
        raise DwdRvArtifactError("artifact uncompressed size is invalid")
    decompressor = zlib.decompressobj()
    try:
        decoded = decompressor.decompress(payload, expected_bytes + 1)
        if len(decoded) > expected_bytes or decompressor.unconsumed_tail:
            raise DwdRvArtifactError("artifact expands beyond its bound")
        decoded += decompressor.flush(expected_bytes + 1 - len(decoded))
    except zlib.error as exc:
        raise DwdRvArtifactError("artifact compression stream is invalid") from exc
    if (
        len(decoded) != expected_bytes
        or not decompressor.eof
        or decompressor.unused_data
    ):
        raise DwdRvArtifactError("artifact compression length does not match")
    return decoded


def _parse_artifact(payload: bytes) -> DwdRvRasterArtifact:
    if not isinstance(payload, bytes) or not 0 < len(payload) <= MAX_ARTIFACT_BYTES:
        raise DwdRvArtifactError("artifact payload is outside bounds")
    prefix_size = len(ARTIFACT_MAGIC) + 4
    if len(payload) < prefix_size or not payload.startswith(ARTIFACT_MAGIC):
        raise DwdRvArtifactError("artifact magic is invalid")
    header_size = struct.unpack(">I", payload[len(ARTIFACT_MAGIC):prefix_size])[0]
    if not 0 < header_size <= MAX_HEADER_BYTES or prefix_size + header_size >= len(payload):
        raise DwdRvArtifactError("artifact header size is invalid")
    header_bytes = payload[prefix_size:prefix_size + header_size]
    try:
        metadata = json.loads(header_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DwdRvArtifactError("artifact header is invalid JSON") from exc
    if not isinstance(metadata, dict) or _canonical_json(metadata) != header_bytes:
        raise DwdRvArtifactError("artifact header is not canonical")
    if metadata.get("schema") != ARTIFACT_SCHEMA:
        raise DwdRvArtifactError("artifact schema is unsupported")
    compressed = payload[prefix_size + header_size:]
    payload_meta = metadata.get("payload")
    if (
        not isinstance(payload_meta, dict)
        or payload_meta.get("codec") != "zlib"
        or payload_meta.get("level") != 6
        or payload_meta.get("compressed_bytes") != len(compressed)
        or payload_meta.get("compressed_sha256")
        != hashlib.sha256(compressed).hexdigest()
    ):
        raise DwdRvArtifactError("artifact compressed payload metadata differs")
    expected_uncompressed = payload_meta.get("uncompressed_bytes")
    if isinstance(expected_uncompressed, bool) or not isinstance(expected_uncompressed, int):
        raise DwdRvArtifactError("artifact uncompressed size is invalid")
    uncompressed = _bounded_decompress(compressed, expected_uncompressed)
    if payload_meta.get("uncompressed_sha256") != hashlib.sha256(uncompressed).hexdigest():
        raise DwdRvArtifactError("artifact array payload checksum differs")
    section_values = metadata.get("arrays")
    if not isinstance(section_values, list) or len(section_values) != 5:
        raise DwdRvArtifactError("artifact array manifest is invalid")
    expected = (
        ("raw_values", "<u4"),
        ("accumulation_mm", "<f8"),
        ("valid_mask", "|u1"),
        ("nodata_mask", "|u1"),
        ("undetect_mask", "|u1"),
    )
    arrays: dict[str, np.ndarray] = {}
    expected_offset = 0
    for section, (name, dtype_text) in zip(section_values, expected, strict=True):
        if not isinstance(section, dict):
            raise DwdRvArtifactError("artifact array section is invalid")
        dtype = np.dtype(dtype_text)
        expected_nbytes = EXPECTED_HEIGHT * EXPECTED_WIDTH * dtype.itemsize
        if (
            section.get("name") != name
            or section.get("dtype") != dtype.str
            or section.get("shape") != [EXPECTED_HEIGHT, EXPECTED_WIDTH]
            or section.get("offset") != expected_offset
            or section.get("nbytes") != expected_nbytes
        ):
            raise DwdRvArtifactError("artifact array layout differs")
        chunk = uncompressed[expected_offset:expected_offset + expected_nbytes]
        if len(chunk) != expected_nbytes or section.get("sha256") != hashlib.sha256(chunk).hexdigest():
            raise DwdRvArtifactError("artifact array checksum differs")
        array = np.frombuffer(chunk, dtype=dtype).reshape(EXPECTED_HEIGHT, EXPECTED_WIDTH)
        array.setflags(write=False)
        arrays[name] = array
        expected_offset += expected_nbytes
    if expected_offset != len(uncompressed):
        raise DwdRvArtifactError("artifact contains unbound array bytes")
    product = metadata.get("product")
    if not isinstance(product, dict):
        raise DwdRvArtifactError("artifact product metadata is invalid")
    raw = arrays["raw_values"]
    accumulation = arrays["accumulation_mm"]
    valid = arrays["valid_mask"]
    nodata = arrays["nodata_mask"]
    undetect = arrays["undetect_mask"]
    expected_nodata = np.asarray(raw == EXPECTED_NODATA, dtype=np.uint8)
    expected_undetect = np.asarray(raw == EXPECTED_UNDETECT, dtype=np.uint8)
    expected_valid = np.asarray(
        (expected_nodata == 0) & (expected_undetect == 0), dtype=np.uint8
    )
    if (
        not np.array_equal(nodata, expected_nodata)
        or not np.array_equal(undetect, expected_undetect)
        or not np.array_equal(valid, expected_valid)
        or np.any(accumulation[valid == 0] != 0.0)
    ):
        raise DwdRvArtifactError("artifact masks do not match native values")
    gain = float(product.get("gain", math.nan))
    offset = float(product.get("offset", math.nan))
    expected_accumulation = raw[valid == 1].astype(np.float64) * gain + offset
    if not np.array_equal(accumulation[valid == 1], expected_accumulation):
        raise DwdRvArtifactError("artifact accumulation does not match encoding")
    return DwdRvRasterArtifact(
        metadata=metadata,
        raw_values=raw,
        accumulation_mm=accumulation,
        valid_mask=valid,
        nodata_mask=nodata,
        undetect_mask=undetect,
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


def _run_mapping(
    capture: RawArtifactCaptureReceipt,
    *,
    decoder_job_id: str,
    attempt_id: str,
    evaluated_at: float,
    outcome: str,
    reason_code: str,
    artifact: DwdRvArtifactRef | None = None,
    frame: _DecodedFrame | None = None,
) -> dict[str, Any]:
    complete = outcome == "decoded_complete"
    if complete != (artifact is not None and frame is not None):
        raise RuntimeError("decoder result fields violate fail-closed invariant")
    coverage = None
    native_grid = None
    if frame is not None:
        minimum_x, minimum_y, maximum_x, maximum_y = frame.bbox
        native_grid = {
            "axis_order": "yx",
            "definition_sha256": frame.grid_definition_sha256,
            "height": EXPECTED_HEIGHT,
            "kind": "regular",
            "width": EXPECTED_WIDTH,
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
        "decoder": {
            "config_sha256": DECODER_CONFIG_SHA256,
            "identity": DECODER_IDENTITY,
            "version": DECODER_VERSION,
        },
        "decoder_job_id": decoder_job_id,
        "evaluated_at": evaluated_at,
        "native_crs": frame.projdef if frame is not None else None,
        "native_grid": native_grid,
        "outcome": outcome,
        "non_operational_candidate_artifact": (
            None if artifact is None else {
                "handle": artifact.handle,
                "sha256": artifact.sha256,
            }
        ),
        "product": capture.product,
        "product_time": capture.product_timestamp,
        "provider": capture.provider,
        "quantity": "precipitation_accumulation_5_minute" if complete else None,
        "reason_code": reason_code,
        "received_at": max(member.received_at for member in capture.members),
        "unit": "mm" if complete else None,
        "coverage": coverage,
    }


def _validate_raw_capture(
    capture: RawArtifactCaptureReceipt,
    receipts_by_role: Mapping[str, RawArtifactReceipt],
    payloads_by_role: Mapping[str, bytes],
) -> tuple[Any, bytes, str]:
    if capture.provider != DWD_RV_PROVIDER or capture.product != DWD_RV_CAPTURE_PRODUCT:
        raise _RejectedDecode("unsupported", "unsupported:capture_contract")
    if capture.product_timestamp is None:
        raise _RejectedDecode("incomplete", "incomplete:product_time")
    if len(capture.members) != 1 or capture.members[0].role != DWD_RV_ROLE:
        raise _RejectedDecode("incomplete", "incomplete:capture_roles")
    member = capture.members[0]
    receipt = receipts_by_role.get(DWD_RV_ROLE)
    payload = payloads_by_role.get(DWD_RV_ROLE)
    if receipt is None or payload is None:
        raise RawArtifactIntegrityError("DWD RV capture member receipt is missing")
    if (
        receipt.provider != DWD_RV_PROVIDER
        or receipt.product != DWD_RV_ARCHIVE_PRODUCT
        or receipt.receipt_id != member.receipt_id
        or receipt.content_sha256 != member.content_sha256
        or receipt.receipt_sha256 != member.receipt_sha256
        or receipt.received_at != member.received_at
        or receipt.size_bytes > MAX_ARCHIVE_BYTES
        or receipt.content_type not in {"application/octet-stream", "application/x-tar"}
    ):
        raise _RejectedDecode("unsupported", "unsupported:raw_member_contract")
    provenance = receipt.provenance
    collector_version = provenance.get("collector_version")
    if collector_version == HANDOFF_TARGET_COLLECTOR_VERSION:
        try:
            validate_decoder_handoff_capture(capture, receipt, payload)
        except DwdRvCaptureHandoffError as exc:
            raise _RejectedDecode(
                "unsupported", "unsupported:capture_handoff"
            ) from exc
    elif collector_version == DWD_RV_COLLECTOR_VERSION:
        # The pre-existing exact-raw v1 profile remains a closed compatibility
        # input for historical replays.  It is not produced by the acquisition
        # runtime and is still non-operational.  The v2 path above adds a second
        # *exact* profile; it does not relax any field in this legacy profile.
        if (
            provenance.get("representation") != NATIVE_REPRESENTATION
            or provenance.get("capture_stage") != NATIVE_CAPTURE_STAGE
            or provenance.get("collector") != DWD_RV_COLLECTOR
            or provenance.get("product") != DWD_RV_ARCHIVE_PRODUCT
            or provenance.get("declared_semantic_class")
            != "quantitative_precipitation_accumulation"
            or provenance.get("declared_quantity") != "precipitation_accumulation"
            or provenance.get("declared_unit") != "mm"
            or provenance.get("declared_accumulation_minutes") != 5.0
            or provenance.get("requested_forecast_lead_minutes") != 0
            or provenance.get("quantitative_semantics_verified_after_decode") is not False
        ):
            raise _RejectedDecode("unsupported", "unsupported:raw_provenance")
    else:
        raise _RejectedDecode("unsupported", "unsupported:raw_provenance")
    digest = receipt.content_sha256
    expected_upstream_id = (
        f"{DWD_RV_ARCHIVE_PRODUCT}|lead_minutes=0|content_sha256={digest}"
    )
    if receipt.upstream_id != expected_upstream_id:
        raise _RejectedDecode("unsupported", "unsupported:raw_member_identity")
    return member, payload, digest


def decode_dwd_rv_capture(
    *,
    capture_id: str,
    decoder_job_id: str,
    attempt_id: str,
    raw_store: RawArtifactStore,
    artifact_store: DwdRvDerivedArtifactStore,
    evidence_store: CanonicalEvidenceStore,
    evaluated_at: float | None = None,
    clock: Callable[[], float] | None = None,
    monotonic_deadline: float | None = None,
    monotonic_clock: Callable[[], float] | None = None,
) -> DwdRvDecodeResult:
    """Decode one verified RV capture and commit evidence only after publication."""
    if not isinstance(raw_store, RawArtifactStore):
        raise TypeError("raw_store must be RawArtifactStore")
    if not isinstance(artifact_store, DwdRvDerivedArtifactStore):
        raise TypeError("artifact_store has the wrong type")
    if not isinstance(evidence_store, CanonicalEvidenceStore):
        raise TypeError("evidence_store has the wrong type")
    if (
        not isinstance(decoder_job_id, str)
        or _DECODER_JOB_RE.fullmatch(decoder_job_id) is None
    ):
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
            raise DwdRvDecodeDeadlineError(
                f"DWD RV decoder deadline expired at {stage}"
            )

    checkpoint("decode_start")
    capture_bundle = raw_store.load_capture_bundle_bytes(capture_id)
    if capture_bundle is None:
        raise DwdRvCaptureMissingError("schema-3 DWD RV capture is missing")
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
        _member, archive_payload, archive_sha256 = _validate_raw_capture(
            capture,
            receipts_by_role,
            payloads_by_role,
        )
        checkpoint("raw_capture_validated")
        selected = _select_archive_member(archive_payload)
        checkpoint("archive_member_selected")
        if selected.product_time != capture.product_timestamp:
            raise _RejectedDecode("unsupported", "unsupported:time_contract")
        expected_capture_upstream_id = (
            f"{DWD_RV_CAPTURE_PRODUCT}|member={selected.name}"
            f"|product_timestamp={capture.product_timestamp:.6f}"
            f"|content_sha256={archive_sha256}"
        )
        if capture.upstream_capture_id != expected_capture_upstream_id:
            raise _RejectedDecode("unsupported", "unsupported:capture_identity")
        frame = _decode_hdf(selected)
        checkpoint("hdf_decoded")
        artifact_payload = _artifact_bytes(
            capture, archive_sha256, selected, frame
        )
        checkpoint("artifact_encoded")
        artifact = artifact_store.publish(
            artifact_payload,
            checkpoint=checkpoint,
        )
        checkpoint("artifact_published")
        verified = artifact_store.read_decoded(artifact)
        checkpoint("artifact_verified")
        if verified.metadata["capture"]["capture_id"] != capture.capture_id:
            raise DwdRvArtifactError("published artifact capture binding differs")
        checkpoint("canonical_commit_before")
        receipt = evidence_store.commit(_run_mapping(
            capture,
            decoder_job_id=decoder_job_id,
            attempt_id=attempt_id,
            evaluated_at=evaluation,
            outcome="decoded_complete",
            reason_code="decoded_complete",
            artifact=artifact,
            frame=frame,
        ))
        return DwdRvDecodeResult(
            outcome="decoded_complete",
            reason_code="decoded_complete",
            evidence_receipt=receipt,
            artifact=artifact,
        )
    except _RejectedDecode as rejected:
        checkpoint("canonical_rejection_commit_before")
        receipt = evidence_store.commit(_run_mapping(
            capture,
            decoder_job_id=decoder_job_id,
            attempt_id=attempt_id,
            evaluated_at=evaluation,
            outcome=rejected.outcome,
            reason_code=rejected.reason_code,
        ))
        return DwdRvDecodeResult(
            outcome=rejected.outcome,
            reason_code=rejected.reason_code,
            evidence_receipt=receipt,
            artifact=None,
        )


__all__ = [
    "ARTIFACT_SCHEMA",
    "DECODER_CONFIG_SHA256",
    "DECODER_IDENTITY",
    "DECODER_VERSION",
    "DwdRvArtifactCapacityError",
    "DwdRvArtifactError",
    "DwdRvArtifactRef",
    "DwdRvCaptureMissingError",
    "DwdRvDecodeResult",
    "DwdRvDecodeDeadlineError",
    "DwdRvDerivedArtifactStore",
    "DwdRvQuantitativeError",
    "DwdRvRasterArtifact",
    "decode_dwd_rv_capture",
]
