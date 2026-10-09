"""Bounded quantitative DBZH/QI decoder for one verified OPERA v3 capture.

This candidate is intentionally absent from every production handler and
provider registry.  It preserves a bounded regional source window as exact
float64 DBZH and QI values plus explicit masks.  It never converts dBZ to
rain-rate, never classifies wet/dry, and never grants motion truth or
operational admission.

The schema-3 raw store remains the sole upstream authority.  One
``OperaDbzhRawAuthority`` session combines the payload read used by the
decoder with the payload-free verification performed by
``CanonicalEvidenceStore`` so a member is read and hashed at most once per
decoder job.
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
from typing import Any, Callable, Iterator, Mapping
import zlib

import h5py
import numpy as np
from pyproj import CRS, Transformer

from .canonical_evidence_store import (
    CanonicalEvidenceStore,
    DecoderRunReceipt,
    EVIDENCE_CONTRACT,
)
from . import opera_native as native
from .raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RawArtifactCaptureReceipt,
    RawArtifactIntegrityError,
    RawArtifactReceipt,
    RawArtifactStore,
)


OPERA_PROVIDER = "opera"
OPERA_MEMBER_ROLE = "hdf5"
DECODER_IDENTITY = "opera-cirrus-dbzh-qi-bounded-region"
DECODER_VERSION = "opera-dbzh-qi-quantitative-v1"
ARTIFACT_SCHEMA = "stv3-worker-opera-dbzh-qi-region-v1"
ARTIFACT_MAGIC = b"STV3-OPERA-DBZH-QI-Q1\n"
ARTIFACT_HANDLE_PREFIX = "artifact:opera-dbzh-quantitative:"
VERDICT_SCHEMA = "stv3-worker-opera-dbzh-decode-verdict-v1"
VERDICT_KEY_SCHEMA = "stv3-worker-opera-dbzh-decode-key-v1"

MAX_REGION_ROWS = 1024
MAX_REGION_COLUMNS = 1024
MAX_REGION_PIXELS = 1_048_576
MAX_SOURCE_BYTES = 32 * 1024 * 1024
MAX_HEADER_BYTES = 256 * 1024
MAX_UNCOMPRESSED_ARRAY_BYTES = 32 * 1024 * 1024
MAX_ARTIFACT_BYTES = 40 * 1024 * 1024
MAX_ARTIFACT_TOTAL_BYTES = 512 * 1024 * 1024
MAX_ARTIFACT_OBJECTS = 4096
MAX_STAGING_BYTES = 80 * 1024 * 1024
MAX_STAGING_OBJECTS = 8
MAX_VERDICT_BYTES = 64 * 1024
MAX_VERDICT_OBJECTS = 65_536
MAX_VERDICT_TOTAL_BYTES = 64 * 1024 * 1024
MAX_ACCOUNTED_WORKING_BYTES = 128 * 1024 * 1024
DECODE_TOTAL_DEADLINE_SECONDS = 120.0
LOCK_POLL_SECONDS = 0.05
BBOX_DENSIFY_SEGMENTS_PER_EDGE = 256
BBOX_PROJECTION_GUARD_METRES = 2_000.0

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CAPTURE_ID_RE = re.compile(r"^capture-[0-9a-f]{64}$")
_OBJECT_NAME_RE = re.compile(r"^[0-9a-f]{64}\.odqr$")
_VERDICT_NAME_RE = re.compile(r"^[0-9a-f]{64}\.json$")
_STAGING_NAME_RE = re.compile(r"^\.opera-dbzh-[A-Za-z0-9_.-]+\.tmp$")
_SAFE_REASON_RE = re.compile(r"^(?:unsupported|incomplete|decode_error):[a-z0-9_]{1,96}$")

_ARRAY_SPECS = (
    ("dbzh_raw", "<f8"),
    ("detected_dbz", "<f8"),
    ("dbzh_nodata_mask", "|u1"),
    ("dbzh_undetect_mask", "|u1"),
    ("dbzh_detected_mask", "|u1"),
    ("coverage_mask", "|u1"),
    ("qi_raw", "<f8"),
    ("qi_valid_mask", "|u1"),
    ("qi_missing_mask", "|u1"),
)


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


_DECODER_CONFIG = {
    "artifact_schema": ARTIFACT_SCHEMA,
    "bbox": {
        "densify_segments_per_edge": BBOX_DENSIFY_SEGMENTS_PER_EDGE,
        "projection_guard_metres": BBOX_PROJECTION_GUARD_METRES,
        "topology": "non_wrapping_wgs84_bbox",
    },
    "capture_contract": native.OPERA_CERTIFICATION_CONTRACT_VERSION,
    "collector_version": native.OPERA_RAW_COLLECTOR_VERSION,
    "decoder_identity": DECODER_IDENTITY,
    "decoder_version": DECODER_VERSION,
    "grid": {
        "axis_order": "yx",
        "column_center_formula": "x=(global_column+0.5)*1000",
        "columns": native.OPERA_GRID_COLUMNS,
        "projection": native.OPERA_PROJDEF,
        "row_center_formula": "y=-(global_row+0.5)*1000",
        "rows": native.OPERA_GRID_ROWS,
    },
    "inspector_version": native.OPERA_STRUCTURE_INSPECTOR_VERSION,
    "limits": {
        "max_region_columns": MAX_REGION_COLUMNS,
        "max_region_pixels": MAX_REGION_PIXELS,
        "max_region_rows": MAX_REGION_ROWS,
        "max_source_bytes": MAX_SOURCE_BYTES,
    },
    "quantity": "maximum_horizontal_reflectivity_factor",
    "quality_task": native.OPERA_QUALITY_TASK,
    "semantics": {
        "dbzh_to_rain_rate": False,
        "motion_truth_eligible": False,
        "qi_zero_is_valid": True,
        "wet_dry_classification": False,
    },
    "unit": "dBZ",
}
DECODER_CONFIG_SHA256 = hashlib.sha256(
    _canonical_json(_DECODER_CONFIG)
).hexdigest()


class OperaDbzhQuantitativeError(RuntimeError):
    """Base failure for this isolated decoder candidate."""


class OperaDbzhCaptureMissingError(OperaDbzhQuantitativeError):
    """The requested schema-3 capture does not exist."""


class OperaDbzhArtifactError(OperaDbzhQuantitativeError):
    """The immutable derived store rejected an object."""


class OperaDbzhArtifactCapacityError(OperaDbzhArtifactError):
    """Publication would exceed a hard derived-store capacity."""


class OperaDbzhDecodeDeadlineError(OperaDbzhQuantitativeError):
    """The cooperative absolute decoder deadline expired."""


class _RejectedDecode(Exception):
    def __init__(self, outcome: str, reason_code: str) -> None:
        if outcome not in {"unsupported", "incomplete", "decode_error"}:
            raise ValueError("invalid deterministic decoder outcome")
        if _SAFE_REASON_RE.fullmatch(reason_code) is None:
            raise ValueError("invalid deterministic decoder reason")
        self.outcome = outcome
        self.reason_code = reason_code
        super().__init__(reason_code)


@dataclass(frozen=True, slots=True)
class OperaWgs84Region:
    minimum_latitude: float
    minimum_longitude: float
    maximum_latitude: float
    maximum_longitude: float


@dataclass(frozen=True, slots=True)
class OperaNativeWindow:
    row_start: int
    row_stop: int
    column_start: int
    column_stop: int
    requested: OperaWgs84Region
    minimum_x: float
    minimum_y: float
    maximum_x: float
    maximum_y: float
    densified_point_count: int
    grid_definition_sha256: str
    coverage_definition_sha256: str

    @property
    def rows(self) -> int:
        return self.row_stop - self.row_start

    @property
    def columns(self) -> int:
        return self.column_stop - self.column_start

    @property
    def pixels(self) -> int:
        return self.rows * self.columns


@dataclass(frozen=True, slots=True)
class OperaDbzhArtifactRef:
    handle: str
    sha256: str
    size_bytes: int


@dataclass(frozen=True, slots=True)
class OperaDbzhRasterArtifact:
    metadata: Mapping[str, Any]
    dbzh_raw: np.ndarray
    detected_dbz: np.ndarray
    dbzh_nodata_mask: np.ndarray
    dbzh_undetect_mask: np.ndarray
    dbzh_detected_mask: np.ndarray
    coverage_mask: np.ndarray
    qi_raw: np.ndarray
    qi_valid_mask: np.ndarray
    qi_missing_mask: np.ndarray


@dataclass(frozen=True, slots=True)
class OperaDbzhDecodeResult:
    outcome: str
    reason_code: str
    evidence_receipt: DecoderRunReceipt
    artifact: OperaDbzhArtifactRef | None


@dataclass(frozen=True, slots=True)
class _VerifiedSource:
    capture: RawArtifactCaptureReceipt
    receipt: RawArtifactReceipt
    payload: bytes
    object_key: str
    revision: native.OperaRevisionIdentity
    identity: native.OperaHdfIdentity
    validation_verdict_id: str
    validation_verdict_sha256: str


@dataclass(frozen=True, slots=True)
class _DecodedRegion:
    window: OperaNativeWindow
    arrays: Mapping[str, np.ndarray]
    counts: Mapping[str, int]


class OperaDbzhRawAuthority:
    """One-capture raw authority session with one verified member read.

    The first bundle read delegates to ``RawArtifactStore`` and caches only
    immutable dataclasses and bytes.  CanonicalEvidenceStore subsequently asks
    this same authority for the capture receipt and independently recomputes
    its receipt contract without hashing the member again.
    """

    def __init__(self, raw_store: RawArtifactStore) -> None:
        if not isinstance(raw_store, RawArtifactStore):
            raise TypeError("raw_store must be RawArtifactStore")
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

    def load_capture_bundle_bytes(
        self, capture_id: str,
    ) -> tuple[
        RawArtifactCaptureReceipt,
        dict[str, RawArtifactReceipt],
        dict[str, bytes],
    ] | None:
        if not isinstance(capture_id, str) or _CAPTURE_ID_RE.fullmatch(capture_id) is None:
            raise ValueError("capture_id is invalid")
        with self._lock:
            if self._capture_id is not None and self._capture_id != capture_id:
                raise OperaDbzhQuantitativeError(
                    "one raw authority session cannot span multiple captures"
                )
            if self._bundle is not None:
                capture, receipts, payloads = self._bundle
                return capture, dict(receipts), dict(payloads)
            if self._absence_cached:
                return None
            if self._receipt_only is not None:
                raise OperaDbzhQuantitativeError(
                    "payload receipt was already verified without decoder bytes"
                )
            self.raw_store.initialize()
            bundle = self.raw_store.load_capture_bundle_bytes(capture_id)
            self._capture_id = capture_id
            self._raw_verification_count += 1
            if bundle is None:
                self._absence_cached = True
                return None
            capture, receipts, payloads = bundle
            self._bundle = (capture, dict(receipts), dict(payloads))
            return capture, dict(receipts), dict(payloads)

    def load_capture_receipt(
        self, capture_id: str,
    ) -> RawArtifactCaptureReceipt | None:
        if not isinstance(capture_id, str) or _CAPTURE_ID_RE.fullmatch(capture_id) is None:
            raise ValueError("capture_id is invalid")
        with self._lock:
            if self._capture_id is not None and self._capture_id != capture_id:
                raise OperaDbzhQuantitativeError(
                    "one raw authority session cannot span multiple captures"
                )
            if self._bundle is not None:
                return self._bundle[0]
            if self._receipt_only is not None:
                return self._receipt_only
            if self._absence_cached:
                return None
            self.raw_store.initialize()
            capture = self.raw_store.load_capture_receipt(capture_id)
            self._capture_id = capture_id
            self._receipt_only = capture
            self._absence_cached = capture is None
            self._raw_verification_count += 1
            return capture


def open_opera_evidence_store(
    path: str | Path,
    *,
    raw_authority: OperaDbzhRawAuthority,
    clock: Callable[[], float] = time.time,
    fault_injector: Callable[[str], None] | None = None,
) -> CanonicalEvidenceStore:
    """Create a canonical store bound to the job's single-read authority."""

    if not isinstance(raw_authority, OperaDbzhRawAuthority):
        raise TypeError("raw_authority has wrong type")
    return CanonicalEvidenceStore(
        path,
        raw_artifact_reader=raw_authority,
        clock=clock,
        fault_injector=fault_injector,
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


_ROOT_LOCKS_GUARD = threading.Lock()
_ROOT_LOCKS: dict[str, threading.RLock] = {}


def _thread_root_lock(root: Path) -> threading.RLock:
    key = os.path.normcase(str(root.resolve(strict=False)))
    with _ROOT_LOCKS_GUARD:
        return _ROOT_LOCKS.setdefault(key, threading.RLock())


@contextmanager
def _deadline_file_lock(
    path: Path,
    *,
    checkpoint: Callable[[str], None],
) -> Iterator[None]:
    """Acquire one OS-level lock while repeatedly enforcing the job deadline."""

    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and (path.is_symlink() or not path.is_file()):
        raise OperaDbzhArtifactError("derived artifact lock path is unsafe")
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    acquired = False
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise OperaDbzhArtifactError("derived artifact lock is not regular")
        if info.st_size == 0:
            os.write(descriptor, b"\0")
            os.fsync(descriptor)
        os.lseek(descriptor, 0, os.SEEK_SET)
        while not acquired:
            checkpoint("artifact_publish_lock_wait")
            try:
                if os.name == "nt":  # pragma: no cover - platform selected
                    import msvcrt

                    msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
                else:  # pragma: no branch - platform selected
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
                time.sleep(LOCK_POLL_SECONDS)
        yield
    finally:
        if acquired:
            try:
                os.lseek(descriptor, 0, os.SEEK_SET)
                if os.name == "nt":  # pragma: no cover - platform selected
                    import msvcrt

                    msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
                else:  # pragma: no branch - platform selected
                    import fcntl

                    fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)
        else:
            os.close(descriptor)


class OperaDbzhDerivedArtifactStore:
    """Bounded immutable object and decoder-verdict store."""

    def __init__(
        self,
        root: str | Path,
        *,
        max_total_bytes: int = MAX_ARTIFACT_TOTAL_BYTES,
        fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        supplied = Path(root)
        if not supplied.is_absolute():
            raise ValueError("derived artifact root must be absolute")
        if (
            isinstance(max_total_bytes, bool)
            or not isinstance(max_total_bytes, int)
            or not MAX_ARTIFACT_BYTES <= max_total_bytes <= MAX_ARTIFACT_TOTAL_BYTES
        ):
            raise ValueError("derived artifact total capacity is invalid")
        self.root = supplied
        self.objects_path = supplied / "objects"
        self.staging_path = supplied / "staging"
        self.verdicts_path = supplied / "verdicts"
        self.lock_path = supplied / ".publish.lock"
        self.max_total_bytes = max_total_bytes
        self._fault_injector = fault_injector
        self._lock = _thread_root_lock(supplied)

    def initialize(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self.objects_path.mkdir(mode=0o700, exist_ok=True)
        self.staging_path.mkdir(mode=0o700, exist_ok=True)
        self.verdicts_path.mkdir(mode=0o700, exist_ok=True)
        for path in (
            self.root,
            self.objects_path,
            self.staging_path,
            self.verdicts_path,
        ):
            info = path.lstat()
            if path.is_symlink() or not stat.S_ISDIR(info.st_mode):
                raise OperaDbzhArtifactError("derived artifact layout is unsafe")
        _fsync_directory(self.root)

    def _fault(self, stage: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(stage)

    def _inventory(
        self, checkpoint: Callable[[str], None],
    ) -> tuple[int, int, int, int, int, int]:
        object_bytes = 0
        object_count = 0
        for path in self.objects_path.iterdir():
            checkpoint("artifact_object_inventory")
            info = path.lstat()
            if (
                path.is_symlink()
                or not stat.S_ISREG(info.st_mode)
                or _OBJECT_NAME_RE.fullmatch(path.name) is None
                or not 0 < info.st_size <= MAX_ARTIFACT_BYTES
            ):
                raise OperaDbzhArtifactError("derived store contains an unsafe object")
            object_count += 1
            object_bytes += info.st_size
            if object_count > MAX_ARTIFACT_OBJECTS or object_bytes > self.max_total_bytes:
                raise OperaDbzhArtifactCapacityError("derived object capacity exceeded")

        staging_bytes = 0
        staging_count = 0
        for path in self.staging_path.iterdir():
            checkpoint("artifact_staging_inventory")
            info = path.lstat()
            if (
                path.is_symlink()
                or not stat.S_ISREG(info.st_mode)
                or _STAGING_NAME_RE.fullmatch(path.name) is None
                or not 0 < info.st_size <= MAX_ARTIFACT_BYTES
            ):
                raise OperaDbzhArtifactError("derived staging contains an unsafe object")
            staging_count += 1
            staging_bytes += info.st_size
            if staging_count > MAX_STAGING_OBJECTS or staging_bytes > MAX_STAGING_BYTES:
                raise OperaDbzhArtifactCapacityError("derived staging capacity exceeded")

        verdict_bytes = 0
        verdict_count = 0
        for path in self.verdicts_path.iterdir():
            checkpoint("artifact_verdict_inventory")
            info = path.lstat()
            if (
                path.is_symlink()
                or not stat.S_ISREG(info.st_mode)
                or _VERDICT_NAME_RE.fullmatch(path.name) is None
                or not 0 < info.st_size <= MAX_VERDICT_BYTES
            ):
                raise OperaDbzhArtifactError("derived verdict store is unsafe")
            verdict_count += 1
            verdict_bytes += info.st_size
            if (
                verdict_count > MAX_VERDICT_OBJECTS
                or verdict_bytes > MAX_VERDICT_TOTAL_BYTES
            ):
                raise OperaDbzhArtifactCapacityError("derived verdict capacity exceeded")
        return (
            object_bytes,
            object_count,
            staging_bytes,
            staging_count,
            verdict_bytes,
            verdict_count,
        )

    @staticmethod
    def _verified_file(
        path: Path,
        *,
        expected_size: int,
        expected_sha256: str,
        maximum: int,
        checkpoint: Callable[[str], None] | None = None,
    ) -> bytes:
        try:
            info = path.lstat()
        except OSError as error:
            raise OperaDbzhArtifactError("derived object is missing") from error
        if (
            path.is_symlink()
            or not stat.S_ISREG(info.st_mode)
            or info.st_size != expected_size
            or not 0 < expected_size <= maximum
            or _SHA256_RE.fullmatch(expected_sha256) is None
        ):
            raise OperaDbzhArtifactError("derived object shape changed")
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
        chunks: list[bytes] = []
        digest = hashlib.sha256()
        total = 0
        try:
            opened = os.fstat(descriptor)
            if not stat.S_ISREG(opened.st_mode) or opened.st_size != expected_size:
                raise OperaDbzhArtifactError("derived object changed while opening")
            while total < expected_size:
                if checkpoint is not None:
                    checkpoint("artifact_integrity_read")
                chunk = os.read(descriptor, min(1024 * 1024, expected_size - total))
                if not chunk:
                    break
                chunks.append(chunk)
                total += len(chunk)
                digest.update(chunk)
            if os.read(descriptor, 1):
                raise OperaDbzhArtifactError("derived object has trailing bytes")
        finally:
            os.close(descriptor)
        after = path.lstat()
        if (
            total != expected_size
            or not hmac.compare_digest(digest.hexdigest(), expected_sha256)
            or (after.st_dev, after.st_ino) != (opened.st_dev, opened.st_ino)
            or after.st_size != opened.st_size
        ):
            raise OperaDbzhArtifactError("derived object failed integrity")
        return b"".join(chunks)

    def publish(
        self,
        payload: bytes,
        *,
        checkpoint: Callable[[str], None],
    ) -> OperaDbzhArtifactRef:
        if not isinstance(payload, bytes) or not 0 < len(payload) <= MAX_ARTIFACT_BYTES:
            raise OperaDbzhArtifactCapacityError("artifact payload exceeds object bound")
        digest = hashlib.sha256(payload).hexdigest()
        reference = OperaDbzhArtifactRef(
            handle=f"{ARTIFACT_HANDLE_PREFIX}{digest}",
            sha256=digest,
            size_bytes=len(payload),
        )
        self.initialize()
        target = self.objects_path / f"{digest}.odqr"
        temporary_path: Path | None = None
        with self._lock, _deadline_file_lock(
            self.lock_path, checkpoint=checkpoint
        ):
            (
                object_bytes,
                object_count,
                staging_bytes,
                staging_count,
                _verdict_bytes,
                _verdict_count,
            ) = self._inventory(checkpoint)
            if target.exists():
                self._verified_file(
                    target,
                    expected_size=len(payload),
                    expected_sha256=digest,
                    maximum=MAX_ARTIFACT_BYTES,
                    checkpoint=checkpoint,
                )
                return reference
            if (
                object_count + 1 > MAX_ARTIFACT_OBJECTS
                or object_bytes + len(payload) > self.max_total_bytes
                or staging_count + 1 > MAX_STAGING_OBJECTS
                or staging_bytes + len(payload) > MAX_STAGING_BYTES
            ):
                raise OperaDbzhArtifactCapacityError("artifact publication exceeds capacity")
            try:
                with tempfile.NamedTemporaryFile(
                    dir=self.staging_path,
                    prefix=".opera-dbzh-",
                    suffix=".tmp",
                    delete=False,
                ) as stream:
                    temporary_path = Path(stream.name)
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
                _fsync_directory(self.staging_path)
                self._fault("artifact.after_stage_fsync")
                try:
                    os.link(temporary_path, target)
                except FileExistsError:
                    pass
                _fsync_directory(self.objects_path)
                self._verified_file(
                    target,
                    expected_size=len(payload),
                    expected_sha256=digest,
                    maximum=MAX_ARTIFACT_BYTES,
                    checkpoint=checkpoint,
                )
                self._fault("artifact.after_publish_verify")
            finally:
                if temporary_path is not None:
                    temporary_path.unlink(missing_ok=True)
                    _fsync_directory(self.staging_path)
        return reference

    def read_bytes(
        self,
        reference: OperaDbzhArtifactRef,
        *,
        checkpoint: Callable[[str], None],
    ) -> bytes:
        if not isinstance(reference, OperaDbzhArtifactRef):
            raise TypeError("artifact reference has wrong type")
        if (
            _SHA256_RE.fullmatch(reference.sha256) is None
            or reference.handle != f"{ARTIFACT_HANDLE_PREFIX}{reference.sha256}"
            or isinstance(reference.size_bytes, bool)
            or not isinstance(reference.size_bytes, int)
            or not 0 < reference.size_bytes <= MAX_ARTIFACT_BYTES
        ):
            raise OperaDbzhArtifactError("artifact reference is invalid")
        checkpoint("artifact_read_before")
        self.initialize()
        payload = self._verified_file(
            self.objects_path / f"{reference.sha256}.odqr",
            expected_size=reference.size_bytes,
            expected_sha256=reference.sha256,
            maximum=MAX_ARTIFACT_BYTES,
            checkpoint=checkpoint,
        )
        checkpoint("artifact_read_after")
        return payload

    def read_decoded(
        self,
        reference: OperaDbzhArtifactRef,
        *,
        checkpoint: Callable[[str], None],
    ) -> OperaDbzhRasterArtifact:
        return _parse_artifact(
            self.read_bytes(reference, checkpoint=checkpoint),
            checkpoint=checkpoint,
        )

    def load_verdict(
        self,
        verdict_id: str,
        *,
        expected_key: Mapping[str, Any],
        checkpoint: Callable[[str], None],
    ) -> Mapping[str, Any] | None:
        if _SHA256_RE.fullmatch(verdict_id or "") is None:
            raise ValueError("verdict_id is invalid")
        self.initialize()
        path = self.verdicts_path / f"{verdict_id}.json"
        if not path.exists():
            return None
        checkpoint("verdict_read_before")
        try:
            info = path.lstat()
        except OSError as error:
            raise OperaDbzhArtifactError("decoder verdict is missing") from error
        if (
            path.is_symlink()
            or not stat.S_ISREG(info.st_mode)
            or not 0 < info.st_size <= MAX_VERDICT_BYTES
        ):
            raise OperaDbzhArtifactError("decoder verdict path is unsafe")
        try:
            payload = path.read_bytes()
        except OSError as error:
            raise OperaDbzhArtifactError("decoder verdict is unreadable") from error
        after = path.lstat()
        if (
            len(payload) != info.st_size
            or (after.st_dev, after.st_ino) != (info.st_dev, info.st_ino)
            or after.st_size != info.st_size
        ):
            raise OperaDbzhArtifactError("decoder verdict changed while reading")
        document = _validated_verdict_payload(
            payload,
            verdict_id=verdict_id,
            expected_key=expected_key,
        )
        checkpoint("verdict_read_after")
        return document

    def publish_verdict(
        self,
        document: Mapping[str, Any],
        *,
        checkpoint: Callable[[str], None],
    ) -> Mapping[str, Any]:
        payload = _canonical_json(document)
        if not 0 < len(payload) <= MAX_VERDICT_BYTES:
            raise OperaDbzhArtifactCapacityError("decoder verdict exceeds byte bound")
        verdict_id = str(document.get("verdict_id") or "")
        expected_key = document.get("key")
        validated = _validated_verdict_payload(
            payload,
            verdict_id=verdict_id,
            expected_key=expected_key if isinstance(expected_key, Mapping) else {},
        )
        self.initialize()
        target = self.verdicts_path / f"{verdict_id}.json"
        temporary_path: Path | None = None
        with self._lock, _deadline_file_lock(
            self.lock_path, checkpoint=checkpoint
        ):
            inventory = self._inventory(checkpoint)
            if target.exists():
                existing = self.load_verdict(
                    verdict_id,
                    expected_key=validated["key"],
                    checkpoint=checkpoint,
                )
                if existing is None or _canonical_json(existing) != payload:
                    raise OperaDbzhArtifactError("decoder verdict identity conflict")
                return existing
            if (
                inventory[4] + len(payload) > MAX_VERDICT_TOTAL_BYTES
                or inventory[5] + 1 > MAX_VERDICT_OBJECTS
                or inventory[2] + len(payload) > MAX_STAGING_BYTES
                or inventory[3] + 1 > MAX_STAGING_OBJECTS
            ):
                raise OperaDbzhArtifactCapacityError("decoder verdict capacity exceeded")
            try:
                with tempfile.NamedTemporaryFile(
                    dir=self.staging_path,
                    prefix=".opera-dbzh-",
                    suffix=".tmp",
                    delete=False,
                ) as stream:
                    temporary_path = Path(stream.name)
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
                _fsync_directory(self.staging_path)
                self._fault("verdict.after_stage_fsync")
                try:
                    os.link(temporary_path, target)
                except FileExistsError:
                    pass
                _fsync_directory(self.verdicts_path)
                loaded = self.load_verdict(
                    verdict_id,
                    expected_key=validated["key"],
                    checkpoint=checkpoint,
                )
                if loaded is None or _canonical_json(loaded) != payload:
                    raise OperaDbzhArtifactError("published decoder verdict differs")
                self._fault("verdict.after_publish_verify")
                return loaded
            finally:
                if temporary_path is not None:
                    temporary_path.unlink(missing_ok=True)
                    _fsync_directory(self.staging_path)


def _finite(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"region {name} must be a finite number")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"region {name} must be a finite number")
    return result


def _region_mapping(region: OperaWgs84Region) -> dict[str, float]:
    return {
        "maximum_latitude": region.maximum_latitude,
        "maximum_longitude": region.maximum_longitude,
        "minimum_latitude": region.minimum_latitude,
        "minimum_longitude": region.minimum_longitude,
    }


def _window_mapping(window: OperaNativeWindow) -> dict[str, Any]:
    return {
        "column_start": window.column_start,
        "column_stop_exclusive": window.column_stop,
        "coverage_definition_sha256": window.coverage_definition_sha256,
        "densified_point_count": window.densified_point_count,
        "grid_definition_sha256": window.grid_definition_sha256,
        "maximum_x": window.maximum_x,
        "maximum_y": window.maximum_y,
        "minimum_x": window.minimum_x,
        "minimum_y": window.minimum_y,
        "requested_wgs84_bbox": _region_mapping(window.requested),
        "row_start": window.row_start,
        "row_stop_exclusive": window.row_stop,
    }


def _window_for_region(
    region: OperaWgs84Region,
    *,
    checkpoint: Callable[[str], None],
) -> OperaNativeWindow:
    if not isinstance(region, OperaWgs84Region):
        raise TypeError("region must be OperaWgs84Region")
    minimum_latitude = _finite(region.minimum_latitude, "minimum_latitude")
    minimum_longitude = _finite(region.minimum_longitude, "minimum_longitude")
    maximum_latitude = _finite(region.maximum_latitude, "maximum_latitude")
    maximum_longitude = _finite(region.maximum_longitude, "maximum_longitude")
    if (
        not -90.0 <= minimum_latitude < maximum_latitude <= 90.0
        or not -180.0 <= minimum_longitude < maximum_longitude <= 180.0
    ):
        raise ValueError("region must be a non-wrapping WGS84 bbox")
    request = OperaWgs84Region(
        minimum_latitude=minimum_latitude,
        minimum_longitude=minimum_longitude,
        maximum_latitude=maximum_latitude,
        maximum_longitude=maximum_longitude,
    )
    segments = BBOX_DENSIFY_SEGMENTS_PER_EDGE
    if not 16 <= segments <= 4096:
        raise RuntimeError("bbox densification bound is invalid")
    longitudes: list[float] = []
    latitudes: list[float] = []
    for index in range(segments + 1):
        if index % 64 == 0:
            checkpoint("bbox_densification")
        fraction = index / segments
        longitude = minimum_longitude + (
            maximum_longitude - minimum_longitude
        ) * fraction
        latitude = minimum_latitude + (
            maximum_latitude - minimum_latitude
        ) * fraction
        longitudes.extend((longitude, longitude, minimum_longitude, maximum_longitude))
        latitudes.extend((minimum_latitude, maximum_latitude, latitude, latitude))
    if len(longitudes) != 4 * (segments + 1):
        raise RuntimeError("bbox densification count drifted")
    try:
        transformer = Transformer.from_crs(
            CRS.from_epsg(4326),
            CRS.from_proj4(native.OPERA_PROJDEF),
            always_xy=True,
        )
        projected_x, projected_y = transformer.transform(longitudes, latitudes)
    except Exception as error:
        raise ValueError("region cannot be transformed to OPERA LAEA") from error
    checkpoint("bbox_projection")
    x_values = tuple(float(value) for value in projected_x)
    y_values = tuple(float(value) for value in projected_y)
    if (
        len(x_values) != len(longitudes)
        or len(y_values) != len(latitudes)
        or not all(math.isfinite(value) for value in (*x_values, *y_values))
    ):
        raise ValueError("region projection is not finite")

    grid_maximum_x = native.OPERA_GRID_COLUMNS * native.OPERA_GRID_SCALE_METRES
    grid_minimum_y = -native.OPERA_GRID_ROWS * native.OPERA_GRID_SCALE_METRES
    tolerance = 1.0e-6
    if any(
        x < -tolerance
        or x > grid_maximum_x + tolerance
        or y < grid_minimum_y - tolerance
        or y > tolerance
        for x, y in zip(x_values, y_values)
    ):
        raise ValueError("region lies outside the exact OPERA LAEA domain")

    raw_minimum_x = min(x_values)
    raw_maximum_x = max(x_values)
    raw_minimum_y = min(y_values)
    raw_maximum_y = max(y_values)
    minimum_x = max(0.0, raw_minimum_x - BBOX_PROJECTION_GUARD_METRES)
    maximum_x = min(
        grid_maximum_x, raw_maximum_x + BBOX_PROJECTION_GUARD_METRES
    )
    minimum_y = max(
        grid_minimum_y, raw_minimum_y - BBOX_PROJECTION_GUARD_METRES
    )
    maximum_y = min(0.0, raw_maximum_y + BBOX_PROJECTION_GUARD_METRES)
    scale = native.OPERA_GRID_SCALE_METRES
    column_start = max(0, math.floor(minimum_x / scale))
    column_stop = min(native.OPERA_GRID_COLUMNS, math.ceil(maximum_x / scale))
    row_start = max(0, math.floor(-maximum_y / scale))
    row_stop = min(native.OPERA_GRID_ROWS, math.ceil(-minimum_y / scale))
    rows = row_stop - row_start
    columns = column_stop - column_start
    pixels = rows * columns
    if (
        rows <= 0
        or columns <= 0
        or rows > MAX_REGION_ROWS
        or columns > MAX_REGION_COLUMNS
        or pixels > MAX_REGION_PIXELS
    ):
        raise ValueError("projected OPERA region exceeds the fixed window bound")
    accounted = MAX_SOURCE_BYTES + pixels * 30 * 3
    if accounted > MAX_ACCOUNTED_WORKING_BYTES:
        raise ValueError("projected OPERA region exceeds the memory account")

    selected_minimum_x = column_start * scale
    selected_maximum_x = column_stop * scale
    selected_minimum_y = -row_stop * scale
    selected_maximum_y = -row_start * scale
    grid_definition = {
        "axis_order": "yx",
        "column_center_formula": "x=(global_column+0.5)*1000",
        "column_start": column_start,
        "column_stop_exclusive": column_stop,
        "global_columns": native.OPERA_GRID_COLUMNS,
        "global_rows": native.OPERA_GRID_ROWS,
        "kind": "regular",
        "projection": native.OPERA_PROJDEF,
        "requested_wgs84_bbox": _region_mapping(request),
        "row_center_formula": "y=-(global_row+0.5)*1000",
        "row_start": row_start,
        "row_stop_exclusive": row_stop,
        "xscale_metres": scale,
        "yscale_metres": scale,
    }
    coverage_definition = {
        "kind": "native_bbox",
        "maximum_x": selected_maximum_x,
        "maximum_y": selected_maximum_y,
        "minimum_x": selected_minimum_x,
        "minimum_y": selected_minimum_y,
    }
    return OperaNativeWindow(
        row_start=row_start,
        row_stop=row_stop,
        column_start=column_start,
        column_stop=column_stop,
        requested=request,
        minimum_x=selected_minimum_x,
        minimum_y=selected_minimum_y,
        maximum_x=selected_maximum_x,
        maximum_y=selected_maximum_y,
        densified_point_count=len(longitudes),
        grid_definition_sha256=hashlib.sha256(
            _canonical_json(grid_definition)
        ).hexdigest(),
        coverage_definition_sha256=hashlib.sha256(
            _canonical_json(coverage_definition)
        ).hexdigest(),
    )


def _validated_verdict_payload(
    payload: bytes,
    *,
    verdict_id: str,
    expected_key: Mapping[str, Any],
) -> Mapping[str, Any]:
    if not isinstance(payload, bytes) or not 0 < len(payload) <= MAX_VERDICT_BYTES:
        raise OperaDbzhArtifactError("decoder verdict has invalid size")
    try:
        document = json.loads(payload.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise OperaDbzhArtifactError("decoder verdict is not JSON") from error
    if not isinstance(document, dict) or payload != _canonical_json(document):
        raise OperaDbzhArtifactError("decoder verdict is not canonical JSON")
    if (
        _SHA256_RE.fullmatch(verdict_id or "") is None
        or document.get("schema") != VERDICT_SCHEMA
        or document.get("verdict_id") != verdict_id
        or document.get("key") != dict(expected_key)
        or verdict_id
        != hashlib.sha256(_canonical_json(dict(expected_key))).hexdigest()
    ):
        raise OperaDbzhArtifactError("decoder verdict identity differs")
    status = document.get("status")
    if status == "decoded_complete":
        if set(document) != {"artifact", "key", "schema", "status", "verdict_id"}:
            raise OperaDbzhArtifactError("success verdict schema differs")
        artifact = document.get("artifact")
        if not isinstance(artifact, dict) or set(artifact) != {
            "handle",
            "metadata_sha256",
            "sha256",
            "size_bytes",
        }:
            raise OperaDbzhArtifactError("success verdict artifact is invalid")
        sha = artifact.get("sha256")
        size = artifact.get("size_bytes")
        if (
            not isinstance(sha, str)
            or _SHA256_RE.fullmatch(sha) is None
            or artifact.get("handle") != f"{ARTIFACT_HANDLE_PREFIX}{sha}"
            or not isinstance(artifact.get("metadata_sha256"), str)
            or _SHA256_RE.fullmatch(str(artifact.get("metadata_sha256"))) is None
            or isinstance(size, bool)
            or not isinstance(size, int)
            or not 0 < size <= MAX_ARTIFACT_BYTES
        ):
            raise OperaDbzhArtifactError("success verdict artifact fields differ")
    elif status == "deterministic_rejection":
        if set(document) != {
            "key",
            "outcome",
            "reason_code",
            "schema",
            "status",
            "verdict_id",
        }:
            raise OperaDbzhArtifactError("rejection verdict schema differs")
        if (
            document.get("outcome") not in {"unsupported", "incomplete", "decode_error"}
            or not isinstance(document.get("reason_code"), str)
            or _SAFE_REASON_RE.fullmatch(str(document.get("reason_code"))) is None
        ):
            raise OperaDbzhArtifactError("rejection verdict fields differ")
    else:
        raise OperaDbzhArtifactError("decoder verdict status is unknown")
    return document


def _verdict_key(
    source: _VerifiedSource,
    window: OperaNativeWindow,
) -> dict[str, Any]:
    return {
        "capture_id": source.capture.capture_id,
        "capture_receipt_sha256": source.capture.capture_receipt_sha256,
        "decoder_config_sha256": DECODER_CONFIG_SHA256,
        "member_content_sha256": source.receipt.content_sha256,
        "member_receipt_sha256": source.receipt.receipt_sha256,
        "region": _region_mapping(window.requested),
        "schema": VERDICT_KEY_SCHEMA,
        "window_grid_sha256": window.grid_definition_sha256,
    }


def _success_verdict(
    key: Mapping[str, Any],
    artifact: OperaDbzhArtifactRef,
    *,
    metadata_sha256: str,
) -> dict[str, Any]:
    verdict_id = hashlib.sha256(_canonical_json(dict(key))).hexdigest()
    return {
        "artifact": {
            "handle": artifact.handle,
            "metadata_sha256": metadata_sha256,
            "sha256": artifact.sha256,
            "size_bytes": artifact.size_bytes,
        },
        "key": dict(key),
        "schema": VERDICT_SCHEMA,
        "status": "decoded_complete",
        "verdict_id": verdict_id,
    }


def _rejection_verdict(
    key: Mapping[str, Any], rejected: _RejectedDecode,
) -> dict[str, Any]:
    verdict_id = hashlib.sha256(_canonical_json(dict(key))).hexdigest()
    return {
        "key": dict(key),
        "outcome": rejected.outcome,
        "reason_code": rejected.reason_code,
        "schema": VERDICT_SCHEMA,
        "status": "deterministic_rejection",
        "verdict_id": verdict_id,
    }


def _canonical_slot(timestamp: float) -> tuple[str, str]:
    if (
        not math.isfinite(timestamp)
        or not timestamp.is_integer()
        or int(timestamp) % 300 != 0
    ):
        raise _RejectedDecode("unsupported", "unsupported:product_time")
    moment = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    job_id = native._canonical_job_id(timestamp)
    object_key = (
        f"{moment:%Y/%m/%d}/OPERA/COMP/"
        f"OPERA@{moment:%Y%m%dT%H%M}@0@DBZH.h5"
    )
    return job_id, object_key


def _member_receipt(
    capture: RawArtifactCaptureReceipt,
    receipts: Mapping[str, RawArtifactReceipt],
) -> RawArtifactReceipt:
    if len(capture.members) != 1 or capture.members[0].role != OPERA_MEMBER_ROLE:
        raise _RejectedDecode("incomplete", "incomplete:capture_roles")
    receipt = receipts.get(OPERA_MEMBER_ROLE)
    member = capture.members[0]
    if not isinstance(receipt, RawArtifactReceipt):
        raise RawArtifactIntegrityError("OPERA HDF5 member receipt is absent")
    if (
        receipt.receipt_id != member.receipt_id
        or receipt.content_sha256 != member.content_sha256
        or receipt.receipt_sha256 != member.receipt_sha256
        or receipt.received_at != member.received_at
    ):
        raise RawArtifactIntegrityError("OPERA member and receipt differ")
    return receipt


def _validate_raw_capture(
    raw_authority: OperaDbzhRawAuthority,
    capture: RawArtifactCaptureReceipt,
    *,
    receipts: Mapping[str, RawArtifactReceipt],
    payloads: Mapping[str, bytes],
) -> _VerifiedSource:
    if (
        capture.provider != OPERA_PROVIDER
        or capture.product != native.OPERA_CAPTURE_PRODUCT
    ):
        raise _RejectedDecode("unsupported", "unsupported:capture_contract")
    if capture.product_timestamp is None:
        raise _RejectedDecode("incomplete", "incomplete:product_time")
    receipt = _member_receipt(capture, receipts)
    if set(payloads) != {OPERA_MEMBER_ROLE}:
        raise RawArtifactIntegrityError("OPERA capture bundle has wrong payload roles")
    payload = payloads[OPERA_MEMBER_ROLE]
    if (
        not isinstance(payload, bytes)
        or receipt.size_bytes != len(payload)
        or not 0 < len(payload) <= MAX_SOURCE_BYTES
    ):
        raise _RejectedDecode("unsupported", "unsupported:source_capacity")
    provenance = receipt.provenance
    if not isinstance(provenance, Mapping):
        raise _RejectedDecode("unsupported", "unsupported:raw_provenance")
    if (
        receipt.provider != OPERA_PROVIDER
        or receipt.product != native.OPERA_HDF5_PRODUCT
        or provenance.get("representation") != NATIVE_REPRESENTATION
        or provenance.get("capture_stage") != NATIVE_CAPTURE_STAGE
        or provenance.get("collector") != native.OPERA_RAW_COLLECTOR
        or provenance.get("collector_version") != native.OPERA_RAW_COLLECTOR_VERSION
        or provenance.get("structure_inspector_version")
        != native.OPERA_STRUCTURE_INSPECTOR_VERSION
        or provenance.get("upstream_product_code") != native.OPERA_PRODUCT_CODE
        or provenance.get("declared_quantity") != native.OPERA_QUANTITY
        or provenance.get("declared_unit") != native.OPERA_UNIT
        or provenance.get("decoded_or_admitted") is not False
        or provenance.get("operational_memory_certified") is not False
    ):
        raise _RejectedDecode("unsupported", "unsupported:raw_provenance")
    timestamp = float(capture.product_timestamp)
    job_id, object_key = _canonical_slot(timestamp)
    try:
        revision = native.OperaRevisionIdentity(
            etag=str(provenance["upstream_etag"]),
            version_id=(
                None
                if provenance.get("upstream_version_id") is None
                else str(provenance["upstream_version_id"])
            ),
            content_length=int(provenance["upstream_content_length"]),
            content_type=receipt.content_type,
        )
        raw_fetch_id, capture_fetch_id = native._revision_fetch_ids(job_id, revision)
        native._assert_receipt_contract(
            receipt,
            fetch_id=raw_fetch_id,
            object_key=object_key,
            nominal_timestamp=timestamp,
            revision=revision,
        )
        expected_upstream = native._expected_upstream_capture_id(
            object_key=object_key,
            timestamp=timestamp,
            revision=revision,
            sha256=receipt.content_sha256,
        )
    except (KeyError, TypeError, ValueError, OverflowError):
        raise _RejectedDecode(
            "unsupported", "unsupported:current_collector_contract"
        ) from None
    if (
        capture.fetch_id != capture_fetch_id
        or capture.upstream_capture_id != expected_upstream
    ):
        raise _RejectedDecode(
            "unsupported", "unsupported:current_certification_contract"
        )
    verdict = native._load_validation_verdict(
        raw_authority.raw_store,
        revision,
        content_sha256=receipt.content_sha256,
    )
    if verdict is None or verdict[0].get("status") != "validated_complete":
        raise _RejectedDecode("incomplete", "incomplete:v3_validation_verdict")
    try:
        identity = native._identity_from_document(verdict[0]["identity"])
        native._assert_identity_matches_request(
            object_key=object_key,
            requested_timestamp=timestamp,
            identity=identity,
        )
    except (KeyError, TypeError, ValueError):
        raise _RejectedDecode(
            "unsupported", "unsupported:v3_validation_identity"
        ) from None
    if (
        identity.upstream_advected_contributions is not True
        or identity.motion_truth_eligible is not False
    ):
        raise _RejectedDecode("unsupported", "unsupported:motion_truth_contract")
    return _VerifiedSource(
        capture=capture,
        receipt=receipt,
        payload=payload,
        object_key=object_key,
        revision=revision,
        identity=identity,
        validation_verdict_id=str(verdict[0]["verdict_id"]),
        validation_verdict_sha256=verdict[1],
    )


def _exact_hdf_contract(
    root: h5py.File,
    *,
    source: _VerifiedSource,
    native_deadline: float,
    checkpoint: Callable[[str], None],
) -> tuple[h5py.Dataset, h5py.Dataset]:
    """Revalidate the decoder-relevant v3 contract without reading pixels."""

    checkpoint("hdf_contract_start")
    native._validate_hdf_self_contained(root, deadline=native_deadline)
    what_group = native._hard_group(root, "what")
    where_group = native._hard_group(root, "where")
    how_group = native._hard_group(root, "how")
    dataset1 = native._hard_group(root, "dataset1")
    dataset_what_group = native._hard_group(root, "dataset1/what")
    data1 = native._hard_group(root, "dataset1/data1")
    data_what_group = native._hard_group(root, "dataset1/data1/what")
    quality1 = native._hard_group(root, "dataset1/data1/quality1")
    quality_what_group = native._hard_group(
        root, "dataset1/data1/quality1/what"
    )
    quality_how_group = native._hard_group(
        root, "dataset1/data1/quality1/how"
    )
    dbzh = native._hard_dataset(root, "dataset1/data1/data")
    qi = native._hard_dataset(root, "dataset1/data1/quality1/data")

    native._exact_members(root, {"what", "where", "how", "dataset1"}, field="root")
    native._exact_members(what_group, set(), field="what")
    native._exact_members(where_group, set(), field="where")
    native._exact_members(how_group, set(), field="how")
    native._exact_members(dataset1, {"what", "data1"}, field="dataset1")
    native._exact_members(dataset_what_group, set(), field="dataset1/what")
    native._exact_members(
        data1, {"what", "data", "quality1"}, field="dataset1/data1"
    )
    native._exact_members(data_what_group, set(), field="dataset1/data1/what")
    native._exact_members(
        quality1, {"what", "how", "data"}, field="dataset1/data1/quality1"
    )
    native._exact_members(
        quality_what_group, set(), field="dataset1/data1/quality1/what"
    )
    native._exact_members(
        quality_how_group, set(), field="dataset1/data1/quality1/how"
    )
    for group, field in (
        (dataset1, "dataset1"),
        (data1, "dataset1/data1"),
        (quality1, "dataset1/data1/quality1"),
    ):
        native._exact_attr_names(group.attrs, set(), field=field)

    native._exact_attr_names(root.attrs, {"Conventions"}, field="root")
    conventions = native._ascii(
        native._required_attr(root.attrs, "Conventions"), field="Conventions"
    )
    if conventions != source.identity.conventions:
        raise ValueError("OPERA conventions differ from certified identity")

    what = what_group.attrs
    native._exact_attr_names(
        what, {"date", "object", "source", "time", "version"}, field="what"
    )
    h5rad_version = native._ascii(
        native._required_attr(what, "version"), field="what.version"
    )
    source_name = native._ascii(
        native._required_attr(what, "source"), field="what.source"
    )
    object_name = native._ascii(
        native._required_attr(what, "object"), field="what.object"
    )
    nominal_timestamp = native._timestamp_from_odim(
        native._ascii(
            native._required_attr(what, "date"), field="what.date", maximum=8
        ),
        native._ascii(
            native._required_attr(what, "time"), field="what.time", maximum=6
        ),
    )
    if (
        h5rad_version != source.identity.h5rad_version
        or source_name != source.identity.source
        or object_name != source.identity.object_name
        or nominal_timestamp != source.identity.nominal_timestamp
    ):
        raise ValueError("OPERA top-level identity differs from v3 verdict")

    how = how_group.attrs
    native._exact_attr_names(
        how, {*native.OPERA_HOW_FIXED_TEXT, "nodes"}, field="how"
    )
    for name, expected in native.OPERA_HOW_FIXED_TEXT.items():
        actual = native._ascii(
            native._required_attr(how, name), field=f"how.{name}", maximum=2048
        )
        if actual != expected:
            raise ValueError(f"OPERA how.{name} differs")
    nodes = native._parse_nodes(native._required_attr(how, "nodes"))
    nodes_sha256 = hashlib.sha256(_canonical_json(list(nodes))).hexdigest()
    if (
        len(nodes) != source.identity.node_count
        or nodes_sha256 != source.identity.nodes_sha256
    ):
        raise ValueError("OPERA nodes differ from v3 verdict")

    where = where_group.attrs
    native._exact_attr_names(
        where,
        {"xsize", "ysize", "xscale", "yscale", "projdef", *native.OPERA_CORNERS},
        field="where",
    )
    rows = native._positive_integer(
        native._required_attr(where, "ysize"), field="where.ysize", maximum=20_000
    )
    columns = native._positive_integer(
        native._required_attr(where, "xsize"), field="where.xsize", maximum=20_000
    )
    xscale = native._exact_number(
        where, "xscale", native.OPERA_GRID_SCALE_METRES, field="where.xscale"
    )
    yscale = native._exact_number(
        where, "yscale", native.OPERA_GRID_SCALE_METRES, field="where.yscale"
    )
    projdef = native._ascii(
        native._required_attr(where, "projdef"), field="where.projdef"
    )
    for name, expected in native.OPERA_CORNERS.items():
        native._exact_number(where, name, expected, field=f"where.{name}")
    if (
        rows != source.identity.rows
        or columns != source.identity.columns
        or xscale != source.identity.xscale_metres
        or yscale != source.identity.yscale_metres
        or hashlib.sha256(projdef.encode("ascii")).hexdigest()
        != source.identity.projection_sha256
    ):
        raise ValueError("OPERA grid differs from v3 verdict")

    dataset_what = dataset_what_group.attrs
    native._exact_attr_names(
        dataset_what,
        {"product", "prodname", "startdate", "starttime", "enddate", "endtime"},
        field="dataset1/what",
    )
    product = native._ascii(
        native._required_attr(dataset_what, "product"), field="dataset1.what.product"
    )
    prodname = native._ascii(
        native._required_attr(dataset_what, "prodname"),
        field="dataset1.what.prodname",
    )
    start_timestamp = native._timestamp_from_odim(
        native._ascii(
            native._required_attr(dataset_what, "startdate"),
            field="dataset1.what.startdate",
            maximum=8,
        ),
        native._ascii(
            native._required_attr(dataset_what, "starttime"),
            field="dataset1.what.starttime",
            maximum=6,
        ),
    )
    end_timestamp = native._timestamp_from_odim(
        native._ascii(
            native._required_attr(dataset_what, "enddate"),
            field="dataset1.what.enddate",
            maximum=8,
        ),
        native._ascii(
            native._required_attr(dataset_what, "endtime"),
            field="dataset1.what.endtime",
            maximum=6,
        ),
    )
    if (
        product != source.identity.product_name
        or prodname != source.identity.product_description
        or start_timestamp != source.identity.input_window_start_timestamp
        or end_timestamp != source.identity.input_window_end_timestamp
    ):
        raise ValueError("OPERA product interval differs from v3 verdict")

    dbzh_what = data_what_group.attrs
    native._exact_attr_names(
        dbzh_what,
        {"quantity", "gain", "offset", "nodata", "undetect"},
        field="dataset1/data1/what",
    )
    quantity = native._ascii(
        native._required_attr(dbzh_what, "quantity"), field="data1.what.quantity"
    )
    native._exact_number(
        dbzh_what, "gain", native.OPERA_DBZH_GAIN, field="data1.what.gain"
    )
    native._exact_number(
        dbzh_what, "offset", native.OPERA_DBZH_OFFSET, field="data1.what.offset"
    )
    native._exact_number(
        dbzh_what, "nodata", native.OPERA_DBZH_NODATA, field="data1.what.nodata"
    )
    native._exact_number(
        dbzh_what,
        "undetect",
        native.OPERA_DBZH_UNDETECT,
        field="data1.what.undetect",
    )
    if quantity != source.identity.quantity:
        raise ValueError("OPERA DBZH quantity differs from v3 verdict")

    qi_what = quality_what_group.attrs
    native._exact_attr_names(
        qi_what, {"gain", "offset", "nodata"}, field="quality1/what"
    )
    native._exact_number(
        qi_what, "gain", native.OPERA_QUALITY_GAIN, field="quality1.what.gain"
    )
    native._exact_number(
        qi_what, "offset", native.OPERA_QUALITY_OFFSET, field="quality1.what.offset"
    )
    native._exact_number(
        qi_what, "nodata", native.OPERA_QUALITY_NODATA, field="quality1.what.nodata"
    )
    qi_how = quality_how_group.attrs
    native._exact_attr_names(qi_how, {"task"}, field="quality1/how")
    quality_task = native._ascii(
        native._required_attr(qi_how, "task"), field="quality1.how.task"
    )
    if quality_task != source.identity.quality_task:
        raise ValueError("OPERA quality task differs from v3 verdict")

    for dataset, field in (
        (dbzh, "dataset1/data1/data"),
        (qi, "dataset1/data1/quality1/data"),
    ):
        native._exact_attr_names(
            dataset.attrs, {"CLASS", "IMAGE_VERSION"}, field=field
        )
        if (
            native._ascii(dataset.attrs["CLASS"], field=f"{field}.CLASS")
            != native.OPERA_IMAGE_CLASS
            or native._ascii(
                dataset.attrs["IMAGE_VERSION"], field=f"{field}.IMAGE_VERSION"
            )
            != native.OPERA_IMAGE_VERSION
        ):
            raise ValueError("OPERA raster image identity differs")
    dbzh_storage = native._validate_raster_dcpl(
        dbzh,
        rows=rows,
        columns=columns,
        file_size=len(source.payload),
        field="DBZH-raster",
        deadline=native_deadline,
    )
    qi_storage = native._validate_raster_dcpl(
        qi,
        rows=rows,
        columns=columns,
        file_size=len(source.payload),
        field="quality-raster",
        deadline=native_deadline,
    )
    if (
        dbzh_storage.object_token == qi_storage.object_token
        or dbzh_storage.chunk_count != source.identity.chunk_count_per_raster
        or qi_storage.chunk_count != source.identity.chunk_count_per_raster
    ):
        raise ValueError("OPERA DBZH/QI storage identity differs")
    intervals = sorted((*dbzh_storage.byte_intervals, *qi_storage.byte_intervals))
    if any(
        current_start < previous_end
        for (_previous_start, previous_end), (current_start, _current_end)
        in zip(intervals, intervals[1:])
    ):
        raise ValueError("OPERA DBZH/QI physical chunk storage overlaps")
    checkpoint("hdf_contract_complete")
    return dbzh, qi


def _decode_hdf_region(
    source: _VerifiedSource,
    *,
    window: OperaNativeWindow,
    native_deadline: float,
    checkpoint: Callable[[str], None],
) -> _DecodedRegion:
    if len(source.payload) > MAX_SOURCE_BYTES:
        raise _RejectedDecode("unsupported", "unsupported:source_capacity")
    try:
        with h5py.File(io.BytesIO(source.payload), "r") as root:
            dbzh_dataset, qi_dataset = _exact_hdf_contract(
                root,
                source=source,
                native_deadline=native_deadline,
                checkpoint=checkpoint,
            )
            if (
                window.row_stop > dbzh_dataset.shape[0]
                or window.column_stop > dbzh_dataset.shape[1]
                or dbzh_dataset.shape != qi_dataset.shape
            ):
                raise ValueError("OPERA selected window exceeds rasters")
            checkpoint("dbzh_slice_before")
            dbzh_raw = np.asarray(
                dbzh_dataset[
                    window.row_start:window.row_stop,
                    window.column_start:window.column_stop,
                ],
                dtype=np.dtype("<f8"),
                order="C",
            )
            checkpoint("dbzh_slice_after")
            qi_raw = np.asarray(
                qi_dataset[
                    window.row_start:window.row_stop,
                    window.column_start:window.column_stop,
                ],
                dtype=np.dtype("<f8"),
                order="C",
            )
            checkpoint("qi_slice_after")
    except (_RejectedDecode, OperaDbzhDecodeDeadlineError, TimeoutError):
        raise
    except OSError as error:
        if getattr(error, "errno", None) is not None:
            raise
        raise _RejectedDecode("decode_error", "decode_error:hdf5_container") from error
    except (KeyError, TypeError, ValueError, RuntimeError, IndexError, OverflowError) as error:
        raise _RejectedDecode("unsupported", "unsupported:hdf5_contract") from error

    expected_shape = (window.rows, window.columns)
    if (
        dbzh_raw.shape != expected_shape
        or qi_raw.shape != expected_shape
        or dbzh_raw.dtype.str != "<f8"
        or qi_raw.dtype.str != "<f8"
        or dbzh_raw.nbytes + qi_raw.nbytes > 16 * MAX_REGION_PIXELS
    ):
        raise _RejectedDecode("decode_error", "decode_error:array_shape")
    dbzh_raw = np.ascontiguousarray(dbzh_raw, dtype=np.dtype("<f8"))
    qi_raw = np.ascontiguousarray(qi_raw, dtype=np.dtype("<f8"))
    dbzh_nodata = np.asarray(dbzh_raw == native.OPERA_DBZH_NODATA, dtype=np.uint8)
    dbzh_undetect = np.asarray(
        dbzh_raw == native.OPERA_DBZH_UNDETECT, dtype=np.uint8
    )
    dbzh_detected = np.asarray(
        (dbzh_nodata == 0) & (dbzh_undetect == 0), dtype=np.uint8
    )
    if (
        np.any((dbzh_nodata == 1) & (dbzh_undetect == 1))
        or not np.isfinite(dbzh_raw[dbzh_detected == 1]).all()
    ):
        raise _RejectedDecode("decode_error", "decode_error:dbzh_domain")
    detected_dbz = np.zeros(expected_shape, dtype=np.dtype("<f8"))
    detected_dbz[dbzh_detected == 1] = (
        dbzh_raw[dbzh_detected == 1] * native.OPERA_DBZH_GAIN
        + native.OPERA_DBZH_OFFSET
    )
    qi_missing = np.asarray(qi_raw == native.OPERA_QUALITY_NODATA, dtype=np.uint8)
    qi_valid = np.asarray(qi_missing == 0, dtype=np.uint8)
    if (
        not np.isfinite(qi_raw[qi_valid == 1]).all()
        or np.any(qi_raw[qi_valid == 1] < 0.0)
        or np.any(qi_raw[qi_valid == 1] > 1.0)
    ):
        raise _RejectedDecode("decode_error", "decode_error:qi_domain")
    coverage = np.ones(expected_shape, dtype=np.uint8)
    arrays: dict[str, np.ndarray] = {
        "dbzh_raw": dbzh_raw,
        "detected_dbz": detected_dbz,
        "dbzh_nodata_mask": dbzh_nodata,
        "dbzh_undetect_mask": dbzh_undetect,
        "dbzh_detected_mask": dbzh_detected,
        "coverage_mask": coverage,
        "qi_raw": qi_raw,
        "qi_valid_mask": qi_valid,
        "qi_missing_mask": qi_missing,
    }
    total_bytes = sum(array.nbytes for array in arrays.values())
    if total_bytes > MAX_UNCOMPRESSED_ARRAY_BYTES:
        raise _RejectedDecode("decode_error", "decode_error:array_capacity")
    counts = {
        "dbzh_detected": int(np.count_nonzero(dbzh_detected)),
        "dbzh_nodata": int(np.count_nonzero(dbzh_nodata)),
        "dbzh_undetect": int(np.count_nonzero(dbzh_undetect)),
        "qi_missing": int(np.count_nonzero(qi_missing)),
        "qi_valid": int(np.count_nonzero(qi_valid)),
        "selected_pixels": window.pixels,
    }
    checkpoint("semantic_masks_complete")
    return _DecodedRegion(window=window, arrays=arrays, counts=counts)


def _artifact_capture_metadata(capture: RawArtifactCaptureReceipt) -> dict[str, Any]:
    return {
        "capture_id": capture.capture_id,
        "capture_manifest_sha256": capture.manifest_sha256,
        "capture_receipt_sha256": capture.capture_receipt_sha256,
        "fetch_id": capture.fetch_id,
        "product": capture.product,
        "product_timestamp": capture.product_timestamp,
        "provider": capture.provider,
        "upstream_capture_id": capture.upstream_capture_id,
    }


def _artifact_source_metadata(source: _VerifiedSource) -> dict[str, Any]:
    provenance = source.receipt.provenance
    return {
        "certification_contract": native.OPERA_CERTIFICATION_CONTRACT_VERSION,
        "collector": native.OPERA_RAW_COLLECTOR,
        "collector_version": native.OPERA_RAW_COLLECTOR_VERSION,
        "content_sha256": source.receipt.content_sha256,
        "content_type": source.receipt.content_type,
        "object_key": source.object_key,
        "raw_receipt_id": source.receipt.receipt_id,
        "raw_receipt_sha256": source.receipt.receipt_sha256,
        "raw_size_bytes": source.receipt.size_bytes,
        "revision_token": native._revision_token(source.revision),
        "structure_inspector_version": native.OPERA_STRUCTURE_INSPECTOR_VERSION,
        "upstream_etag": source.revision.etag,
        "upstream_version_id": source.revision.version_id,
        "validation_verdict_id": source.validation_verdict_id,
        "validation_verdict_sha256": source.validation_verdict_sha256,
        "rights_status": provenance.get("rights_status"),
    }


def _artifact_product_metadata(source: _VerifiedSource) -> dict[str, Any]:
    identity = source.identity
    return {
        "dbzh_gain": native.OPERA_DBZH_GAIN,
        "dbzh_nodata": native.OPERA_DBZH_NODATA,
        "dbzh_offset": native.OPERA_DBZH_OFFSET,
        "dbzh_undetect": native.OPERA_DBZH_UNDETECT,
        "hydrometeor_classification_applied": False,
        "input_window_end_timestamp": identity.input_window_end_timestamp,
        "input_window_start_timestamp": identity.input_window_start_timestamp,
        "motion_truth_eligible": False,
        "nominal_timestamp": identity.nominal_timestamp,
        "operational_admission_eligible": False,
        "qi_gain": native.OPERA_QUALITY_GAIN,
        "qi_missing": native.OPERA_QUALITY_NODATA,
        "qi_offset": native.OPERA_QUALITY_OFFSET,
        "qi_task": native.OPERA_QUALITY_TASK,
        "qi_zero_is_valid_quality": True,
        "quantity": "maximum_horizontal_reflectivity_factor",
        "rain_rate_conversion_applied": False,
        "unit": "dBZ",
        "upstream_advected_contributions": True,
    }


def _artifact_grid_metadata(window: OperaNativeWindow) -> dict[str, Any]:
    return {
        "axis_order": "yx",
        "columns": window.columns,
        "coverage_definition_sha256": window.coverage_definition_sha256,
        "densification_segments_per_edge": BBOX_DENSIFY_SEGMENTS_PER_EDGE,
        "densified_point_count": window.densified_point_count,
        "grid_definition_sha256": window.grid_definition_sha256,
        "native_bbox": {
            "maximum_x": window.maximum_x,
            "maximum_y": window.maximum_y,
            "minimum_x": window.minimum_x,
            "minimum_y": window.minimum_y,
        },
        "projection": native.OPERA_PROJDEF,
        "projection_guard_metres": BBOX_PROJECTION_GUARD_METRES,
        "requested_wgs84_bbox": _region_mapping(window.requested),
        "row_direction": "north_to_south",
        "row_start": window.row_start,
        "row_stop_exclusive": window.row_stop,
        "rows": window.rows,
        "column_start": window.column_start,
        "column_stop_exclusive": window.column_stop,
        "xscale_metres": native.OPERA_GRID_SCALE_METRES,
        "yscale_metres": native.OPERA_GRID_SCALE_METRES,
    }


def _artifact_metadata_core(
    source: _VerifiedSource,
    window: OperaNativeWindow,
) -> dict[str, Any]:
    return {
        "capture": _artifact_capture_metadata(source.capture),
        "decoder": {
            "config_sha256": DECODER_CONFIG_SHA256,
            "identity": DECODER_IDENTITY,
            "version": DECODER_VERSION,
        },
        "grid": _artifact_grid_metadata(window),
        "product": _artifact_product_metadata(source),
        "schema": ARTIFACT_SCHEMA,
        "source": _artifact_source_metadata(source),
    }


def _artifact_bytes(
    source: _VerifiedSource,
    frame: _DecodedRegion,
    *,
    checkpoint: Callable[[str], None],
) -> tuple[bytes, str]:
    descriptors: list[dict[str, Any]] = []
    compressed_arrays: list[bytes] = []
    for name, dtype_text in _ARRAY_SPECS:
        checkpoint(f"artifact_encode_{name}")
        array = frame.arrays.get(name)
        if not isinstance(array, np.ndarray):
            raise OperaDbzhArtifactError(f"artifact array {name} is absent")
        expected_dtype = np.dtype(dtype_text)
        if array.shape != (frame.window.rows, frame.window.columns):
            raise OperaDbzhArtifactError(f"artifact array {name} shape differs")
        normalized = np.ascontiguousarray(array, dtype=expected_dtype)
        raw = normalized.tobytes(order="C")
        if len(raw) != normalized.size * expected_dtype.itemsize:
            raise OperaDbzhArtifactError(f"artifact array {name} byte size differs")
        compressed = zlib.compress(raw, level=9)
        descriptors.append({
            "codec": "zlib-9",
            "compressed_bytes": len(compressed),
            "compressed_sha256": hashlib.sha256(compressed).hexdigest(),
            "dtype": dtype_text,
            "name": name,
            "raw_bytes": len(raw),
            "raw_sha256": hashlib.sha256(raw).hexdigest(),
            "shape": [frame.window.rows, frame.window.columns],
        })
        compressed_arrays.append(compressed)
    metadata = {
        **_artifact_metadata_core(source, frame.window),
        "arrays": descriptors,
        "statistics": dict(frame.counts),
    }
    header = _canonical_json(metadata)
    if not 0 < len(header) <= MAX_HEADER_BYTES:
        raise OperaDbzhArtifactCapacityError("artifact metadata exceeds header bound")
    payload = b"".join(
        (ARTIFACT_MAGIC, struct.pack(">Q", len(header)), header, *compressed_arrays)
    )
    if not 0 < len(payload) <= MAX_ARTIFACT_BYTES:
        raise OperaDbzhArtifactCapacityError("artifact exceeds immutable object bound")
    checkpoint("artifact_serialized")
    return payload, hashlib.sha256(header).hexdigest()


def _bounded_decompress(payload: bytes, expected_bytes: int) -> bytes:
    if (
        not isinstance(payload, bytes)
        or isinstance(expected_bytes, bool)
        or not isinstance(expected_bytes, int)
        or not 0 <= expected_bytes <= MAX_UNCOMPRESSED_ARRAY_BYTES
    ):
        raise OperaDbzhArtifactError("compressed array bound is invalid")
    decompressor = zlib.decompressobj()
    try:
        decoded = decompressor.decompress(payload, expected_bytes + 1)
        if decompressor.unconsumed_tail:
            raise OperaDbzhArtifactError("compressed array exceeds raw byte bound")
        decoded += decompressor.flush()
    except zlib.error as error:
        raise OperaDbzhArtifactError("compressed array is invalid") from error
    if (
        len(decoded) != expected_bytes
        or not decompressor.eof
        or decompressor.unused_data
    ):
        raise OperaDbzhArtifactError("compressed array length differs")
    return decoded


def _exact_mapping(
    value: object,
    expected: set[str],
    field: str,
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != expected:
        raise OperaDbzhArtifactError(f"artifact {field} schema differs")
    return value


def _artifact_arrays_semantically_valid(
    arrays: Mapping[str, np.ndarray],
) -> bool:
    dbzh_raw = arrays["dbzh_raw"]
    detected_dbz = arrays["detected_dbz"]
    nodata = arrays["dbzh_nodata_mask"]
    undetect = arrays["dbzh_undetect_mask"]
    detected = arrays["dbzh_detected_mask"]
    coverage = arrays["coverage_mask"]
    qi_raw = arrays["qi_raw"]
    qi_valid = arrays["qi_valid_mask"]
    qi_missing = arrays["qi_missing_mask"]
    expected_nodata = np.asarray(
        dbzh_raw == native.OPERA_DBZH_NODATA, dtype=np.uint8
    )
    expected_undetect = np.asarray(
        dbzh_raw == native.OPERA_DBZH_UNDETECT, dtype=np.uint8
    )
    expected_detected = np.asarray(
        (expected_nodata == 0) & (expected_undetect == 0), dtype=np.uint8
    )
    expected_qi_missing = np.asarray(
        qi_raw == native.OPERA_QUALITY_NODATA, dtype=np.uint8
    )
    expected_qi_valid = np.asarray(expected_qi_missing == 0, dtype=np.uint8)
    if (
        not np.array_equal(nodata, expected_nodata)
        or not np.array_equal(undetect, expected_undetect)
        or not np.array_equal(detected, expected_detected)
        or not np.array_equal(qi_missing, expected_qi_missing)
        or not np.array_equal(qi_valid, expected_qi_valid)
        or not np.all(coverage == 1)
        or not np.all(detected_dbz[expected_detected == 0] == 0.0)
        or not np.isfinite(dbzh_raw[expected_detected == 1]).all()
        or not np.isfinite(qi_raw[expected_qi_valid == 1]).all()
        or np.any(qi_raw[expected_qi_valid == 1] < 0.0)
        or np.any(qi_raw[expected_qi_valid == 1] > 1.0)
    ):
        return False
    expected_dbz = (
        dbzh_raw[expected_detected == 1] * native.OPERA_DBZH_GAIN
        + native.OPERA_DBZH_OFFSET
    )
    return np.array_equal(detected_dbz[expected_detected == 1], expected_dbz)


def _parse_artifact(
    payload: bytes,
    *,
    checkpoint: Callable[[str], None],
) -> OperaDbzhRasterArtifact:
    if not isinstance(payload, bytes) or not 0 < len(payload) <= MAX_ARTIFACT_BYTES:
        raise OperaDbzhArtifactError("artifact payload size is invalid")
    prefix_bytes = len(ARTIFACT_MAGIC) + 8
    if len(payload) < prefix_bytes or not payload.startswith(ARTIFACT_MAGIC):
        raise OperaDbzhArtifactError("artifact magic differs")
    header_bytes = struct.unpack(
        ">Q", payload[len(ARTIFACT_MAGIC):prefix_bytes]
    )[0]
    if not 0 < header_bytes <= MAX_HEADER_BYTES:
        raise OperaDbzhArtifactError("artifact header length is invalid")
    header_end = prefix_bytes + header_bytes
    if header_end > len(payload):
        raise OperaDbzhArtifactError("artifact header is truncated")
    header = payload[prefix_bytes:header_end]
    try:
        metadata = json.loads(header.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise OperaDbzhArtifactError("artifact header is not JSON") from error
    if not isinstance(metadata, dict) or header != _canonical_json(metadata):
        raise OperaDbzhArtifactError("artifact header is not canonical")
    _exact_mapping(
        metadata,
        {"arrays", "capture", "decoder", "grid", "product", "schema", "source", "statistics"},
        "root",
    )
    if metadata.get("schema") != ARTIFACT_SCHEMA:
        raise OperaDbzhArtifactError("artifact schema differs")
    decoder = _exact_mapping(
        metadata.get("decoder"), {"config_sha256", "identity", "version"}, "decoder"
    )
    if decoder != {
        "config_sha256": DECODER_CONFIG_SHA256,
        "identity": DECODER_IDENTITY,
        "version": DECODER_VERSION,
    }:
        raise OperaDbzhArtifactError("artifact decoder identity differs")
    capture_metadata = _exact_mapping(
        metadata.get("capture"),
        {
            "capture_id",
            "capture_manifest_sha256",
            "capture_receipt_sha256",
            "fetch_id",
            "product",
            "product_timestamp",
            "provider",
            "upstream_capture_id",
        },
        "capture",
    )
    if (
        not isinstance(capture_metadata.get("capture_id"), str)
        or _CAPTURE_ID_RE.fullmatch(str(capture_metadata.get("capture_id"))) is None
        or any(
            not isinstance(capture_metadata.get(field), str)
            or _SHA256_RE.fullmatch(str(capture_metadata.get(field))) is None
            for field in ("capture_manifest_sha256", "capture_receipt_sha256")
        )
        or capture_metadata.get("provider") != OPERA_PROVIDER
        or capture_metadata.get("product") != native.OPERA_CAPTURE_PRODUCT
        or isinstance(capture_metadata.get("product_timestamp"), bool)
        or not isinstance(capture_metadata.get("product_timestamp"), (int, float))
        or not math.isfinite(float(capture_metadata["product_timestamp"]))
    ):
        raise OperaDbzhArtifactError("artifact capture identity is invalid")
    source_metadata = _exact_mapping(
        metadata.get("source"),
        {
            "certification_contract",
            "collector",
            "collector_version",
            "content_sha256",
            "content_type",
            "object_key",
            "raw_receipt_id",
            "raw_receipt_sha256",
            "raw_size_bytes",
            "revision_token",
            "rights_status",
            "structure_inspector_version",
            "upstream_etag",
            "upstream_version_id",
            "validation_verdict_id",
            "validation_verdict_sha256",
        },
        "source",
    )
    if (
        source_metadata.get("certification_contract")
        != native.OPERA_CERTIFICATION_CONTRACT_VERSION
        or source_metadata.get("collector") != native.OPERA_RAW_COLLECTOR
        or source_metadata.get("collector_version")
        != native.OPERA_RAW_COLLECTOR_VERSION
        or source_metadata.get("structure_inspector_version")
        != native.OPERA_STRUCTURE_INSPECTOR_VERSION
        or not isinstance(source_metadata.get("object_key"), str)
        or native._KEY_RE.fullmatch(str(source_metadata.get("object_key"))) is None
        or any(
            not isinstance(source_metadata.get(field), str)
            or _SHA256_RE.fullmatch(str(source_metadata.get(field))) is None
            for field in (
                "content_sha256",
                "raw_receipt_sha256",
                "revision_token",
                "validation_verdict_id",
                "validation_verdict_sha256",
            )
        )
        or isinstance(source_metadata.get("raw_size_bytes"), bool)
        or not isinstance(source_metadata.get("raw_size_bytes"), int)
        or not 0 < int(source_metadata["raw_size_bytes"]) <= MAX_SOURCE_BYTES
    ):
        raise OperaDbzhArtifactError("artifact source identity is invalid")
    product_metadata = _exact_mapping(
        metadata.get("product"),
        {
            "dbzh_gain",
            "dbzh_nodata",
            "dbzh_offset",
            "dbzh_undetect",
            "hydrometeor_classification_applied",
            "input_window_end_timestamp",
            "input_window_start_timestamp",
            "motion_truth_eligible",
            "nominal_timestamp",
            "operational_admission_eligible",
            "qi_gain",
            "qi_missing",
            "qi_offset",
            "qi_task",
            "qi_zero_is_valid_quality",
            "quantity",
            "rain_rate_conversion_applied",
            "unit",
            "upstream_advected_contributions",
        },
        "product",
    )
    if (
        product_metadata.get("dbzh_gain") != native.OPERA_DBZH_GAIN
        or product_metadata.get("dbzh_offset") != native.OPERA_DBZH_OFFSET
        or product_metadata.get("dbzh_nodata") != native.OPERA_DBZH_NODATA
        or product_metadata.get("dbzh_undetect") != native.OPERA_DBZH_UNDETECT
        or product_metadata.get("qi_gain") != native.OPERA_QUALITY_GAIN
        or product_metadata.get("qi_offset") != native.OPERA_QUALITY_OFFSET
        or product_metadata.get("qi_missing") != native.OPERA_QUALITY_NODATA
        or product_metadata.get("qi_task") != native.OPERA_QUALITY_TASK
    ):
        raise OperaDbzhArtifactError("artifact quantitative constants differ")
    grid = _exact_mapping(
        metadata.get("grid"),
        {
            "axis_order",
            "column_start",
            "column_stop_exclusive",
            "columns",
            "coverage_definition_sha256",
            "densification_segments_per_edge",
            "densified_point_count",
            "grid_definition_sha256",
            "native_bbox",
            "projection",
            "projection_guard_metres",
            "requested_wgs84_bbox",
            "row_direction",
            "row_start",
            "row_stop_exclusive",
            "rows",
            "xscale_metres",
            "yscale_metres",
        },
        "grid",
    )
    rows = grid.get("rows")
    columns = grid.get("columns")
    if (
        isinstance(rows, bool)
        or not isinstance(rows, int)
        or isinstance(columns, bool)
        or not isinstance(columns, int)
        or not 1 <= rows <= MAX_REGION_ROWS
        or not 1 <= columns <= MAX_REGION_COLUMNS
        or rows * columns > MAX_REGION_PIXELS
        or grid.get("axis_order") != "yx"
        or grid.get("row_direction") != "north_to_south"
        or grid.get("projection") != native.OPERA_PROJDEF
    ):
        raise OperaDbzhArtifactError("artifact grid is invalid")
    native_bbox = _exact_mapping(
        grid.get("native_bbox"),
        {"maximum_x", "maximum_y", "minimum_x", "minimum_y"},
        "grid.native_bbox",
    )
    requested_bbox = _exact_mapping(
        grid.get("requested_wgs84_bbox"),
        {
            "maximum_latitude",
            "maximum_longitude",
            "minimum_latitude",
            "minimum_longitude",
        },
        "grid.requested_wgs84_bbox",
    )
    if not all(
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
        for value in (*native_bbox.values(), *requested_bbox.values())
    ):
        raise OperaDbzhArtifactError("artifact bbox is invalid")
    descriptors = metadata.get("arrays")
    if not isinstance(descriptors, list) or len(descriptors) != len(_ARRAY_SPECS):
        raise OperaDbzhArtifactError("artifact array descriptors differ")
    arrays: dict[str, np.ndarray] = {}
    cursor = header_end
    for descriptor, (expected_name, expected_dtype) in zip(descriptors, _ARRAY_SPECS):
        checkpoint(f"artifact_parse_{expected_name}")
        item = _exact_mapping(
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
            f"array.{expected_name}",
        )
        dtype = np.dtype(expected_dtype)
        raw_bytes = rows * columns * dtype.itemsize
        compressed_bytes = item.get("compressed_bytes")
        if (
            item.get("codec") != "zlib-9"
            or item.get("name") != expected_name
            or item.get("dtype") != expected_dtype
            or item.get("shape") != [rows, columns]
            or item.get("raw_bytes") != raw_bytes
            or not isinstance(item.get("raw_sha256"), str)
            or _SHA256_RE.fullmatch(str(item.get("raw_sha256"))) is None
            or not isinstance(item.get("compressed_sha256"), str)
            or _SHA256_RE.fullmatch(str(item.get("compressed_sha256"))) is None
            or isinstance(compressed_bytes, bool)
            or not isinstance(compressed_bytes, int)
            or not 1 <= compressed_bytes <= MAX_ARTIFACT_BYTES
        ):
            raise OperaDbzhArtifactError(f"artifact descriptor {expected_name} differs")
        stop = cursor + compressed_bytes
        if stop > len(payload):
            raise OperaDbzhArtifactError("artifact compressed array is truncated")
        compressed = payload[cursor:stop]
        cursor = stop
        if not hmac.compare_digest(
            hashlib.sha256(compressed).hexdigest(),
            str(item["compressed_sha256"]),
        ):
            raise OperaDbzhArtifactError("artifact compressed array hash differs")
        raw = _bounded_decompress(compressed, raw_bytes)
        if not hmac.compare_digest(
            hashlib.sha256(raw).hexdigest(), str(item["raw_sha256"])
        ):
            raise OperaDbzhArtifactError("artifact raw array hash differs")
        array = np.frombuffer(raw, dtype=dtype).reshape((rows, columns)).copy()
        array.flags.writeable = False
        arrays[expected_name] = array
    if cursor != len(payload):
        raise OperaDbzhArtifactError("artifact has trailing bytes")
    if not _artifact_arrays_semantically_valid(arrays):
        raise OperaDbzhArtifactError("artifact arrays fail semantic revalidation")
    statistics = _exact_mapping(
        metadata.get("statistics"),
        {"dbzh_detected", "dbzh_nodata", "dbzh_undetect", "qi_missing", "qi_valid", "selected_pixels"},
        "statistics",
    )
    expected_statistics = {
        "dbzh_detected": int(np.count_nonzero(arrays["dbzh_detected_mask"])),
        "dbzh_nodata": int(np.count_nonzero(arrays["dbzh_nodata_mask"])),
        "dbzh_undetect": int(np.count_nonzero(arrays["dbzh_undetect_mask"])),
        "qi_missing": int(np.count_nonzero(arrays["qi_missing_mask"])),
        "qi_valid": int(np.count_nonzero(arrays["qi_valid_mask"])),
        "selected_pixels": rows * columns,
    }
    if statistics != expected_statistics:
        raise OperaDbzhArtifactError("artifact statistics differ from arrays")
    if (
        product_metadata.get("quantity")
        != "maximum_horizontal_reflectivity_factor"
        or product_metadata.get("unit") != "dBZ"
        or product_metadata.get("rain_rate_conversion_applied") is not False
        or product_metadata.get("hydrometeor_classification_applied") is not False
        or product_metadata.get("upstream_advected_contributions") is not True
        or product_metadata.get("motion_truth_eligible") is not False
        or product_metadata.get("operational_admission_eligible") is not False
        or product_metadata.get("qi_zero_is_valid_quality") is not True
    ):
        raise OperaDbzhArtifactError("artifact product semantics differ")
    return OperaDbzhRasterArtifact(
        metadata=metadata,
        dbzh_raw=arrays["dbzh_raw"],
        detected_dbz=arrays["detected_dbz"],
        dbzh_nodata_mask=arrays["dbzh_nodata_mask"],
        dbzh_undetect_mask=arrays["dbzh_undetect_mask"],
        dbzh_detected_mask=arrays["dbzh_detected_mask"],
        coverage_mask=arrays["coverage_mask"],
        qi_raw=arrays["qi_raw"],
        qi_valid_mask=arrays["qi_valid_mask"],
        qi_missing_mask=arrays["qi_missing_mask"],
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
    window: OperaNativeWindow | None = None,
    artifact: OperaDbzhArtifactRef | None = None,
) -> dict[str, Any]:
    complete = outcome == "decoded_complete"
    if complete != (window is not None and artifact is not None):
        raise RuntimeError("OPERA decoder result violates fail-closed invariant")
    grid = None
    coverage = None
    if window is not None:
        grid = {
            "axis_order": "yx",
            "definition_sha256": window.grid_definition_sha256,
            "height": window.rows,
            "kind": "regular",
            "width": window.columns,
        }
        coverage = {
            "definition_sha256": window.coverage_definition_sha256,
            "kind": "native_bbox",
            "maximum_x": window.maximum_x,
            "maximum_y": window.maximum_y,
            "minimum_x": window.minimum_x,
            "minimum_y": window.minimum_y,
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
        "native_crs": native.OPERA_PROJDEF if complete else None,
        "native_grid": grid,
        "non_operational_candidate_artifact": (
            None
            if artifact is None
            else {"handle": artifact.handle, "sha256": artifact.sha256}
        ),
        "outcome": outcome,
        "product": capture.product,
        "product_time": capture.product_timestamp,
        "provider": capture.provider,
        "quantity": "maximum_horizontal_reflectivity_factor" if complete else None,
        "reason_code": reason_code,
        "received_at": max(member.received_at for member in capture.members),
        "unit": "dBZ" if complete else None,
    }


def _artifact_from_verdict(document: Mapping[str, Any]) -> OperaDbzhArtifactRef:
    artifact = document["artifact"]
    return OperaDbzhArtifactRef(
        handle=str(artifact["handle"]),
        sha256=str(artifact["sha256"]),
        size_bytes=int(artifact["size_bytes"]),
    )


def _verified_cached_artifact(
    artifact_store: OperaDbzhDerivedArtifactStore,
    document: Mapping[str, Any],
    *,
    source: _VerifiedSource,
    window: OperaNativeWindow,
    checkpoint: Callable[[str], None],
) -> OperaDbzhArtifactRef:
    reference = _artifact_from_verdict(document)
    decoded = artifact_store.read_decoded(reference, checkpoint=checkpoint)
    metadata_sha256 = hashlib.sha256(_canonical_json(decoded.metadata)).hexdigest()
    if (
        metadata_sha256 != document["artifact"]["metadata_sha256"]
        or any(
            decoded.metadata.get(key) != value
            for key, value in _artifact_metadata_core(source, window).items()
        )
    ):
        raise OperaDbzhArtifactError("cached artifact differs from current raw authority")
    return reference


def decode_opera_dbzh_region(
    *,
    capture_id: str,
    decoder_job_id: str,
    attempt_id: str,
    region: OperaWgs84Region,
    raw_authority: OperaDbzhRawAuthority,
    artifact_store: OperaDbzhDerivedArtifactStore,
    evidence_store: CanonicalEvidenceStore,
    evaluated_at: float,
    monotonic_deadline: float | None = None,
    monotonic_clock: Callable[[], float] = time.monotonic,
) -> OperaDbzhDecodeResult:
    """Decode one bounded region and return only a non-operational candidate."""

    if not isinstance(raw_authority, OperaDbzhRawAuthority):
        raise TypeError("raw_authority has wrong type")
    if not isinstance(artifact_store, OperaDbzhDerivedArtifactStore):
        raise TypeError("artifact_store has wrong type")
    if not isinstance(evidence_store, CanonicalEvidenceStore):
        raise TypeError("evidence_store has wrong type")
    if getattr(evidence_store, "_raw_artifact_reader", None) is not raw_authority:
        raise TypeError("evidence_store must share this single-read raw authority")
    initial_monotonic = float(monotonic_clock())
    if not math.isfinite(initial_monotonic):
        raise ValueError("monotonic clock must be finite")
    if monotonic_deadline is None:
        deadline = initial_monotonic + DECODE_TOTAL_DEADLINE_SECONDS
    else:
        if isinstance(monotonic_deadline, bool) or not isinstance(
            monotonic_deadline, (int, float)
        ):
            raise ValueError("decoder deadline must be finite")
        deadline = float(monotonic_deadline)
        if not math.isfinite(deadline):
            raise ValueError("decoder deadline must be finite")

    def checkpoint(stage: str) -> None:
        current = float(monotonic_clock())
        if not math.isfinite(current) or current >= deadline:
            raise OperaDbzhDecodeDeadlineError(
                f"OPERA decoder deadline expired at {stage}"
            )

    checkpoint("decode_start")
    window = _window_for_region(region, checkpoint=checkpoint)
    checkpoint("region_validated")
    bundle = raw_authority.load_capture_bundle_bytes(capture_id)
    if bundle is None:
        raise OperaDbzhCaptureMissingError("schema-3 OPERA capture is missing")
    checkpoint("raw_bundle_verified")
    capture, receipts, payloads = bundle
    received_at = max(member.received_at for member in capture.members)
    if (
        isinstance(evaluated_at, bool)
        or not isinstance(evaluated_at, (int, float))
        or not math.isfinite(float(evaluated_at))
        or float(evaluated_at) < received_at
    ):
        raise ValueError("evaluated_at must be finite and follow raw receipt")
    evaluation = float(evaluated_at)
    try:
        source = _validate_raw_capture(
            raw_authority,
            capture,
            receipts=receipts,
            payloads=payloads,
        )
    except _RejectedDecode as rejected:
        checkpoint("canonical_contract_rejection_before")
        receipt = evidence_store.commit(_run_mapping(
            capture,
            decoder_job_id=decoder_job_id,
            attempt_id=attempt_id,
            evaluated_at=evaluation,
            outcome=rejected.outcome,
            reason_code=rejected.reason_code,
        ))
        return OperaDbzhDecodeResult(
            outcome=rejected.outcome,
            reason_code=rejected.reason_code,
            evidence_receipt=receipt,
            artifact=None,
        )

    checkpoint("raw_capture_validated")
    key = _verdict_key(source, window)
    verdict_id = hashlib.sha256(_canonical_json(key)).hexdigest()
    cached = artifact_store.load_verdict(
        verdict_id,
        expected_key=key,
        checkpoint=checkpoint,
    )
    if cached is not None and cached["status"] == "deterministic_rejection":
        outcome = str(cached["outcome"])
        reason = str(cached["reason_code"])
        checkpoint("canonical_cached_rejection_before")
        receipt = evidence_store.commit(_run_mapping(
            capture,
            decoder_job_id=decoder_job_id,
            attempt_id=attempt_id,
            evaluated_at=evaluation,
            outcome=outcome,
            reason_code=reason,
        ))
        return OperaDbzhDecodeResult(
            outcome=outcome,
            reason_code=reason,
            evidence_receipt=receipt,
            artifact=None,
        )
    if cached is not None:
        artifact = _verified_cached_artifact(
            artifact_store,
            cached,
            source=source,
            window=window,
            checkpoint=checkpoint,
        )
        checkpoint("canonical_cached_success_before")
        receipt = evidence_store.commit(_run_mapping(
            capture,
            decoder_job_id=decoder_job_id,
            attempt_id=attempt_id,
            evaluated_at=evaluation,
            outcome="decoded_complete",
            reason_code="decoded_complete",
            window=window,
            artifact=artifact,
        ))
        return OperaDbzhDecodeResult(
            outcome="decoded_complete",
            reason_code="decoded_complete",
            evidence_receipt=receipt,
            artifact=artifact,
        )

    try:
        remaining = deadline - float(monotonic_clock())
        if not math.isfinite(remaining) or remaining <= 0.0:
            raise OperaDbzhDecodeDeadlineError(
                "OPERA decoder deadline expired before HDF inspection"
            )
        frame = _decode_hdf_region(
            source,
            window=window,
            native_deadline=time.monotonic() + remaining,
            checkpoint=checkpoint,
        )
    except _RejectedDecode as rejected:
        artifact_store.publish_verdict(
            _rejection_verdict(key, rejected), checkpoint=checkpoint
        )
        checkpoint("canonical_rejection_before")
        receipt = evidence_store.commit(_run_mapping(
            capture,
            decoder_job_id=decoder_job_id,
            attempt_id=attempt_id,
            evaluated_at=evaluation,
            outcome=rejected.outcome,
            reason_code=rejected.reason_code,
        ))
        return OperaDbzhDecodeResult(
            outcome=rejected.outcome,
            reason_code=rejected.reason_code,
            evidence_receipt=receipt,
            artifact=None,
        )

    artifact_payload, metadata_sha256 = _artifact_bytes(
        source, frame, checkpoint=checkpoint
    )
    artifact = artifact_store.publish(
        artifact_payload, checkpoint=checkpoint
    )
    verified = artifact_store.read_decoded(artifact, checkpoint=checkpoint)
    if (
        hashlib.sha256(_canonical_json(verified.metadata)).hexdigest()
        != metadata_sha256
        or any(
            verified.metadata.get(key_name) != value
            for key_name, value in _artifact_metadata_core(source, window).items()
        )
    ):
        raise OperaDbzhArtifactError("published artifact differs from raw decode")
    artifact_store.publish_verdict(
        _success_verdict(
            key, artifact, metadata_sha256=metadata_sha256
        ),
        checkpoint=checkpoint,
    )
    checkpoint("canonical_success_before")
    evidence_receipt = evidence_store.commit(_run_mapping(
        capture,
        decoder_job_id=decoder_job_id,
        attempt_id=attempt_id,
        evaluated_at=evaluation,
        outcome="decoded_complete",
        reason_code="decoded_complete",
        window=window,
        artifact=artifact,
    ))
    return OperaDbzhDecodeResult(
        outcome="decoded_complete",
        reason_code="decoded_complete",
        evidence_receipt=evidence_receipt,
        artifact=artifact,
    )


__all__ = [
    "ARTIFACT_SCHEMA",
    "DECODER_CONFIG_SHA256",
    "DECODER_IDENTITY",
    "DECODER_VERSION",
    "OperaDbzhArtifactCapacityError",
    "OperaDbzhArtifactError",
    "OperaDbzhArtifactRef",
    "OperaDbzhCaptureMissingError",
    "OperaDbzhDecodeDeadlineError",
    "OperaDbzhDecodeResult",
    "OperaDbzhDerivedArtifactStore",
    "OperaDbzhQuantitativeError",
    "OperaDbzhRasterArtifact",
    "OperaDbzhRawAuthority",
    "OperaNativeWindow",
    "OperaWgs84Region",
    "decode_opera_dbzh_region",
    "open_opera_evidence_store",
]
