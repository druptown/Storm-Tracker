"""Bounded, non-operational NOAA Enterprise Rain Rate decoder candidate.

The decoder consumes only a verified schema-3 capture produced by
``noaa_err_native``.  It validates the certified GLB-5 NetCDF profile, reads
only one explicitly bounded geographic window, preserves RRQPE and DQF source
codes and publishes a deterministic immutable candidate artifact.  Nothing in
this module is a service handler, scheduler, admission decision, ground
confirmation, cell detector or Home Assistant integration.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
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

from .canonical_evidence_store import (
    CanonicalEvidenceStore,
    DecoderRunReceipt,
    EVIDENCE_CONTRACT,
)
from . import noaa_err_native as native
from .raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RAW_ARTIFACT_CAPTURE_SCHEMA,
    RawArtifactCaptureMember,
    RawArtifactCaptureReceipt,
    RawArtifactIntegrityError,
    RawArtifactReceipt,
    RawArtifactStore,
)


DECODER_IDENTITY = "noaa_enterprise_rainrate_glb5_bounded_region"
DECODER_VERSION = "worker-noaa-err-quantitative-v3"
ARTIFACT_SCHEMA = "stv3-worker-noaa-err-quantitative-region-v2"
ARTIFACT_MAGIC = b"STV3-NOAA-ERR-Q2\n"
ARTIFACT_HANDLE_PREFIX = "artifact:noaa-err-quantitative:"

GLOBAL_ROWS = 6501
GLOBAL_COLUMNS = 18000
GRID_RESOLUTION_DEGREES = 0.02
GRID_NORTH = 70.0
GRID_SOUTH = -60.0
GRID_WEST = -180.0
GRID_EAST = 179.98
RRQPE_FILL = -9990
DQF_FILL = -1
DQF_BAD_QUALITY_MASK = 1
DQF_DEGRADED_MASK = 2
DQF_ALLOWED_VALUES = frozenset({-1, 0, 1, 2, 3})

MAX_REGION_ROWS = 1024
MAX_REGION_COLUMNS = 1024
MAX_REGION_PIXELS = 1_048_576
MAX_NETCDF_BYTES = native.MAX_NETCDF4_BYTES
MAX_ARTIFACT_BYTES = 32 * 1024 * 1024
MAX_ARTIFACT_TOTAL_BYTES = 512 * 1024 * 1024
MAX_ARTIFACT_OBJECTS = 65_536
MAX_STAGING_BYTES = 64 * 1024 * 1024
MAX_STAGING_OBJECTS = 64
MAX_HEADER_BYTES = 256 * 1024
MAX_UNCOMPRESSED_ARRAY_BYTES = 24 * 1024 * 1024
DECODE_TOTAL_DEADLINE_SECONDS = 60.0

_OBJECT_NAME_RE = re.compile(r"^[0-9a-f]{64}\.neqr$")
_STAGING_NAME_RE = re.compile(r"^\.noaa-err-[A-Za-z0-9_-]{1,128}\.tmp$")

EXPECTED_ROOT_OBJECTS = frozenset({
    "Columns",
    "DQF",
    "MonitoringMetaData",
    "RRQPE",
    "Rows",
    "quality_information",
})
EXPECTED_RRQPE_CHUNKS = (813, 2250)
EXPECTED_DQF_CHUNKS = (1084, 3000)
EXPECTED_RRQPE_DEFLATE_LEVEL = 6
EXPECTED_DQF_DEFLATE_LEVEL = 9

ARRAY_ORDER = (
    ("rrqpe_raw", "<i2"),
    ("precipitation_rate_mm_h", "<f8"),
    ("rate_valid_mask", "|u1"),
    ("rate_missing_mask", "|u1"),
    ("dqf_raw", "|i1"),
    ("dqf_missing_mask", "|u1"),
    ("dqf_bad_quality_mask", "|u1"),
    ("dqf_degraded_mask", "|u1"),
    ("quality_optimal_mask", "|u1"),
)

DECODER_CONFIG = {
    "artifact_schema": ARTIFACT_SCHEMA,
    "axis_order": "yx",
    "dqf_allowed_values": sorted(DQF_ALLOWED_VALUES),
    "dqf_bad_quality_mask": DQF_BAD_QUALITY_MASK,
    "dqf_degraded_mask": DQF_DEGRADED_MASK,
    "dqf_fill": DQF_FILL,
    "grid": {
        "columns": GLOBAL_COLUMNS,
        "east": GRID_EAST,
        "north": GRID_NORTH,
        "resolution_degrees": GRID_RESOLUTION_DEGREES,
        "row_direction": "north_to_south",
        "rows": GLOBAL_ROWS,
        "south": GRID_SOUTH,
        "west": GRID_WEST,
    },
    "max_region_columns": MAX_REGION_COLUMNS,
    "max_region_pixels": MAX_REGION_PIXELS,
    "max_region_rows": MAX_REGION_ROWS,
    "resource_contract": {
        "cooperative_deadline_seconds": DECODE_TOTAL_DEADLINE_SECONDS,
        "hard_c_or_rss_isolation": False,
        "max_artifact_bytes": MAX_ARTIFACT_BYTES,
        "max_artifact_objects": MAX_ARTIFACT_OBJECTS,
        "max_artifact_store_bytes": MAX_ARTIFACT_TOTAL_BYTES,
        "max_staging_bytes": MAX_STAGING_BYTES,
        "max_staging_objects": MAX_STAGING_OBJECTS,
        "max_uncompressed_array_bytes": MAX_UNCOMPRESSED_ARRAY_BYTES,
    },
    "source_profile": {
        "dimension_scale_binding": "netcdf4_rows_columns_exact_v1",
        "input_files": ["GOES-19", "GOES-18", "Himawari-9", "MSG2", "MSG3"],
        "missing_inputs": 0,
    },
    "quantity": "instantaneous_precipitation_rate",
    "rrqpe_fill": RRQPE_FILL,
    "rrqpe_gain": 0.1,
    "rrqpe_offset": 0.0,
    "unit": "mm/h",
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
    _canonical_json(DECODER_CONFIG)
).hexdigest()


class NoaaErrQuantitativeError(RuntimeError):
    """Base error for the isolated quantitative candidate."""


class NoaaErrCaptureMissingError(NoaaErrQuantitativeError):
    """The requested schema-3 capture does not exist."""


class NoaaErrArtifactError(NoaaErrQuantitativeError):
    """Immutable derived-artifact publication or verification failed."""


class NoaaErrArtifactCapacityError(NoaaErrArtifactError):
    """A whole artifact was refused by the fixed shared capacity bound."""


class NoaaErrDecodeDeadlineError(NoaaErrQuantitativeError):
    """The cooperative decoder deadline expired at a bounded checkpoint."""


class _RejectedDecode(Exception):
    def __init__(self, outcome: str, reason_code: str) -> None:
        super().__init__(reason_code)
        self.outcome = outcome
        self.reason_code = reason_code


@dataclass(frozen=True, slots=True)
class NoaaErrRegion:
    """One non-wrapping geographic request, expressed in WGS84 degrees."""

    minimum_latitude: float
    minimum_longitude: float
    maximum_latitude: float
    maximum_longitude: float


@dataclass(frozen=True, slots=True)
class NoaaErrWindow:
    row_start: int
    row_stop: int
    column_start: int
    column_stop: int
    requested: NoaaErrRegion
    minimum_latitude: float
    minimum_longitude: float
    maximum_latitude: float
    maximum_longitude: float
    grid_definition_sha256: str
    coverage_definition_sha256: str

    @property
    def rows(self) -> int:
        return self.row_stop - self.row_start

    @property
    def columns(self) -> int:
        return self.column_stop - self.column_start


@dataclass(frozen=True, slots=True)
class NoaaErrArtifactRef:
    handle: str
    sha256: str
    size_bytes: int


@dataclass(frozen=True, slots=True)
class NoaaErrRasterArtifact:
    metadata: Mapping[str, Any]
    rrqpe_raw: np.ndarray
    precipitation_rate_mm_h: np.ndarray
    rate_valid_mask: np.ndarray
    rate_missing_mask: np.ndarray
    dqf_raw: np.ndarray
    dqf_missing_mask: np.ndarray
    dqf_bad_quality_mask: np.ndarray
    dqf_degraded_mask: np.ndarray
    quality_optimal_mask: np.ndarray


@dataclass(frozen=True, slots=True)
class NoaaErrDecodeResult:
    outcome: str
    reason_code: str
    evidence_receipt: DecoderRunReceipt
    artifact: NoaaErrArtifactRef | None


@dataclass(frozen=True, slots=True)
class _DecodedRegion:
    arrays: Mapping[str, np.ndarray]
    window: NoaaErrWindow
    product_start: float
    product_end: float
    source_key: str
    source_etag: str
    source_sha256: str


_ROOT_LOCKS_GUARD = threading.Lock()
_ROOT_LOCKS: dict[str, threading.RLock] = {}


def _noop_checkpoint(_stage: str) -> None:
    return None


def _fsync_directory(path: Path) -> None:
    """Durably publish directory metadata on the Linux/HA target.

    Windows does not expose portable directory fsync through ``os.open``;
    that development-only fallback is deliberately not claimed as crash-proof.
    """

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        if os.name == "nt":
            return
        raise
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _thread_root_lock(root: Path) -> threading.RLock:
    key = os.path.normcase(str(root))
    with _ROOT_LOCKS_GUARD:
        return _ROOT_LOCKS.setdefault(key, threading.RLock())


@contextmanager
def _cross_process_lock(path: Path) -> Iterator[None]:
    """Serialize capacity check plus publish across threads and processes."""

    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or path.is_symlink():
            raise NoaaErrArtifactError("NOAA artifactlock is geen regulier bestand")
    with path.open("a+b") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise NoaaErrArtifactError("NOAA artifactlock is geen regulier bestand")
        stream.seek(0, os.SEEK_END)
        if stream.tell() == 0:
            stream.write(b"\0")
            stream.flush()
            os.fsync(stream.fileno())
        stream.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(stream.fileno(), msvcrt.LK_LOCK, 1)
            try:
                yield
            finally:
                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


class NoaaErrDerivedArtifactStore:
    """Bounded immutable object store for NOAA candidate artifacts."""

    def __init__(
        self,
        root: str | Path,
        *,
        max_total_bytes: int = MAX_ARTIFACT_TOTAL_BYTES,
        fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        supplied = Path(root)
        if not supplied.is_absolute():
            raise ValueError("NOAA derived-artifactroot moet absoluut zijn")
        if (
            isinstance(max_total_bytes, bool)
            or not isinstance(max_total_bytes, int)
            or max_total_bytes < MAX_ARTIFACT_BYTES
            or max_total_bytes > MAX_ARTIFACT_TOTAL_BYTES
        ):
            raise ValueError("NOAA artifactcapaciteit is te klein of ongeldig")
        self.root = supplied
        self.objects_path = self.root / "objects"
        self.staging_path = self.root / "staging"
        self.lock_path = self.root / ".publish.lock"
        self.max_total_bytes = max_total_bytes
        self._fault_injector = fault_injector
        self._lock = _thread_root_lock(self.root.resolve(strict=False))

    def initialize(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self.objects_path.mkdir(parents=True, exist_ok=True)
        self.staging_path.mkdir(parents=True, exist_ok=True)
        for path in (self.root, self.objects_path, self.staging_path):
            info = path.lstat()
            if not stat.S_ISDIR(info.st_mode) or path.is_symlink():
                raise NoaaErrArtifactError("NOAA artifactpad is geen echte map")
        _fsync_directory(self.root)

    def _fault(self, stage: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(stage)

    def _store_usage(
        self, *, checkpoint: Callable[[str], None] = _noop_checkpoint,
    ) -> tuple[int, int, int, int]:
        total = 0
        object_count = 0
        for path in self.objects_path.iterdir():
            checkpoint("artifact_object_inventory")
            info = path.lstat()
            if (
                not stat.S_ISREG(info.st_mode)
                or path.is_symlink()
                or _OBJECT_NAME_RE.fullmatch(path.name) is None
                or not 0 < info.st_size <= MAX_ARTIFACT_BYTES
            ):
                raise NoaaErrArtifactError("NOAA artifactstore bevat vreemd object")
            object_count += 1
            if object_count > MAX_ARTIFACT_OBJECTS:
                raise NoaaErrArtifactCapacityError(
                    "NOAA artifactstore overschrijdt zijn objectgrens"
                )
            total += info.st_size
            if total > self.max_total_bytes:
                raise NoaaErrArtifactCapacityError(
                    "NOAA artifactstore overschrijdt zijn vaste capaciteit"
                )
        staging_bytes = 0
        staging_count = 0
        for path in self.staging_path.iterdir():
            checkpoint("artifact_staging_inventory")
            info = path.lstat()
            if (
                not stat.S_ISREG(info.st_mode)
                or path.is_symlink()
                or _STAGING_NAME_RE.fullmatch(path.name) is None
                or not 0 < info.st_size <= MAX_ARTIFACT_BYTES
            ):
                raise NoaaErrArtifactError("NOAA staging bevat vreemd object")
            staging_count += 1
            staging_bytes += info.st_size
            if (
                staging_count > MAX_STAGING_OBJECTS
                or staging_bytes > MAX_STAGING_BYTES
            ):
                raise NoaaErrArtifactCapacityError(
                    "NOAA staging overschrijdt zijn vaste capaciteit"
                )
        return total, object_count, staging_bytes, staging_count

    def _verify_path(
        self, path: Path, *, expected_sha256: str, expected_size: int,
    ) -> bytes:
        try:
            info = path.lstat()
        except OSError as error:
            raise NoaaErrArtifactError("NOAA artifactobject ontbreekt") from error
        if (
            not stat.S_ISREG(info.st_mode)
            or path.is_symlink()
            or info.st_size != expected_size
            or not 0 < expected_size <= MAX_ARTIFACT_BYTES
        ):
            raise NoaaErrArtifactError("NOAA artifactobject heeft verkeerde vorm")
        try:
            payload = path.read_bytes()
        except OSError as error:
            raise NoaaErrArtifactError("NOAA artifactobject is onleesbaar") from error
        if len(payload) != expected_size or not hmac.compare_digest(
            hashlib.sha256(payload).hexdigest(), expected_sha256
        ):
            raise NoaaErrArtifactError("NOAA artifactobject faalt SHA-256")
        return payload

    def publish(
        self,
        payload: bytes,
        *,
        checkpoint: Callable[[str], None] = _noop_checkpoint,
    ) -> NoaaErrArtifactRef:
        if not isinstance(payload, bytes) or not 0 < len(payload) <= MAX_ARTIFACT_BYTES:
            raise NoaaErrArtifactError("NOAA artifactpayload valt buiten de grens")
        digest = hashlib.sha256(payload).hexdigest()
        ref = NoaaErrArtifactRef(
            handle=f"{ARTIFACT_HANDLE_PREFIX}{digest}",
            sha256=digest,
            size_bytes=len(payload),
        )
        self.initialize()
        target = self.objects_path / f"{digest}.neqr"
        temporary_path: Path | None = None
        with self._lock, _cross_process_lock(self.lock_path):
            checkpoint("artifact_publish_lock")
            current_bytes, object_count, staging_bytes, staging_count = (
                self._store_usage(checkpoint=checkpoint)
            )
            if target.exists():
                self._verify_path(
                    target, expected_sha256=digest, expected_size=len(payload)
                )
                return ref
            if (
                object_count + 1 > MAX_ARTIFACT_OBJECTS
                or current_bytes + len(payload) > self.max_total_bytes
                or staging_count + 1 > MAX_STAGING_OBJECTS
                or staging_bytes + len(payload) > MAX_STAGING_BYTES
            ):
                raise NoaaErrArtifactCapacityError(
                    "NOAA artifactpublicatie past niet binnen de capaciteit"
                )
            try:
                with tempfile.NamedTemporaryFile(
                    dir=self.staging_path,
                    prefix=".noaa-err-",
                    suffix=".tmp",
                    delete=False,
                ) as stream:
                    temporary_path = Path(stream.name)
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
                _fsync_directory(self.staging_path)
                self._fault("after_staging_fsync")
                try:
                    os.link(temporary_path, target)
                except FileExistsError:
                    pass
                _fsync_directory(self.objects_path)
                self._verify_path(
                    target, expected_sha256=digest, expected_size=len(payload)
                )
                self._fault("after_publish_verify")
            finally:
                if temporary_path is not None:
                    temporary_path.unlink(missing_ok=True)
                    _fsync_directory(self.staging_path)
        return ref

    def read_bytes(
        self,
        reference: NoaaErrArtifactRef,
        *,
        checkpoint: Callable[[str], None] = _noop_checkpoint,
    ) -> bytes:
        if not isinstance(reference, NoaaErrArtifactRef):
            raise TypeError("NOAA artifactreferentie heeft verkeerd type")
        expected_handle = f"{ARTIFACT_HANDLE_PREFIX}{reference.sha256}"
        if (
            not _is_sha256(reference.sha256)
            or isinstance(reference.size_bytes, bool)
            or not isinstance(reference.size_bytes, int)
            or not 0 < reference.size_bytes <= MAX_ARTIFACT_BYTES
            or reference.handle != expected_handle
        ):
            raise NoaaErrArtifactError("NOAA artifacthandle en SHA verschillen")
        checkpoint("artifact_read_before")
        self.initialize()
        path = self.objects_path / f"{reference.sha256}.neqr"
        payload = self._verify_path(
            path,
            expected_sha256=reference.sha256,
            expected_size=reference.size_bytes,
        )
        checkpoint("artifact_read_after")
        return payload

    def read_decoded(
        self,
        reference: NoaaErrArtifactRef,
        *,
        checkpoint: Callable[[str], None] = _noop_checkpoint,
    ) -> NoaaErrRasterArtifact:
        return _parse_artifact(
            self.read_bytes(reference, checkpoint=checkpoint),
            checkpoint=checkpoint,
        )


def _finite(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"NOAA region {name} moet een eindig getal zijn")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"NOAA region {name} moet een eindig getal zijn")
    return result


def _window_for_region(region: NoaaErrRegion) -> NoaaErrWindow:
    if not isinstance(region, NoaaErrRegion):
        raise TypeError("region moet NoaaErrRegion zijn")
    minimum_latitude = _finite(region.minimum_latitude, "minimum_latitude")
    minimum_longitude = _finite(region.minimum_longitude, "minimum_longitude")
    maximum_latitude = _finite(region.maximum_latitude, "maximum_latitude")
    maximum_longitude = _finite(region.maximum_longitude, "maximum_longitude")
    if (
        minimum_latitude < GRID_SOUTH
        or maximum_latitude > GRID_NORTH
        or minimum_longitude < GRID_WEST
        or maximum_longitude > GRID_EAST
        or minimum_latitude >= maximum_latitude
        or minimum_longitude >= maximum_longitude
    ):
        raise ValueError("NOAA region valt buiten het niet-wrappende GLB-raster")

    epsilon = 1.0e-9
    row_start = max(
        0,
        math.ceil(
            (GRID_NORTH - maximum_latitude) / GRID_RESOLUTION_DEGREES - epsilon
        ),
    )
    row_stop = min(
        GLOBAL_ROWS,
        math.floor(
            (GRID_NORTH - minimum_latitude) / GRID_RESOLUTION_DEGREES + epsilon
        )
        + 1,
    )
    column_start = max(
        0,
        math.ceil(
            (minimum_longitude - GRID_WEST) / GRID_RESOLUTION_DEGREES - epsilon
        ),
    )
    column_stop = min(
        GLOBAL_COLUMNS,
        math.floor(
            (maximum_longitude - GRID_WEST) / GRID_RESOLUTION_DEGREES + epsilon
        )
        + 1,
    )
    rows = row_stop - row_start
    columns = column_stop - column_start
    if (
        rows <= 0
        or columns <= 0
        or rows > MAX_REGION_ROWS
        or columns > MAX_REGION_COLUMNS
        or rows * columns > MAX_REGION_PIXELS
    ):
        raise ValueError("NOAA regionvenster valt buiten de vaste resourcegrens")

    north_center = GRID_NORTH - row_start * GRID_RESOLUTION_DEGREES
    south_center = GRID_NORTH - (row_stop - 1) * GRID_RESOLUTION_DEGREES
    west_center = GRID_WEST + column_start * GRID_RESOLUTION_DEGREES
    east_center = GRID_WEST + (column_stop - 1) * GRID_RESOLUTION_DEGREES
    half = GRID_RESOLUTION_DEGREES / 2.0
    actual_minimum_latitude = max(
        GRID_SOUTH - half, south_center - half
    )
    actual_maximum_latitude = min(
        GRID_NORTH + half, north_center + half
    )
    actual_minimum_longitude = max(
        GRID_WEST - half, west_center - half
    )
    actual_maximum_longitude = min(
        GRID_EAST + half, east_center + half
    )
    request_mapping = {
        "maximum_latitude": maximum_latitude,
        "maximum_longitude": maximum_longitude,
        "minimum_latitude": minimum_latitude,
        "minimum_longitude": minimum_longitude,
    }
    grid_mapping = {
        "axis_order": "yx",
        "column_center_formula": "-180.00+0.02*global_column",
        "column_start": column_start,
        "column_stop_exclusive": column_stop,
        "global_columns": GLOBAL_COLUMNS,
        "global_rows": GLOBAL_ROWS,
        "kind": "regular",
        "requested_region": request_mapping,
        "row_center_formula": "70.00-0.02*global_row",
        "row_start": row_start,
        "row_stop_exclusive": row_stop,
    }
    coverage_mapping = {
        "kind": "native_bbox",
        "maximum_x": actual_maximum_longitude,
        "maximum_y": actual_maximum_latitude,
        "minimum_x": actual_minimum_longitude,
        "minimum_y": actual_minimum_latitude,
    }
    return NoaaErrWindow(
        row_start=row_start,
        row_stop=row_stop,
        column_start=column_start,
        column_stop=column_stop,
        requested=NoaaErrRegion(
            minimum_latitude=minimum_latitude,
            minimum_longitude=minimum_longitude,
            maximum_latitude=maximum_latitude,
            maximum_longitude=maximum_longitude,
        ),
        minimum_latitude=actual_minimum_latitude,
        minimum_longitude=actual_minimum_longitude,
        maximum_latitude=actual_maximum_latitude,
        maximum_longitude=actual_maximum_longitude,
        grid_definition_sha256=hashlib.sha256(
            _canonical_json(grid_mapping)
        ).hexdigest(),
        coverage_definition_sha256=hashlib.sha256(
            _canonical_json(coverage_mapping)
        ).hexdigest(),
    )


def _scalar(attributes: Any, name: str) -> object:
    if name not in attributes:
        raise _RejectedDecode("unsupported", f"unsupported:attribute_{name}")
    value = attributes[name]
    array = np.asarray(value)
    if array.shape not in {(), (1,)}:
        raise _RejectedDecode("unsupported", f"unsupported:attribute_{name}")
    return array.reshape(-1)[0] if array.shape == (1,) else array.item()


def _text(attributes: Any, name: str, expected: str) -> None:
    value = _scalar(attributes, name)
    if isinstance(value, (bytes, np.bytes_)):
        try:
            value = bytes(value).decode("utf-8", errors="strict")
        except UnicodeDecodeError:
            raise _RejectedDecode(
                "unsupported", f"unsupported:attribute_{name}"
            ) from None
    if value != expected:
        raise _RejectedDecode("unsupported", f"unsupported:attribute_{name}")


def _number(attributes: Any, name: str, expected: float, tolerance: float = 0.0) -> None:
    value = _scalar(attributes, name)
    if isinstance(value, bool) or not isinstance(value, (int, float, np.number)):
        raise _RejectedDecode("unsupported", f"unsupported:attribute_{name}")
    candidate = float(value)
    if not math.isfinite(candidate) or not math.isclose(
        candidate, expected, rel_tol=0.0, abs_tol=tolerance
    ):
        raise _RejectedDecode("unsupported", f"unsupported:attribute_{name}")


def _hard_dataset(root: h5py.File, name: str) -> h5py.Dataset:
    link = root.get(name, getlink=True)
    value = root.get(name)
    if not isinstance(link, h5py.HardLink) or not isinstance(value, h5py.Dataset):
        raise _RejectedDecode("unsupported", "unsupported:hdf_object_tree")
    return value


def _dataset_token(dataset: h5py.Dataset) -> bytes:
    try:
        info = h5py.h5o.get_info(dataset.id)
        token = getattr(info, "token", None)
        if token is not None:
            return bytes(token)
        address = getattr(info, "addr", None)
        if isinstance(address, int) and address >= 0:
            return str(address).encode("ascii")
    except (OSError, RuntimeError, ValueError, TypeError):
        pass
    return repr(dataset.id).encode("utf-8")


def _validate_exact_deflate(
    dataset: h5py.Dataset,
    *,
    chunks: tuple[int, int],
    level: int,
    fill_value: int,
    window: NoaaErrWindow,
    checkpoint: Callable[[str], None] = _noop_checkpoint,
) -> None:
    if (
        dataset.chunks != chunks
        or dataset.compression != "gzip"
        or dataset.compression_opts != level
        or dataset.shuffle
        or dataset.fletcher32
        or dataset.scaleoffset is not None
        or int(dataset.fillvalue) != fill_value
    ):
        raise _RejectedDecode("unsupported", "unsupported:hdf_storage_profile")
    try:
        creation = dataset.id.get_create_plist()
        if creation.get_nfilters() != 1:
            raise _RejectedDecode(
                "unsupported", "unsupported:hdf_filter_pipeline"
            )
        filter_id, filter_flags, client_data, filter_name = creation.get_filter(0)
        if (
            filter_id != h5py.h5z.FILTER_DEFLATE
            or filter_flags != h5py.h5z.FLAG_OPTIONAL
            or tuple(client_data) != (level,)
            or bytes(filter_name) != b"deflate"
        ):
            raise _RejectedDecode(
                "unsupported", "unsupported:hdf_filter_pipeline"
            )
        chunk_row_start = (window.row_start // chunks[0]) * chunks[0]
        chunk_column_start = (window.column_start // chunks[1]) * chunks[1]
        for row in range(chunk_row_start, window.row_stop, chunks[0]):
            for column in range(
                chunk_column_start, window.column_stop, chunks[1]
            ):
                checkpoint("hdf_selected_chunk_metadata")
                info = dataset.id.get_chunk_info_by_coord((row, column))
                if (
                    info.byte_offset is None
                    or not isinstance(info.size, int)
                    or info.size <= 0
                    or info.filter_mask != 0
                ):
                    raise _RejectedDecode(
                        "incomplete", "incomplete:unallocated_or_unfiltered_chunk"
                    )
    except _RejectedDecode:
        raise
    except (OSError, RuntimeError, ValueError, TypeError, AttributeError):
        raise _RejectedDecode(
            "decode_error", "decode_error:hdf_chunk_metadata"
        ) from None


def _referenced_dataset(root: h5py.File, reference: object) -> h5py.Dataset:
    try:
        if not reference:
            raise ValueError("lege HDF5-objectreferentie")
        value = root[reference]
    except (KeyError, OSError, RuntimeError, TypeError, ValueError):
        raise _RejectedDecode(
            "unsupported", "unsupported:dimension_reference"
        ) from None
    if not isinstance(value, h5py.Dataset):
        raise _RejectedDecode("unsupported", "unsupported:dimension_reference")
    return value


def _dimension_name(expected_length: int) -> str:
    return (
        "This is a netCDF dimension but not a netCDF variable. "
        f"{expected_length:9d}"
    )


def _validate_dimension_scale(
    root: h5py.File,
    dataset: h5py.Dataset,
    *,
    expected_length: int,
    expected_dimid: int,
    expected_dimension: int,
    rrqpe: h5py.Dataset,
    dqf: h5py.Dataset,
    checkpoint: Callable[[str], None] = _noop_checkpoint,
) -> None:
    if (
        dataset.shape != (expected_length,)
        or dataset.dtype != np.dtype(">f4")
        or dataset.chunks is not None
        or dataset.compression is not None
    ):
        raise _RejectedDecode("unsupported", "unsupported:dimension_scale")
    try:
        class_value = np.asarray(dataset.attrs["CLASS"])
        name_value = np.asarray(dataset.attrs["NAME"])
        dimid_value = np.asarray(dataset.attrs["_Netcdf4Dimid"])
        references = np.asarray(dataset.attrs["REFERENCE_LIST"])
    except (KeyError, OSError, RuntimeError, TypeError, ValueError):
        raise _RejectedDecode("unsupported", "unsupported:dimension_scale") from None
    reference_fields = references.dtype.fields or {}
    referenced_object_field = reference_fields.get("dataset", (None, None))
    referenced_dimension_field = reference_fields.get("dimension", (None, None))
    referenced_object_dtype, referenced_object_offset = referenced_object_field[:2]
    referenced_dimension_dtype, referenced_dimension_offset = (
        referenced_dimension_field[:2]
    )
    if (
        class_value.shape != ()
        or class_value.dtype != np.dtype("S15")
        or class_value.item() != b"DIMENSION_SCALE"
        or name_value.shape != ()
        or name_value.dtype != np.dtype("S63")
        or name_value.item() != _dimension_name(expected_length).encode("ascii")
        or dimid_value.shape != ()
        or dimid_value.dtype != np.dtype("<i4")
        or int(dimid_value.item()) != expected_dimid
        or references.shape != (2,)
        or references.dtype.names != ("dataset", "dimension")
        or references.dtype.itemsize != 16
        or referenced_object_dtype is None
        or h5py.check_dtype(ref=referenced_object_dtype) is not h5py.Reference
        or referenced_object_offset != 0
        or referenced_dimension_dtype is None
        or referenced_dimension_dtype != np.dtype("<i4")
        or referenced_dimension_offset != 8
    ):
        raise _RejectedDecode("unsupported", "unsupported:dimension_scale_binding")
    expected_tokens = {_dataset_token(rrqpe), _dataset_token(dqf)}
    observed_tokens: set[bytes] = set()
    for item in references:
        checkpoint("hdf_dimension_reference")
        if int(item["dimension"]) != expected_dimension:
            raise _RejectedDecode(
                "unsupported", "unsupported:dimension_scale_binding"
            )
        observed_tokens.add(_dataset_token(_referenced_dataset(root, item["dataset"])))
    if observed_tokens != expected_tokens:
        raise _RejectedDecode("unsupported", "unsupported:dimension_scale_binding")
    try:
        checkpoint("hdf_dimension_scale_read")
        values = np.asarray(dataset[...], dtype=np.dtype("<f4"))
    except (OSError, RuntimeError, ValueError, TypeError):
        raise _RejectedDecode("decode_error", "decode_error:dimension_scale") from None
    if values.shape != (expected_length,) or not np.all(values == 0.0):
        raise _RejectedDecode("unsupported", "unsupported:dimension_scale_values")


def _validate_dataset_axes(
    root: h5py.File,
    dataset: h5py.Dataset,
    *,
    rows: h5py.Dataset,
    columns: h5py.Dataset,
    checkpoint: Callable[[str], None] = _noop_checkpoint,
) -> None:
    try:
        coordinates = np.asarray(dataset.attrs["_Netcdf4Coordinates"])
        dimensions = np.asarray(dataset.attrs["DIMENSION_LIST"])
    except (KeyError, OSError, RuntimeError, TypeError, ValueError):
        raise _RejectedDecode("unsupported", "unsupported:dataset_axes") from None
    dimension_reference_dtype = h5py.check_dtype(vlen=dimensions.dtype)
    if (
        coordinates.shape != (2,)
        or coordinates.dtype != np.dtype("<i4")
        or coordinates.tolist() != [1, 0]
        or dimensions.shape != (2,)
        or dimension_reference_dtype is None
        or h5py.check_dtype(ref=dimension_reference_dtype) is not h5py.Reference
    ):
        raise _RejectedDecode("unsupported", "unsupported:dataset_axes")
    expected = (rows, columns)
    for index, expected_dataset in enumerate(expected):
        checkpoint("hdf_dataset_axis_reference")
        refs = np.asarray(dimensions[index], dtype=object)
        if refs.shape != (1,):
            raise _RejectedDecode("unsupported", "unsupported:dataset_axes")
        observed = _referenced_dataset(root, refs[0])
        if _dataset_token(observed) != _dataset_token(expected_dataset):
            raise _RejectedDecode("unsupported", "unsupported:dataset_axes")


def _decode_hdf_region(
    payload: bytes,
    *,
    selected: native.NoaaErrObject,
    window: NoaaErrWindow,
    source_sha256: str,
    checkpoint: Callable[[str], None] = _noop_checkpoint,
) -> _DecodedRegion:
    checkpoint("native_profile_before")
    try:
        identity = native._inspect_netcdf4_identity(payload, selected=selected)
    except native.NoaaErrNativeUnsupportedError:
        raise _RejectedDecode("unsupported", "unsupported:native_profile") from None
    except native.NoaaErrNativeCorruptionError:
        raise _RejectedDecode("decode_error", "decode_error:native_profile") from None
    except (OSError, RuntimeError, ValueError, TypeError, KeyError):
        raise _RejectedDecode("decode_error", "decode_error:native_inspector") from None
    checkpoint("native_profile_after")
    try:
        with h5py.File(io.BytesIO(payload), "r") as root:
            checkpoint("hdf_open")
            if frozenset(root.keys()) != EXPECTED_ROOT_OBJECTS:
                raise _RejectedDecode("unsupported", "unsupported:hdf_object_tree")
            rows = _hard_dataset(root, "Rows")
            columns = _hard_dataset(root, "Columns")
            rrqpe = _hard_dataset(root, "RRQPE")
            dqf = _hard_dataset(root, "DQF")
            monitoring = _hard_dataset(root, "MonitoringMetaData")
            quality_information = _hard_dataset(root, "quality_information")
            tokens = {
                _dataset_token(dataset)
                for dataset in (
                    rows,
                    columns,
                    rrqpe,
                    dqf,
                    monitoring,
                    quality_information,
                )
            }
            if len(tokens) != 6:
                raise _RejectedDecode("unsupported", "unsupported:hdf_object_alias")

            _validate_dimension_scale(
                root,
                rows,
                expected_length=GLOBAL_ROWS,
                expected_dimid=1,
                expected_dimension=0,
                rrqpe=rrqpe,
                dqf=dqf,
                checkpoint=checkpoint,
            )
            _validate_dimension_scale(
                root,
                columns,
                expected_length=GLOBAL_COLUMNS,
                expected_dimid=0,
                expected_dimension=1,
                rrqpe=rrqpe,
                dqf=dqf,
                checkpoint=checkpoint,
            )
            _validate_dataset_axes(
                root, rrqpe, rows=rows, columns=columns, checkpoint=checkpoint
            )
            _validate_dataset_axes(
                root, dqf, rows=rows, columns=columns, checkpoint=checkpoint
            )
            if rrqpe.shape != (GLOBAL_ROWS, GLOBAL_COLUMNS) or rrqpe.dtype != np.dtype(">i2"):
                # The live NetCDF stores signed native int16; h5py reports
                # ``int16`` on little-endian hosts and ``>i2`` on the golden.
                if rrqpe.dtype.kind != "i" or rrqpe.dtype.itemsize != 2:
                    raise _RejectedDecode("unsupported", "unsupported:rrqpe_dtype")
            if dqf.shape != (GLOBAL_ROWS, GLOBAL_COLUMNS) or dqf.dtype != np.dtype("i1"):
                raise _RejectedDecode("unsupported", "unsupported:dqf_dtype")
            _validate_exact_deflate(
                rrqpe,
                chunks=EXPECTED_RRQPE_CHUNKS,
                level=EXPECTED_RRQPE_DEFLATE_LEVEL,
                fill_value=RRQPE_FILL,
                window=window,
                checkpoint=checkpoint,
            )
            _validate_exact_deflate(
                dqf,
                chunks=EXPECTED_DQF_CHUNKS,
                level=EXPECTED_DQF_DEFLATE_LEVEL,
                fill_value=DQF_FILL,
                window=window,
                checkpoint=checkpoint,
            )
            _number(rrqpe.attrs, "_FillValue", float(RRQPE_FILL))
            _number(rrqpe.attrs, "scale_factor", 0.1, tolerance=1.0e-6)
            _number(rrqpe.attrs, "add_offset", 0.0)
            _text(rrqpe.attrs, "units", "mm/h")
            _number(dqf.attrs, "_FillValue", float(DQF_FILL))
            valid_range = np.asarray(dqf.attrs.get("valid_range"))
            masks = np.asarray(dqf.attrs.get("flag_masks"))
            meanings = dqf.attrs.get("flag_meanings")
            if isinstance(meanings, (bytes, np.bytes_)):
                meanings = bytes(meanings).decode("utf-8", errors="strict")
            if (
                valid_range.shape != (2,)
                or valid_range.dtype.kind != "i"
                or valid_range.tolist() != [0, 3]
                or masks.shape != (2,)
                or masks.dtype.kind != "i"
                or masks.tolist() != [1, 2]
                or meanings
                != "bad_quality degraded_due_to_LZA_or_latitude_threshold_exceeded"
            ):
                raise _RejectedDecode("unsupported", "unsupported:dqf_semantics")
            _number(monitoring.attrs, "Number_of_Input_Files", 5.0)
            _number(monitoring.attrs, "Number_of_Missing_Inputs", 0.0)
            try:
                native._validate_complete_input_files(monitoring.attrs)
            except (TypeError, ValueError, KeyError, UnicodeError):
                raise _RejectedDecode(
                    "unsupported", "unsupported:input_file_binding"
                ) from None

            selection = (
                slice(window.row_start, window.row_stop),
                slice(window.column_start, window.column_stop),
            )
            try:
                checkpoint("hdf_regional_raster_before")
                raw_rate = np.asarray(rrqpe[selection], dtype=np.dtype("<i2"), order="C")
                raw_dqf = np.asarray(dqf[selection], dtype=np.dtype("|i1"), order="C")
                checkpoint("hdf_regional_raster_after")
            except (OSError, RuntimeError, ValueError, TypeError):
                raise _RejectedDecode(
                    "decode_error", "decode_error:regional_raster_read"
                ) from None
    except _RejectedDecode:
        raise
    except (OSError, RuntimeError, ValueError, TypeError, KeyError, UnicodeError):
        raise _RejectedDecode("decode_error", "decode_error:hdf_container") from None

    expected_shape = (window.rows, window.columns)
    if raw_rate.shape != expected_shape or raw_dqf.shape != expected_shape:
        raise _RejectedDecode("decode_error", "decode_error:regional_shape")
    if np.any((raw_rate < 0) & (raw_rate != RRQPE_FILL)):
        raise _RejectedDecode("unsupported", "unsupported:negative_rrqpe_code")
    unique_dqf = {int(value) for value in np.unique(raw_dqf)}
    if not unique_dqf.issubset(DQF_ALLOWED_VALUES):
        raise _RejectedDecode("unsupported", "unsupported:unknown_dqf_bits")
    rate_missing = np.ascontiguousarray(raw_rate == RRQPE_FILL, dtype=np.uint8)
    rate_valid = np.ascontiguousarray(rate_missing == 0, dtype=np.uint8)
    dqf_missing = np.ascontiguousarray(raw_dqf == DQF_FILL, dtype=np.uint8)
    if not np.array_equal(rate_missing, dqf_missing):
        raise _RejectedDecode("unsupported", "unsupported:inconsistent_missingness")
    dqf_bad = np.ascontiguousarray(
        (raw_dqf != DQF_FILL) & ((raw_dqf & DQF_BAD_QUALITY_MASK) != 0),
        dtype=np.uint8,
    )
    dqf_degraded = np.ascontiguousarray(
        (raw_dqf != DQF_FILL) & ((raw_dqf & DQF_DEGRADED_MASK) != 0),
        dtype=np.uint8,
    )
    quality_optimal = np.ascontiguousarray(raw_dqf == 0, dtype=np.uint8)
    rate = np.zeros(expected_shape, dtype=np.dtype("<f8"))
    valid = rate_valid == 1
    rate[valid] = raw_rate[valid].astype(np.float64) * 0.1
    if not np.isfinite(rate[valid]).all() or np.any(rate[valid] < 0.0):
        raise _RejectedDecode("decode_error", "decode_error:physical_rate")
    arrays = {
        "rrqpe_raw": np.ascontiguousarray(raw_rate, dtype=np.dtype("<i2")),
        "precipitation_rate_mm_h": np.ascontiguousarray(rate, dtype=np.dtype("<f8")),
        "rate_valid_mask": rate_valid,
        "rate_missing_mask": rate_missing,
        "dqf_raw": np.ascontiguousarray(raw_dqf, dtype=np.dtype("|i1")),
        "dqf_missing_mask": dqf_missing,
        "dqf_bad_quality_mask": dqf_bad,
        "dqf_degraded_mask": dqf_degraded,
        "quality_optimal_mask": quality_optimal,
    }
    return _DecodedRegion(
        arrays=arrays,
        window=window,
        product_start=identity.product_start,
        product_end=identity.product_end,
        source_key=selected.key,
        source_etag=selected.etag,
        source_sha256=source_sha256,
    )


def _artifact_capture_metadata(
    capture: RawArtifactCaptureReceipt,
) -> dict[str, Any]:
    return {
        "capture_id": capture.capture_id,
        "capture_receipt_sha256": capture.capture_receipt_sha256,
        "fetch_id": capture.fetch_id,
        "manifest_sha256": capture.manifest_sha256,
        "members": _capture_members(capture),
        "product": capture.product,
        "product_timestamp": capture.product_timestamp,
        "provider": capture.provider,
        "upstream_capture_id": capture.upstream_capture_id,
    }


def _artifact_source_metadata(
    *,
    listing_receipt: RawArtifactReceipt,
    netcdf_receipt: RawArtifactReceipt,
    frame: _DecodedRegion,
) -> dict[str, Any]:
    if not hmac.compare_digest(
        frame.source_sha256, netcdf_receipt.content_sha256
    ):
        raise NoaaErrArtifactError("NOAA decoded frame en raw bron verschillen")
    return {
        "etag": frame.source_etag,
        "listing_content_sha256": listing_receipt.content_sha256,
        "netcdf_content_sha256": netcdf_receipt.content_sha256,
        "s3_key": frame.source_key,
    }


def _artifact_product_metadata(frame: _DecodedRegion) -> dict[str, Any]:
    return {
        "instantaneous_not_accumulation": True,
        "product_end": frame.product_end,
        "product_start": frame.product_start,
        "quantity": "instantaneous_precipitation_rate",
        "semantic_class": native.NOAA_ERR_SEMANTIC_CLASS,
        "unit": "mm/h",
    }


def _artifact_bytes(
    capture: RawArtifactCaptureReceipt,
    *,
    listing_receipt: RawArtifactReceipt,
    netcdf_receipt: RawArtifactReceipt,
    frame: _DecodedRegion,
    checkpoint: Callable[[str], None] = _noop_checkpoint,
) -> bytes:
    sections: list[dict[str, Any]] = []
    chunks: list[bytes] = []
    offset = 0
    shape = (frame.window.rows, frame.window.columns)
    for name, dtype_text in ARRAY_ORDER:
        checkpoint("artifact_array_serialize")
        array = frame.arrays.get(name)
        dtype = np.dtype(dtype_text)
        if not isinstance(array, np.ndarray) or array.shape != shape:
            raise NoaaErrArtifactError("NOAA artifact mist een rastersectie")
        normalized = np.ascontiguousarray(array, dtype=dtype)
        chunk = normalized.tobytes(order="C")
        sections.append({
            "dtype": dtype.str,
            "length": len(chunk),
            "name": name,
            "offset": offset,
            "sha256": hashlib.sha256(chunk).hexdigest(),
            "shape": list(shape),
        })
        chunks.append(chunk)
        offset += len(chunk)
    uncompressed = b"".join(chunks)
    if not 0 < len(uncompressed) <= MAX_UNCOMPRESSED_ARRAY_BYTES:
        raise NoaaErrArtifactError("NOAA arrays overschrijden de artifactgrens")
    checkpoint("artifact_compress_before")
    compressed = zlib.compress(uncompressed, level=6)
    checkpoint("artifact_compress_after")
    requested = frame.window.requested
    metadata = {
        "arrays": sections,
        "capture": _artifact_capture_metadata(capture),
        "compression": {
            "codec": "zlib",
            "compressed_length": len(compressed),
            "compressed_sha256": hashlib.sha256(compressed).hexdigest(),
            "level": 6,
            "uncompressed_length": len(uncompressed),
            "uncompressed_sha256": hashlib.sha256(uncompressed).hexdigest(),
        },
        "decoder": {
            "config_sha256": DECODER_CONFIG_SHA256,
            "identity": DECODER_IDENTITY,
            "version": DECODER_VERSION,
        },
        "dqf": {
            "allowed_values": sorted(DQF_ALLOWED_VALUES),
            "bad_quality_bit": DQF_BAD_QUALITY_MASK,
            "degraded_bit": DQF_DEGRADED_MASK,
            "fill_value": DQF_FILL,
            "flag_meanings": [
                "bad_quality",
                "degraded_due_to_LZA_or_latitude_threshold_exceeded",
            ],
            "quality_score_invented": False,
        },
        "grid": {
            "axis_order": "yx",
            "columns": frame.window.columns,
            "coverage_definition_sha256": (
                frame.window.coverage_definition_sha256
            ),
            "global_column_start": frame.window.column_start,
            "global_column_stop_exclusive": frame.window.column_stop,
            "global_row_start": frame.window.row_start,
            "global_row_stop_exclusive": frame.window.row_stop,
            "grid_definition_sha256": frame.window.grid_definition_sha256,
            "maximum_latitude": frame.window.maximum_latitude,
            "maximum_longitude": frame.window.maximum_longitude,
            "minimum_latitude": frame.window.minimum_latitude,
            "minimum_longitude": frame.window.minimum_longitude,
            "native_crs": "EPSG:4326",
            "resolution_degrees": GRID_RESOLUTION_DEGREES,
            "row_direction": "north_to_south",
            "rows": frame.window.rows,
        },
        "product": _artifact_product_metadata(frame),
        "region_request": {
            "maximum_latitude": requested.maximum_latitude,
            "maximum_longitude": requested.maximum_longitude,
            "minimum_latitude": requested.minimum_latitude,
            "minimum_longitude": requested.minimum_longitude,
        },
        "rrqpe": {
            "fill_value": RRQPE_FILL,
            "gain": 0.1,
            "offset": 0.0,
        },
        "schema": ARTIFACT_SCHEMA,
        "source": _artifact_source_metadata(
            listing_receipt=listing_receipt,
            netcdf_receipt=netcdf_receipt,
            frame=frame,
        ),
    }
    checkpoint("artifact_header_before")
    header = _canonical_json(metadata)
    checkpoint("artifact_header_after")
    if len(header) > MAX_HEADER_BYTES:
        raise NoaaErrArtifactError("NOAA artifactheader overschrijdt de grens")
    result = ARTIFACT_MAGIC + struct.pack(">I", len(header)) + header + compressed
    checkpoint("artifact_finalize")
    if len(result) > MAX_ARTIFACT_BYTES:
        raise NoaaErrArtifactError("NOAA artifact overschrijdt de vaste grens")
    return result


def _bounded_decompress(
    payload: bytes,
    expected_bytes: int,
    *,
    checkpoint: Callable[[str], None] = _noop_checkpoint,
) -> bytes:
    if (
        not isinstance(expected_bytes, int)
        or expected_bytes <= 0
        or expected_bytes > MAX_UNCOMPRESSED_ARRAY_BYTES
    ):
        raise NoaaErrArtifactError("NOAA artifact heeft ongeldige decoded grootte")
    try:
        checkpoint("artifact_decompress_before")
        decoder = zlib.decompressobj()
        output = decoder.decompress(payload, expected_bytes + 1)
        if len(output) > expected_bytes or decoder.unconsumed_tail:
            raise NoaaErrArtifactError("NOAA artifact expandeert boven de grens")
        output += decoder.flush(expected_bytes + 1 - len(output))
        checkpoint("artifact_decompress_after")
    except zlib.error:
        raise NoaaErrArtifactError("NOAA artifactcompressie is beschadigd") from None
    if (
        len(output) != expected_bytes
        or decoder.unused_data
        or not decoder.eof
    ):
        raise NoaaErrArtifactError("NOAA artifactcompressie is niet exact")
    return output


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _artifact_timestamp(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise NoaaErrArtifactError("NOAA artifacttijd is ongeldig")
    result = float(value)
    if not math.isfinite(result) or result < 0.0:
        raise NoaaErrArtifactError("NOAA artifacttijd is ongeldig")
    return result


def _validate_artifact_provenance(
    *,
    capture: object,
    source: object,
    product: object,
) -> None:
    capture_fields = frozenset({
        "capture_id",
        "capture_receipt_sha256",
        "fetch_id",
        "manifest_sha256",
        "members",
        "product",
        "product_timestamp",
        "provider",
        "upstream_capture_id",
    })
    source_fields = frozenset({
        "etag",
        "listing_content_sha256",
        "netcdf_content_sha256",
        "s3_key",
    })
    product_fields = frozenset({
        "instantaneous_not_accumulation",
        "product_end",
        "product_start",
        "quantity",
        "semantic_class",
        "unit",
    })
    if (
        not isinstance(capture, dict)
        or frozenset(capture) != capture_fields
        or capture.get("provider") != "noaa_enterprise_rainrate"
        or capture.get("product") != native.NOAA_ERR_CAPTURE_PRODUCT
        or not isinstance(source, dict)
        or frozenset(source) != source_fields
        or not isinstance(product, dict)
        or frozenset(product) != product_fields
        or product.get("instantaneous_not_accumulation") is not True
        or product.get("quantity") != "instantaneous_precipitation_rate"
        or product.get("semantic_class") != native.NOAA_ERR_SEMANTIC_CLASS
        or product.get("unit") != "mm/h"
    ):
        raise NoaaErrArtifactError("NOAA artifactprovenancecontract wijkt af")
    try:
        fetch_id = RawArtifactStore._identifier(
            capture.get("fetch_id"), "fetch_id", 512
        )
        upstream_capture_id = RawArtifactStore._identifier(
            capture.get("upstream_capture_id"), "upstream_capture_id", 1024
        )
    except (TypeError, ValueError):
        raise NoaaErrArtifactError("NOAA artifactcapturebinding wijkt af") from None
    capture_id = capture.get("capture_id")
    if (
        capture_id
        != RawArtifactStore._capture_id(
            provider="noaa_enterprise_rainrate",
            product=native.NOAA_ERR_CAPTURE_PRODUCT,
            fetch_id=fetch_id,
        )
        or not _is_sha256(capture.get("manifest_sha256"))
        or not _is_sha256(capture.get("capture_receipt_sha256"))
    ):
        raise NoaaErrArtifactError("NOAA artifactcapturebinding wijkt af")

    members_value = capture.get("members")
    if not isinstance(members_value, list) or len(members_value) != 2:
        raise NoaaErrArtifactError("NOAA artifactcaptureleden wijken af")
    members: list[RawArtifactCaptureMember] = []
    for expected_role, value in zip(("listing_xml", "netcdf4"), members_value):
        if (
            not isinstance(value, dict)
            or frozenset(value)
            != frozenset({
                "content_sha256",
                "member_id",
                "receipt_id",
                "receipt_sha256",
                "received_at",
                "role",
            })
            or value.get("role") != expected_role
            or value.get("member_id") != expected_role
            or not isinstance(value.get("receipt_id"), str)
            or len(value["receipt_id"]) != 68
            or not value["receipt_id"].startswith("raw-")
            or not _is_sha256(value["receipt_id"][4:])
            or not _is_sha256(value.get("content_sha256"))
            or not _is_sha256(value.get("receipt_sha256"))
        ):
            raise NoaaErrArtifactError("NOAA artifactcaptureleden wijken af")
        members.append(RawArtifactCaptureMember(
            role=expected_role,
            receipt_id=value["receipt_id"],
            content_sha256=value["content_sha256"],
            receipt_sha256=value["receipt_sha256"],
            received_at=_artifact_timestamp(value.get("received_at")),
        ))
    member_tuple = tuple(members)
    product_timestamp = _artifact_timestamp(capture.get("product_timestamp"))
    manifest = {
        "fetch_id": fetch_id,
        "members": [
            {
                "content_sha256": member.content_sha256,
                "received_at": member.received_at,
                "receipt_id": member.receipt_id,
                "role": member.role,
            }
            for member in member_tuple
        ],
        "product": native.NOAA_ERR_CAPTURE_PRODUCT,
        "product_timestamp": product_timestamp,
        "provider": "noaa_enterprise_rainrate",
        "schema": RAW_ARTIFACT_CAPTURE_SCHEMA,
        "upstream_capture_id": upstream_capture_id,
    }
    expected_manifest_sha256 = hashlib.sha256(
        _canonical_json(manifest)
    ).hexdigest()
    if not hmac.compare_digest(
        capture["manifest_sha256"], expected_manifest_sha256
    ):
        raise NoaaErrArtifactError("NOAA artifactmanifestbinding wijkt af")
    expected_capture_receipt_sha256 = RawArtifactStore._capture_receipt_sha256(
        capture_id=capture_id,
        provider="noaa_enterprise_rainrate",
        product=native.NOAA_ERR_CAPTURE_PRODUCT,
        fetch_id=fetch_id,
        upstream_capture_id=upstream_capture_id,
        product_timestamp=product_timestamp,
        manifest_sha256=expected_manifest_sha256,
        members=member_tuple,
    )
    if not hmac.compare_digest(
        capture["capture_receipt_sha256"], expected_capture_receipt_sha256
    ):
        raise NoaaErrArtifactError("NOAA artifactcapture-receiptbinding wijkt af")

    if (
        not _is_sha256(source.get("listing_content_sha256"))
        or not _is_sha256(source.get("netcdf_content_sha256"))
        or source["listing_content_sha256"] != members[0].content_sha256
        or source["netcdf_content_sha256"] != members[1].content_sha256
        or not isinstance(source.get("etag"), str)
        or native._ETAG_RE.fullmatch(source["etag"]) is None
    ):
        raise NoaaErrArtifactError("NOAA artifactbronbinding wijkt af")
    try:
        key, match = native._safe_object_key(
            source.get("s3_key"), prefix=f"{native.NOAA_ERR_PRODUCT_PREFIX}/"
        )
        filename_start = native._filename_timestamp(
            match.group("start"), context="NOAA artifact start"
        )
        filename_end = native._filename_timestamp(
            match.group("end"), context="NOAA artifact einde"
        )
    except (TypeError, ValueError):
        raise NoaaErrArtifactError("NOAA artifactbronbinding wijkt af") from None
    if (
        match.group("inputs") != "5"
        or match.group("major") != "1"
        or match.group("revision") != "1"
        or match.group("start")[:10]
        != "".join(match.group(name) for name in ("year", "month", "day", "hour"))
        or not 599.0 <= filename_end - filename_start <= 600.0
    ):
        raise NoaaErrArtifactError("NOAA artifactsleutelbinding wijkt af")
    product_start = _artifact_timestamp(product.get("product_start"))
    product_end = _artifact_timestamp(product.get("product_end"))
    if (
        product_start != filename_start
        or product_start != product_timestamp
        or not 530.0 <= product_end - product_start <= 600.0
        or not 0.0 <= filename_end - product_end <= 61.0
    ):
        raise NoaaErrArtifactError("NOAA artifactproducttijd wijkt af")
    expected_upstream_capture_id = (
        f"{native.NOAA_ERR_CAPTURE_PRODUCT}|key={key}|etag={source['etag']}"
        f"|listing={source['listing_content_sha256']}"
        f"|netcdf4={source['netcdf_content_sha256']}"
    )
    if upstream_capture_id != expected_upstream_capture_id:
        raise NoaaErrArtifactError("NOAA artifactupstreambinding wijkt af")


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"niet-standaard JSON-constante {value}")


def _parse_artifact(
    payload: bytes,
    *,
    checkpoint: Callable[[str], None] = _noop_checkpoint,
) -> NoaaErrRasterArtifact:
    if not isinstance(payload, bytes) or not 0 < len(payload) <= MAX_ARTIFACT_BYTES:
        raise NoaaErrArtifactError("NOAA artifactbytes vallen buiten de grens")
    prefix_size = len(ARTIFACT_MAGIC) + 4
    if len(payload) < prefix_size or not payload.startswith(ARTIFACT_MAGIC):
        raise NoaaErrArtifactError("NOAA artifactmagic ontbreekt")
    header_size = struct.unpack(
        ">I", payload[len(ARTIFACT_MAGIC):prefix_size]
    )[0]
    if header_size <= 0 or header_size > MAX_HEADER_BYTES:
        raise NoaaErrArtifactError("NOAA artifactheaderlengte is ongeldig")
    header_end = prefix_size + header_size
    if header_end >= len(payload):
        raise NoaaErrArtifactError("NOAA artifactheader is afgekapt")
    header = payload[prefix_size:header_end]
    try:
        checkpoint("artifact_json_before")
        metadata = json.loads(
            header.decode("utf-8", errors="strict"),
            parse_constant=_reject_json_constant,
        )
        canonical_header = _canonical_json(metadata)
        checkpoint("artifact_json_after")
    except (
        UnicodeError,
        json.JSONDecodeError,
        ValueError,
        OverflowError,
        RecursionError,
        TypeError,
    ):
        raise NoaaErrArtifactError("NOAA artifactheader is geen JSON") from None
    if (
        not isinstance(metadata, dict)
        or metadata.get("schema") != ARTIFACT_SCHEMA
        or canonical_header != header
    ):
        raise NoaaErrArtifactError("NOAA artifactheader is niet canoniek")
    if frozenset(metadata) != frozenset({
        "arrays",
        "capture",
        "compression",
        "decoder",
        "dqf",
        "grid",
        "product",
        "region_request",
        "rrqpe",
        "schema",
        "source",
    }):
        raise NoaaErrArtifactError("NOAA artifactheader heeft onbekende velden")
    decoder = metadata.get("decoder")
    grid = metadata.get("grid")
    compression = metadata.get("compression")
    sections = metadata.get("arrays")
    capture = metadata.get("capture")
    product = metadata.get("product")
    dqf = metadata.get("dqf")
    rrqpe = metadata.get("rrqpe")
    source = metadata.get("source")
    region_request = metadata.get("region_request")
    if (
        not isinstance(decoder, dict)
        or decoder
        != {
            "config_sha256": DECODER_CONFIG_SHA256,
            "identity": DECODER_IDENTITY,
            "version": DECODER_VERSION,
        }
        or not isinstance(grid, dict)
        or not isinstance(compression, dict)
        or not isinstance(sections, list)
        or len(sections) != len(ARRAY_ORDER)
    ):
        raise NoaaErrArtifactError("NOAA artifactcontract wijkt af")
    _validate_artifact_provenance(
        capture=capture,
        source=source,
        product=product,
    )
    if dqf != {
        "allowed_values": [-1, 0, 1, 2, 3],
        "bad_quality_bit": 1,
        "degraded_bit": 2,
        "fill_value": -1,
        "flag_meanings": [
            "bad_quality",
            "degraded_due_to_LZA_or_latitude_threshold_exceeded",
        ],
        "quality_score_invented": False,
    } or rrqpe != {"fill_value": -9990, "gain": 0.1, "offset": 0.0}:
        raise NoaaErrArtifactError("NOAA artifactwaardesemantiek wijkt af")
    if not isinstance(region_request, dict) or frozenset(region_request) != frozenset({
        "maximum_latitude",
        "maximum_longitude",
        "minimum_latitude",
        "minimum_longitude",
    }):
        raise NoaaErrArtifactError("NOAA artifactregio wijkt af")
    try:
        expected_window = _window_for_region(NoaaErrRegion(
            minimum_latitude=region_request["minimum_latitude"],
            minimum_longitude=region_request["minimum_longitude"],
            maximum_latitude=region_request["maximum_latitude"],
            maximum_longitude=region_request["maximum_longitude"],
        ))
    except (TypeError, ValueError):
        raise NoaaErrArtifactError("NOAA artifactregio wijkt af") from None
    rows = grid.get("rows")
    columns = grid.get("columns")
    if (
        isinstance(rows, bool)
        or not isinstance(rows, int)
        or isinstance(columns, bool)
        or not isinstance(columns, int)
        or rows <= 0
        or columns <= 0
        or rows > MAX_REGION_ROWS
        or columns > MAX_REGION_COLUMNS
        or rows * columns > MAX_REGION_PIXELS
    ):
        raise NoaaErrArtifactError("NOAA artifactgrid valt buiten de grens")
    expected_grid = {
        "axis_order": "yx",
        "columns": expected_window.columns,
        "coverage_definition_sha256": expected_window.coverage_definition_sha256,
        "global_column_start": expected_window.column_start,
        "global_column_stop_exclusive": expected_window.column_stop,
        "global_row_start": expected_window.row_start,
        "global_row_stop_exclusive": expected_window.row_stop,
        "grid_definition_sha256": expected_window.grid_definition_sha256,
        "maximum_latitude": expected_window.maximum_latitude,
        "maximum_longitude": expected_window.maximum_longitude,
        "minimum_latitude": expected_window.minimum_latitude,
        "minimum_longitude": expected_window.minimum_longitude,
        "native_crs": "EPSG:4326",
        "resolution_degrees": GRID_RESOLUTION_DEGREES,
        "row_direction": "north_to_south",
        "rows": expected_window.rows,
    }
    if grid != expected_grid:
        raise NoaaErrArtifactError("NOAA artifactgridbinding wijkt af")
    compressed = payload[header_end:]
    expected_compression_fields = frozenset({
        "codec",
        "compressed_length",
        "compressed_sha256",
        "level",
        "uncompressed_length",
        "uncompressed_sha256",
    })
    if (
        frozenset(compression) != expected_compression_fields
        or compression.get("codec") != "zlib"
        or compression.get("level") != 6
        or compression.get("compressed_length") != len(compressed)
        or compression.get("compressed_sha256")
        != hashlib.sha256(compressed).hexdigest()
    ):
        raise NoaaErrArtifactError("NOAA artifactcompressiebinding wijkt af")
    expected_uncompressed = compression.get("uncompressed_length")
    if isinstance(expected_uncompressed, bool) or not isinstance(
        expected_uncompressed, int
    ):
        raise NoaaErrArtifactError("NOAA artifact mist decoded lengte")
    uncompressed = _bounded_decompress(
        compressed, expected_uncompressed, checkpoint=checkpoint
    )
    if compression.get("uncompressed_sha256") != hashlib.sha256(
        uncompressed
    ).hexdigest():
        raise NoaaErrArtifactError("NOAA decoded arrays falen SHA-256")

    arrays: dict[str, np.ndarray] = {}
    expected_offset = 0
    expected_section_fields = frozenset({
        "dtype", "length", "name", "offset", "sha256", "shape",
    })
    for index, (expected_name, expected_dtype) in enumerate(ARRAY_ORDER):
        checkpoint("artifact_array_parse")
        section = sections[index]
        if (
            not isinstance(section, dict)
            or frozenset(section) != expected_section_fields
        ):
            raise NoaaErrArtifactError("NOAA artifactsectie is ongeldig")
        dtype = np.dtype(expected_dtype)
        length = rows * columns * dtype.itemsize
        if (
            section.get("name") != expected_name
            or section.get("dtype") != dtype.str
            or section.get("shape") != [rows, columns]
            or section.get("offset") != expected_offset
            or section.get("length") != length
        ):
            raise NoaaErrArtifactError("NOAA artifactsectiebinding wijkt af")
        chunk = uncompressed[expected_offset:expected_offset + length]
        if len(chunk) != length or section.get("sha256") != hashlib.sha256(
            chunk
        ).hexdigest():
            raise NoaaErrArtifactError("NOAA artifactsectie faalt SHA-256")
        array = np.frombuffer(chunk, dtype=dtype).reshape(rows, columns).copy()
        array.setflags(write=False)
        arrays[expected_name] = array
        expected_offset += length
    if expected_offset != len(uncompressed):
        raise NoaaErrArtifactError("NOAA artifact bevat extra arraybytes")
    checkpoint("artifact_semantics_before")
    semantics_valid = _arrays_semantically_valid(arrays)
    checkpoint("artifact_semantics_after")
    if not semantics_valid:
        raise NoaaErrArtifactError("NOAA artifactarrays schenden de semantiek")
    return NoaaErrRasterArtifact(metadata=metadata, **arrays)


def _arrays_semantically_valid(arrays: Mapping[str, np.ndarray]) -> bool:
    raw_rate = arrays["rrqpe_raw"]
    rate = arrays["precipitation_rate_mm_h"]
    valid = arrays["rate_valid_mask"]
    missing = arrays["rate_missing_mask"]
    raw_dqf = arrays["dqf_raw"]
    dqf_missing = arrays["dqf_missing_mask"]
    dqf_bad = arrays["dqf_bad_quality_mask"]
    dqf_degraded = arrays["dqf_degraded_mask"]
    optimal = arrays["quality_optimal_mask"]
    expected_missing = np.asarray(raw_rate == RRQPE_FILL, dtype=np.uint8)
    expected_valid = np.asarray(expected_missing == 0, dtype=np.uint8)
    expected_dqf_missing = np.asarray(raw_dqf == DQF_FILL, dtype=np.uint8)
    expected_bad = np.asarray(
        (raw_dqf != DQF_FILL) & ((raw_dqf & DQF_BAD_QUALITY_MASK) != 0),
        dtype=np.uint8,
    )
    expected_degraded = np.asarray(
        (raw_dqf != DQF_FILL) & ((raw_dqf & DQF_DEGRADED_MASK) != 0),
        dtype=np.uint8,
    )
    expected_optimal = np.asarray(raw_dqf == 0, dtype=np.uint8)
    unique_dqf = {int(value) for value in np.unique(raw_dqf)}
    if (
        not unique_dqf.issubset(DQF_ALLOWED_VALUES)
        or np.any((raw_rate < 0) & (raw_rate != RRQPE_FILL))
        or not np.array_equal(valid, expected_valid)
        or not np.array_equal(missing, expected_missing)
        or not np.array_equal(dqf_missing, expected_dqf_missing)
        or not np.array_equal(expected_missing, expected_dqf_missing)
        or not np.array_equal(dqf_bad, expected_bad)
        or not np.array_equal(dqf_degraded, expected_degraded)
        or not np.array_equal(optimal, expected_optimal)
        or np.any(rate[expected_valid == 0] != 0.0)
        or not np.isfinite(rate[expected_valid == 1]).all()
    ):
        return False
    expected_rate = raw_rate[expected_valid == 1].astype(np.float64) * 0.1
    return np.array_equal(rate[expected_valid == 1], expected_rate)


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


def _member_receipt(
    capture: RawArtifactCaptureReceipt,
    receipts: Mapping[str, RawArtifactReceipt],
    role: str,
) -> RawArtifactReceipt:
    matching = [member for member in capture.members if member.role == role]
    if len(matching) != 1:
        raise _RejectedDecode("incomplete", "incomplete:capture_roles")
    member = matching[0]
    receipt = receipts.get(role)
    if not isinstance(receipt, RawArtifactReceipt):
        raise RawArtifactIntegrityError(
            f"NOAA ERR capture member {role} receipt ontbreekt"
        )
    if (
        receipt.receipt_id != member.receipt_id
        or receipt.content_sha256 != member.content_sha256
        or receipt.receipt_sha256 != member.receipt_sha256
        or receipt.received_at != member.received_at
    ):
        raise RawArtifactIntegrityError(
            f"NOAA ERR capture member {role} wijkt af van receipt"
        )
    return receipt


def _validate_raw_capture(
    raw_store: RawArtifactStore,
    capture: RawArtifactCaptureReceipt,
    *,
    receipts: Mapping[str, RawArtifactReceipt],
    payloads: Mapping[str, bytes],
) -> tuple[
    RawArtifactReceipt,
    RawArtifactReceipt,
    native.NoaaErrObject,
    bytes,
    str,
]:
    if (
        capture.provider != "noaa_enterprise_rainrate"
        or capture.product != native.NOAA_ERR_CAPTURE_PRODUCT
    ):
        raise _RejectedDecode("unsupported", "unsupported:capture_contract")
    if capture.product_timestamp is None:
        raise _RejectedDecode("incomplete", "incomplete:product_time")
    if (
        len(capture.members) != 2
        or {member.role for member in capture.members}
        != {"listing_xml", "netcdf4"}
    ):
        raise _RejectedDecode("incomplete", "incomplete:capture_roles")
    suffix = ":capture"
    if not capture.fetch_id.endswith(suffix) or len(capture.fetch_id) <= len(suffix):
        raise _RejectedDecode("unsupported", "unsupported:capture_fetch_id")
    job_id = capture.fetch_id[:-len(suffix)]
    try:
        listing_receipt = _member_receipt(
            capture, receipts, "listing_xml"
        )
        netcdf_receipt = _member_receipt(capture, receipts, "netcdf4")
        prefix = native._validate_listing_receipt(
            listing_receipt, job_id=job_id
        )
    except RawArtifactIntegrityError:
        raise
    except (RuntimeError, ValueError, TypeError, KeyError):
        raise _RejectedDecode(
            "unsupported", "unsupported:listing_receipt_contract"
        ) from None
    if frozenset(payloads) != frozenset({"listing_xml", "netcdf4"}):
        raise RawArtifactIntegrityError("NOAA ERR capturebundle mist exacte bytes")
    listing_payload = payloads["listing_xml"]
    listing_digest = listing_receipt.content_sha256
    try:
        selected = native._parse_listing_xml(
            listing_payload, expected_prefix=prefix
        )
        native._validate_netcdf_receipt(
            netcdf_receipt,
            job_id=job_id,
            selected=selected,
        )
    except (RuntimeError, ValueError, TypeError, KeyError):
        raise _RejectedDecode(
            "unsupported", "unsupported:netcdf_receipt_contract"
        ) from None
    listing_provenance = listing_receipt.provenance
    netcdf_provenance = netcdf_receipt.provenance
    if (
        listing_receipt.provider != "noaa_enterprise_rainrate"
        or listing_receipt.product != native.NOAA_ERR_LISTING_PRODUCT
        or listing_provenance.get("representation") != NATIVE_REPRESENTATION
        or listing_provenance.get("capture_stage") != NATIVE_CAPTURE_STAGE
        or listing_provenance.get("collector") != native.NOAA_ERR_RAW_COLLECTOR
        or listing_provenance.get("collector_version")
        != native.NOAA_ERR_RAW_COLLECTOR_VERSION
        or netcdf_receipt.provider != "noaa_enterprise_rainrate"
        or netcdf_receipt.product != native.NOAA_ERR_NETCDF4_PRODUCT
        or netcdf_receipt.size_bytes <= 0
        or netcdf_receipt.size_bytes > MAX_NETCDF_BYTES
        or netcdf_provenance.get("representation") != NATIVE_REPRESENTATION
        or netcdf_provenance.get("capture_stage") != NATIVE_CAPTURE_STAGE
        or netcdf_provenance.get("collector") != native.NOAA_ERR_RAW_COLLECTOR
        or netcdf_provenance.get("collector_version")
        != native.NOAA_ERR_RAW_COLLECTOR_VERSION
        or netcdf_provenance.get("declared_semantic_class")
        != native.NOAA_ERR_SEMANTIC_CLASS
        or netcdf_provenance.get("declared_quantity")
        != "instantaneous_precipitation_rate"
        or netcdf_provenance.get("declared_unit") != "mm/h"
        or netcdf_provenance.get(
            "quantitative_semantics_verified_after_decode"
        )
        is not False
    ):
        raise _RejectedDecode("unsupported", "unsupported:raw_provenance")
    netcdf_payload = payloads["netcdf4"]
    netcdf_digest = netcdf_receipt.content_sha256
    expected_capture_upstream_id = (
        f"{native.NOAA_ERR_CAPTURE_PRODUCT}|key={selected.key}"
        f"|etag={selected.etag}|listing={listing_digest}"
        f"|netcdf4={netcdf_digest}"
    )
    if (
        capture.upstream_capture_id != expected_capture_upstream_id
        or capture.product_timestamp != selected.product_start
    ):
        raise _RejectedDecode("unsupported", "unsupported:capture_identity")
    return (
        listing_receipt,
        netcdf_receipt,
        selected,
        netcdf_payload,
        netcdf_digest,
    )


def _run_mapping(
    capture: RawArtifactCaptureReceipt,
    *,
    decoder_job_id: str,
    attempt_id: str,
    evaluated_at: float,
    outcome: str,
    reason_code: str,
    artifact: NoaaErrArtifactRef | None = None,
    frame: _DecodedRegion | None = None,
) -> dict[str, Any]:
    complete = outcome == "decoded_complete"
    if complete != (artifact is not None and frame is not None):
        raise RuntimeError("NOAA decoderresultaat schendt fail-closed invariant")
    native_grid = None
    coverage = None
    if frame is not None:
        window = frame.window
        native_grid = {
            "axis_order": "yx",
            "definition_sha256": window.grid_definition_sha256,
            "height": window.rows,
            "kind": "regular",
            "width": window.columns,
        }
        coverage = {
            "definition_sha256": window.coverage_definition_sha256,
            "kind": "native_bbox",
            "maximum_x": window.maximum_longitude,
            "maximum_y": window.maximum_latitude,
            "minimum_x": window.minimum_longitude,
            "minimum_y": window.minimum_latitude,
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
        "native_crs": "EPSG:4326" if complete else None,
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
        "quantity": "instantaneous_precipitation_rate" if complete else None,
        "reason_code": reason_code,
        "received_at": max(member.received_at for member in capture.members),
        "unit": "mm/h" if complete else None,
    }


def decode_noaa_err_region(
    *,
    capture_id: str,
    decoder_job_id: str,
    attempt_id: str,
    region: NoaaErrRegion,
    raw_store: RawArtifactStore,
    artifact_store: NoaaErrDerivedArtifactStore,
    evidence_store: CanonicalEvidenceStore,
    evaluated_at: float | None = None,
    clock: Callable[[], float] | None = None,
    monotonic_deadline: float | None = None,
    monotonic_clock: Callable[[], float] | None = None,
) -> NoaaErrDecodeResult:
    """Decode one bounded region and never grant operational admission."""

    if not isinstance(raw_store, RawArtifactStore):
        raise TypeError("raw_store moet RawArtifactStore zijn")
    if not isinstance(artifact_store, NoaaErrDerivedArtifactStore):
        raise TypeError("artifact_store heeft verkeerd type")
    if not isinstance(evidence_store, CanonicalEvidenceStore):
        raise TypeError("evidence_store heeft verkeerd type")
    active_monotonic_clock = monotonic_clock or time.monotonic
    monotonic_now = float(active_monotonic_clock())
    if not math.isfinite(monotonic_now):
        raise ValueError("monotone decoderklok moet eindig zijn")
    if monotonic_deadline is None:
        active_deadline = monotonic_now + DECODE_TOTAL_DEADLINE_SECONDS
    else:
        if isinstance(monotonic_deadline, bool) or not isinstance(
            monotonic_deadline, (int, float)
        ):
            raise ValueError("monotone decoderdeadline moet eindig zijn")
        active_deadline = float(monotonic_deadline)
        if not math.isfinite(active_deadline):
            raise ValueError("monotone decoderdeadline moet eindig zijn")

    def checkpoint(stage: str) -> None:
        current = float(active_monotonic_clock())
        if not math.isfinite(current) or current >= active_deadline:
            raise NoaaErrDecodeDeadlineError(
                f"NOAA decoderdeadline verstreek bij {stage}"
            )

    checkpoint("decode_start")
    window = _window_for_region(region)
    checkpoint("region_validated")
    bundle = raw_store.load_capture_bundle_bytes(capture_id)
    if bundle is None:
        raise NoaaErrCaptureMissingError("schema-3 NOAA ERR capture ontbreekt")
    capture, capture_receipts, capture_payloads = bundle
    received_at = max(member.received_at for member in capture.members)
    evaluation = (
        float((clock or __import__("time").time)())
        if evaluated_at is None
        else float(evaluated_at)
    )
    if not math.isfinite(evaluation) or evaluation < received_at:
        raise ValueError("evaluated_at moet eindig zijn en receipt niet voorafgaan")
    try:
        (
            listing_receipt,
            netcdf_receipt,
            selected,
            netcdf_payload,
            netcdf_digest,
        ) = _validate_raw_capture(
            raw_store,
            capture,
            receipts=capture_receipts,
            payloads=capture_payloads,
        )
        checkpoint("raw_capture_validated")
        frame = _decode_hdf_region(
            netcdf_payload,
            selected=selected,
            window=window,
            source_sha256=netcdf_digest,
            checkpoint=checkpoint,
        )
        artifact_payload = _artifact_bytes(
            capture,
            listing_receipt=listing_receipt,
            netcdf_receipt=netcdf_receipt,
            frame=frame,
            checkpoint=checkpoint,
        )
        artifact = artifact_store.publish(
            artifact_payload, checkpoint=checkpoint
        )
        verified = artifact_store.read_decoded(
            artifact, checkpoint=checkpoint
        )
        if (
            verified.metadata.get("capture")
            != _artifact_capture_metadata(capture)
            or verified.metadata.get("source")
            != _artifact_source_metadata(
                listing_receipt=listing_receipt,
                netcdf_receipt=netcdf_receipt,
                frame=frame,
            )
            or verified.metadata.get("product")
            != _artifact_product_metadata(frame)
        ):
            raise NoaaErrArtifactError(
                "NOAA published artifact wijkt af van verified raw input"
            )
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
        return NoaaErrDecodeResult(
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
        return NoaaErrDecodeResult(
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
    "NoaaErrArtifactCapacityError",
    "NoaaErrArtifactError",
    "NoaaErrArtifactRef",
    "NoaaErrCaptureMissingError",
    "NoaaErrDecodeResult",
    "NoaaErrDecodeDeadlineError",
    "NoaaErrDerivedArtifactStore",
    "NoaaErrQuantitativeError",
    "NoaaErrRasterArtifact",
    "NoaaErrRegion",
    "decode_noaa_err_region",
]
