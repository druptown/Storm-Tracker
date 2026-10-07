"""Exact, non-operational native capture for official H SAF H40B.

The candidate stores the exact FTP NLST data-channel payload used for
selection and the exact selected ``.nc.gz`` product before parsing either.
Credentials remain environment-only and are never included in receipts,
artifacts, return values or exception messages.

Only the official PUM 1.3 container identity is inspected.  No coordinate
axis or precipitation/QC raster value is read and no meteorological decision
is emitted.  This module is intentionally absent from every live registry,
scheduler and service table.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from ftplib import FTP, all_errors as FTP_ERRORS, error_perm
import errno
import hashlib
import io
import math
import os
from pathlib import Path
import re
import stat
import threading
import time
from typing import Any, Callable, Mapping
import zlib

import h5py
import numpy as np

from .raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RAW_ARTIFACT_SCHEMA_VERSION,
    RawArtifactCaptureReceipt,
    RawArtifactReceipt,
    RawArtifactStore,
)


FTP_HOST = "ftphsaf.meteoam.it"
FTP_PORT = 21
FTP_DIRECTORY = "/products/h40B/h40_cur_mon_data"
FTP_LIST_COMMAND = f"NLST {FTP_DIRECTORY}"
PRODUCT_ID = "H40B"
PRODUCT_ACRONYM = "P-IN-FCI"
COLLECTION_REFERENCE = "EO:EUM:DAT:1086"
PUM_REFERENCE = "SAF/HSAF/PUM-40B"
PUM_REVISION = "1.3"

PROVIDER = "hsaf_h40b"
LISTING_PRODUCT = "hsaf_h40b_official_ftp_nlst"
NETCDF_GZIP_PRODUCT = "hsaf_h40b_pin_fci_original_netcdf_gzip"
CAPTURE_PRODUCT = "hsaf_h40b_ftp_selection_netcdf_gzip_capture"
COLLECTOR = "hsaf_h40b_official_ftp_exact_native"
COLLECTOR_VERSION = "worker-hsaf-h40b-exact-native-v1"
LISTING_INSPECTOR_VERSION = "hsaf-h40b-official-nlst-v1"
CONTAINER_INSPECTOR_VERSION = "hsaf-h40b-pum-1.3-netcdf4-profile-v1"
ADMISSION_STATUS = "exact_raw_shadow_not_decoded_not_admitted"

FILENAME_PATTERN = re.compile(
    r"^h40_(?P<date>\d{8})_(?P<time>\d{4})_fdk\.nc\.gz$"
)
JOB_ID_PATTERN = re.compile(r"^[^\x00-\x1f\x7f]{1,480}$")
COMPLETION_PATTERN = re.compile(r"^(?:226|250)(?:[ -].*)?$", re.ASCII)

GRID_SIZE = 5_568
OBSERVING_CYCLE_MINUTES = 10
APPROX_NADIR_RESOLUTION_KM = 2.0
RR_VARIABLE = "rr"
RR_DTYPE = "int16"
RR_VALID_RANGE = (0, 2_000)
RR_SCALE_FACTOR = 0.1
RR_ADD_OFFSET = 0.0
RR_MISSING = -990
RR_UNIT = "mm/h"
QIND_VARIABLE = "qind"
QIND_DTYPE = "int8"
QIND_VALID_RANGE = (0, 100)
QIND_MISSING = -99
QIND_UNIT = "percent"
PROJECTION_VARIABLE = "geostationary_projection"
PROJECTION_PARAMETERS = {
    "grid_mapping_name": "geostationary",
    "latitude_of_projection_origin": 0.0,
    "longitude_of_projection_origin": 0.0,
    "perspective_point_height": 35_785_831.0,
    "semi_major_axis": 6_378_169.0,
    "semi_minor_axis": 6_356_584.0,
    "sweep_angle_axis": "y",
}

MAX_LISTING_BYTES = 2 * 1024 * 1024
MAX_LISTING_ENTRIES = 20_000
MAX_COMPRESSED_BYTES = 64 * 1024 * 1024
MAX_UNCOMPRESSED_BYTES = 192 * 1024 * 1024
MAX_HDF5_OBJECTS = 128
MAX_HEADER_TEXT = 8_192
STREAM_CHUNK_BYTES = 128 * 1024
FTP_IO_TIMEOUT_SECONDS = 25.0
CAPTURE_TOTAL_DEADLINE_SECONDS = 90.0
LOCK_POLL_SECONDS = 0.05

RAW_SHARE_ROOT = Path("/share/storm_tracker_worker")
RAW_ARTIFACT_ROOT = RAW_SHARE_ROOT / "raw" / "hsaf_h40b_native"

_RAW_STORE: RawArtifactStore | None = None
_RAW_STORE_LOCK = threading.Lock()
_CAPTURE_LOCK = threading.Lock()


class HsafH40bNativeError(RuntimeError):
    """Base failure; a failed or absent capture says nothing about weather."""


class HsafH40bNativeContractError(HsafH40bNativeError):
    """Official transport/product identity left the pinned profile."""


class HsafH40bNativeDeadlineError(HsafH40bNativeError):
    """The bounded end-to-end capture deadline elapsed."""


class HsafH40bNativeCorruptionError(HsafH40bNativeError):
    """Durable raw evidence conflicts with its receipt or capture identity."""


@dataclass(frozen=True, slots=True)
class HsafCredentials:
    # Keep an accidental repr/log call from turning the short-lived transport
    # bundle into diagnostic output.  Equality remains available for tests and
    # the FTP boundary, but neither value has a printable representation.
    username: str = field(repr=False)
    password: str = field(repr=False)


@dataclass(frozen=True, slots=True)
class HsafFtpResponse:
    payload: bytes
    received_at: float
    completion: str
    declared_size: int | None = None


@dataclass(frozen=True, slots=True)
class HsafListingIdentity:
    selected_basename: str
    selected_remote_path: str
    nominal_slot_time: float
    candidate_count: int
    first_slot_time: float
    last_slot_time: float


@dataclass(frozen=True, slots=True)
class HsafNetcdfIdentity:
    object_count: int
    group_count: int
    dataset_count: int
    rr_shape: tuple[int, int]
    rr_dtype: str
    qind_shape: tuple[int, int]
    qind_dtype: str
    nx_shape: tuple[int]
    ny_shape: tuple[int]
    conventions: str
    platform: str
    algorithm_version: str
    parallax_correction: str
    pixel_values_read: bool = False
    coordinate_values_read: bool = False


def _remaining(deadline: float) -> float:
    remaining = float(deadline) - time.monotonic()
    if not math.isfinite(remaining) or remaining <= 0:
        raise HsafH40bNativeDeadlineError(
            "H SAF H40B capturedeadline is verstreken"
        )
    return remaining


def _bounded_text(value: object, context: str, maximum: int = MAX_HEADER_TEXT) -> str:
    if (
        not isinstance(value, str)
        or value != value.strip()
        or not value
        or len(value) > maximum
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
    ):
        raise HsafH40bNativeContractError(f"H SAF H40B {context} is ongeldig")
    return value


def _job_id(job: Any) -> str:
    value = getattr(job, "job_id", None)
    payload = getattr(job, "payload", None)
    if not isinstance(value, str) or JOB_ID_PATTERN.fullmatch(value) is None:
        raise HsafH40bNativeContractError(
            "H SAF H40B capturejob mist een begrensde job_id"
        )
    if type(payload) is not dict or payload:
        raise HsafH40bNativeContractError(
            "H SAF H40B native capture vereist een exact lege jobpayload"
        )
    return value


def _credentials_from_environment() -> HsafCredentials:
    username = os.getenv("STV3_HSAF_USERNAME", "")
    password = os.getenv("STV3_HSAF_PASSWORD", "")
    if (
        username != username.strip()
        or password != password.strip()
        or not 3 <= len(username) <= 512
        or not 8 <= len(password) <= 4_096
        or not username.isascii()
        or not password.isascii()
        or any(ord(char) < 0x21 or ord(char) == 0x7F for char in username)
        or any(ord(char) < 0x21 or ord(char) == 0x7F for char in password)
    ):
        raise HsafH40bNativeContractError(
            "H SAF H40B FTP-credential ontbreekt of is ongeldig"
        )
    return HsafCredentials(username=username, password=password)


def _validated_credentials(credentials: object) -> HsafCredentials:
    """Validate an injected bundle without ever formatting its values."""

    if not isinstance(credentials, HsafCredentials):
        raise HsafH40bNativeContractError(
            "H SAF H40B FTP-credential is ongeldig"
        )
    username = credentials.username
    password = credentials.password
    if (
        not isinstance(username, str)
        or not isinstance(password, str)
        or username != username.strip()
        or password != password.strip()
        or not 3 <= len(username) <= 512
        or not 8 <= len(password) <= 4_096
        or not username.isascii()
        or not password.isascii()
        or any(ord(char) < 0x21 or ord(char) == 0x7F for char in username)
        or any(ord(char) < 0x21 or ord(char) == 0x7F for char in password)
    ):
        raise HsafH40bNativeContractError(
            "H SAF H40B FTP-credential is ongeldig"
        )
    return credentials


def _assert_credentials_absent(
    value: bytes | str | None,
    credentials: HsafCredentials,
) -> None:
    """Reject an upstream/control value that reflects either credential.

    H40B uses a separate FTP data channel, but a compromised or drifting
    endpoint must still not be able to copy USER/PASS material into immutable
    raw evidence or a public upstream identifier.  This check runs before the
    corresponding response is handed to ``RawArtifactStore``.
    """

    credentials = _validated_credentials(credentials)
    if value is None:
        return
    if isinstance(value, bytes):
        leaked = any(
            secret.encode("ascii") in value
            for secret in (credentials.username, credentials.password)
        )
    elif isinstance(value, str):
        leaked = any(
            secret in value
            for secret in (credentials.username, credentials.password)
        )
    else:
        raise HsafH40bNativeContractError(
            "H SAF H40B credentialscan kreeg een ongeldig veld"
        )
    if leaked:
        raise HsafH40bNativeContractError(
            "H SAF H40B upstreamrespons bevat credentialmateriaal"
        )


def _validated_ftp_response(
    response: object,
    *,
    credentials: HsafCredentials,
    maximum_bytes: int,
    declared_size_allowed: bool,
) -> HsafFtpResponse:
    """Validate the complete transport envelope before durable storage."""

    credentials = _validated_credentials(credentials)
    if not isinstance(response, HsafFtpResponse):
        raise HsafH40bNativeContractError(
            "H SAF H40B FTP-responsecontract is ongeldig"
        )
    payload = response.payload
    received_at = response.received_at
    declared_size = response.declared_size
    if (
        not isinstance(payload, bytes)
        or not 1 <= len(payload) <= maximum_bytes
        or isinstance(received_at, bool)
        or not isinstance(received_at, (int, float))
        or not math.isfinite(float(received_at))
        or not 0.0 <= float(received_at) <= 2**53
        or (
            declared_size is not None
            and (
                not declared_size_allowed
                or isinstance(declared_size, bool)
                or not isinstance(declared_size, int)
                or declared_size != len(payload)
            )
        )
    ):
        raise HsafH40bNativeContractError(
            "H SAF H40B FTP-responsecontract is ongeldig"
        )
    completion = _completion(response.completion)
    _assert_credentials_absent(payload, credentials)
    _assert_credentials_absent(completion, credentials)
    return HsafFtpResponse(
        payload=payload,
        received_at=float(received_at),
        completion=completion,
        declared_size=declared_size,
    )


def _completion(value: object) -> str:
    result = _bounded_text(value, "FTP-completion", maximum=512)
    if COMPLETION_PATTERN.fullmatch(result) is None:
        raise HsafH40bNativeContractError(
            "H SAF H40B FTP-transfer werd niet volledig afgesloten"
        )
    return result


def _validated_product_path(remote_path: object) -> str:
    if not isinstance(remote_path, str):
        raise HsafH40bNativeContractError(
            "H SAF H40B productpad is ongeldig"
        )
    prefix = f"{FTP_DIRECTORY}/"
    if not remote_path.startswith(prefix):
        raise HsafH40bNativeContractError(
            "H SAF H40B productpad valt buiten de vaste directory"
        )
    basename = remote_path.removeprefix(prefix)
    if (
        not basename
        or "/" in basename
        or "\\" in basename
        or FILENAME_PATTERN.fullmatch(basename) is None
    ):
        raise HsafH40bNativeContractError(
            "H SAF H40B productpad wijkt af van PUM 1.3"
        )
    _slot_from_basename(basename)
    return remote_path


def _apply_ftp_deadline(ftp: FTP, *, deadline: float) -> float:
    timeout = min(FTP_IO_TIMEOUT_SECONDS, _remaining(deadline))
    ftp.timeout = timeout
    if ftp.sock is not None:
        ftp.sock.settimeout(timeout)
    return timeout


def _ftp_retrieve(
    command: str,
    *,
    credentials: HsafCredentials,
    maximum_bytes: int,
    deadline: float,
    request_size: bool,
) -> HsafFtpResponse:
    credentials = _validated_credentials(credentials)
    if command == FTP_LIST_COMMAND:
        pass
    elif command.startswith("RETR "):
        _validated_product_path(command.removeprefix("RETR "))
    else:
        raise HsafH40bNativeContractError(
            "H SAF H40B FTP-command valt buiten het vaste productpad"
        )
    payload = bytearray()
    ftp = FTP()
    declared_size: int | None = None
    try:
        ftp.connect(
            FTP_HOST,
            FTP_PORT,
            timeout=min(FTP_IO_TIMEOUT_SECONDS, _remaining(deadline)),
        )
        _apply_ftp_deadline(ftp, deadline=deadline)
        ftp.login(credentials.username, credentials.password)
        ftp.set_pasv(True)
        if request_size:
            remote_path = command.removeprefix("RETR ")
            _apply_ftp_deadline(ftp, deadline=deadline)
            try:
                size = ftp.size(remote_path)
            except error_perm:
                size = None
            if size is not None:
                if isinstance(size, bool) or not isinstance(size, int):
                    raise HsafH40bNativeContractError(
                        "H SAF H40B FTP SIZE is ongeldig"
                    )
                if not 1 <= size <= maximum_bytes:
                    raise HsafH40bNativeContractError(
                        "H SAF H40B FTP SIZE valt buiten de bytegrens"
                    )
                declared_size = size

        _apply_ftp_deadline(ftp, deadline=deadline)
        ftp.voidcmd("TYPE I")
        _apply_ftp_deadline(ftp, deadline=deadline)
        with ftp.transfercmd(command) as data_socket:
            while True:
                data_socket.settimeout(
                    min(FTP_IO_TIMEOUT_SECONDS, _remaining(deadline))
                )
                block = data_socket.recv(STREAM_CHUNK_BYTES)
                if not block:
                    break
                payload.extend(block)
                if len(payload) > maximum_bytes:
                    raise HsafH40bNativeContractError(
                        "H SAF H40B FTP-response overschrijdt de bytegrens"
                    )
        _apply_ftp_deadline(ftp, deadline=deadline)
        response = ftp.voidresp()
        completion = _completion(response)
    except HsafH40bNativeError:
        raise
    except FTP_ERRORS:
        raise HsafH40bNativeError(
            "H SAF H40B officiële FTP-transfer is mislukt"
        ) from None
    finally:
        try:
            ftp.close()
        except FTP_ERRORS:
            pass
    result = bytes(payload)
    if not result:
        raise HsafH40bNativeContractError(
            "H SAF H40B FTP-response is leeg"
        )
    if declared_size is not None and len(result) != declared_size:
        raise HsafH40bNativeContractError(
            "H SAF H40B FTP-productlengte wijkt af van SIZE"
        )
    return _validated_ftp_response(
        HsafFtpResponse(
            payload=result,
            received_at=time.time(),
            completion=completion,
            declared_size=declared_size,
        ),
        credentials=credentials,
        maximum_bytes=maximum_bytes,
        declared_size_allowed=request_size,
    )


def _fetch_listing(
    credentials: HsafCredentials, *, deadline: float
) -> HsafFtpResponse:
    return _ftp_retrieve(
        FTP_LIST_COMMAND,
        credentials=credentials,
        maximum_bytes=MAX_LISTING_BYTES,
        deadline=deadline,
        request_size=False,
    )


def _fetch_product(
    credentials: HsafCredentials,
    remote_path: str,
    *,
    deadline: float,
) -> HsafFtpResponse:
    _validated_product_path(remote_path)
    return _ftp_retrieve(
        f"RETR {remote_path}",
        credentials=credentials,
        maximum_bytes=MAX_COMPRESSED_BYTES,
        deadline=deadline,
        request_size=True,
    )


def _slot_from_basename(basename: str) -> float:
    match = FILENAME_PATTERN.fullmatch(basename)
    if match is None:
        raise HsafH40bNativeContractError(
            "H SAF H40B bestandsnaam wijkt af van PUM 1.3"
        )
    try:
        moment = datetime.strptime(
            match.group("date") + match.group("time"), "%Y%m%d%H%M"
        ).replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise HsafH40bNativeContractError(
            "H SAF H40B bestandsnaam bevat ongeldige UTC-tijd"
        ) from exc
    if moment.minute % OBSERVING_CYCLE_MINUTES != 0:
        raise HsafH40bNativeContractError(
            "H SAF H40B bestandsnaam valt niet op tienminutenrooster"
        )
    return moment.timestamp()


def _listing_basename(value: str) -> str:
    if value.startswith(f"{FTP_DIRECTORY}/"):
        basename = value.removeprefix(f"{FTP_DIRECTORY}/")
    elif "/" not in value:
        basename = value
    else:
        raise HsafH40bNativeContractError(
            "H SAF H40B listing bevat pad buiten officiële directory"
        )
    if (
        not basename
        or basename in {".", ".."}
        or "/" in basename
        or "\\" in basename
        or len(basename) > 128
    ):
        raise HsafH40bNativeContractError(
            "H SAF H40B listing bevat onveilige bestandsnaam"
        )
    return basename


def _inspect_listing(payload: bytes) -> HsafListingIdentity:
    if not 1 <= len(payload) <= MAX_LISTING_BYTES:
        raise HsafH40bNativeContractError(
            "H SAF H40B listing valt buiten de bytegrens"
        )
    try:
        text = payload.decode("ascii")
    except UnicodeDecodeError as exc:
        raise HsafH40bNativeContractError(
            "H SAF H40B listing is niet ASCII"
        ) from exc
    lines = text.splitlines()
    if not 1 <= len(lines) <= MAX_LISTING_ENTRIES:
        raise HsafH40bNativeContractError(
            "H SAF H40B listing bevat ongeldig aantal regels"
        )
    candidates: dict[str, float] = {}
    seen_lines: set[str] = set()
    for raw in lines:
        if raw != raw.strip() or not raw or any(
            ord(char) < 0x20 or ord(char) == 0x7F for char in raw
        ):
            raise HsafH40bNativeContractError(
                "H SAF H40B listing bevat ongeldige regel"
            )
        if raw in seen_lines:
            raise HsafH40bNativeContractError(
                "H SAF H40B listing bevat dubbele regel"
            )
        seen_lines.add(raw)
        basename = _listing_basename(raw)
        if FILENAME_PATTERN.fullmatch(basename) is None:
            continue
        if basename in candidates:
            raise HsafH40bNativeContractError(
                "H SAF H40B listing bevat dubbele productidentiteit"
            )
        candidates[basename] = _slot_from_basename(basename)
    if not candidates:
        raise HsafH40bNativeContractError(
            "H SAF H40B listing bevat geen P-IN-FCI NetCDF-product"
        )
    selected_basename, selected_time = max(
        candidates.items(), key=lambda item: (item[1], item[0])
    )
    times = tuple(candidates.values())
    return HsafListingIdentity(
        selected_basename=selected_basename,
        selected_remote_path=f"{FTP_DIRECTORY}/{selected_basename}",
        nominal_slot_time=selected_time,
        candidate_count=len(candidates),
        first_slot_time=min(times),
        last_slot_time=max(times),
    )


def _decompress_single_gzip(payload: bytes) -> bytes:
    if (
        not 1 <= len(payload) <= MAX_COMPRESSED_BYTES
        or not payload.startswith(b"\x1f\x8b\x08")
    ):
        raise HsafH40bNativeContractError(
            "H SAF H40B product is geen begrensde gzip"
        )
    decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
    try:
        output = decoder.decompress(payload, MAX_UNCOMPRESSED_BYTES + 1)
        if decoder.unconsumed_tail:
            raise HsafH40bNativeContractError(
                "H SAF H40B uitgepakt product overschrijdt de bytegrens"
            )
        output += decoder.flush()
    except zlib.error as exc:
        raise HsafH40bNativeContractError(
            "H SAF H40B gzip CRC/container is ongeldig"
        ) from exc
    if (
        not decoder.eof
        or decoder.unused_data
        or not 1 <= len(output) <= MAX_UNCOMPRESSED_BYTES
    ):
        raise HsafH40bNativeContractError(
            "H SAF H40B gzip is onvolledig, aaneengeschakeld of te groot"
        )
    return output


def _scalar_attribute(dataset: Any, name: str) -> Any:
    if name not in dataset.attrs:
        raise HsafH40bNativeContractError(
            f"H SAF H40B attribuut {name} ontbreekt"
        )
    value = dataset.attrs[name]
    array = np.asarray(value)
    if array.size != 1:
        raise HsafH40bNativeContractError(
            f"H SAF H40B attribuut {name} is niet scalair"
        )
    result = array.reshape(-1)[0]
    if hasattr(result, "item"):
        result = result.item()
    if isinstance(result, bytes):
        try:
            result = result.decode("ascii")
        except UnicodeDecodeError as exc:
            raise HsafH40bNativeContractError(
                f"H SAF H40B attribuut {name} is geen ASCII"
            ) from exc
    return result


def _require_text_attribute(dataset: Any, name: str, expected: str) -> str:
    value = _scalar_attribute(dataset, name)
    if value != expected:
        raise HsafH40bNativeContractError(
            f"H SAF H40B attribuut {name} wijkt af"
        )
    return expected


def _require_number_attribute(
    dataset: Any, name: str, expected: float, *, tolerance: float = 0.0
) -> float:
    value = _scalar_attribute(dataset, name)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise HsafH40bNativeContractError(
            f"H SAF H40B attribuut {name} is niet numeriek"
        )
    result = float(value)
    if not math.isfinite(result) or not math.isclose(
        result, float(expected), rel_tol=0.0, abs_tol=tolerance
    ):
        raise HsafH40bNativeContractError(
            f"H SAF H40B attribuut {name} wijkt af"
        )
    return result


def _inspect_netcdf_gzip(payload: bytes) -> HsafNetcdfIdentity:
    netcdf = _decompress_single_gzip(payload)
    if not netcdf.startswith(b"\x89HDF\r\n\x1a\n"):
        raise HsafH40bNativeContractError(
            "H SAF H40B digitale product is geen ondersteund NetCDF4/HDF5"
        )
    groups = 0
    datasets = 0
    try:
        with h5py.File(io.BytesIO(netcdf), "r") as product:
            link_scan = {"count": 0, "unsafe": False, "overflow": False}

            def visit_link(name: str) -> bool | None:
                link_scan["count"] += 1
                if link_scan["count"] > MAX_HDF5_OBJECTS:
                    link_scan["overflow"] = True
                    return True
                if not isinstance(product.get(name, getlink=True), h5py.HardLink):
                    link_scan["unsafe"] = True
                    return True
                return None

            product.visit_links(visit_link)
            if link_scan["overflow"] or link_scan["unsafe"]:
                raise HsafH40bNativeContractError(
                    "H SAF H40B NetCDF bevat te veel of onveilige links"
                )

            object_scan = {"overflow": False}

            def visit(_name: str, obj: Any) -> bool | None:
                nonlocal groups, datasets
                if groups + datasets >= MAX_HDF5_OBJECTS:
                    object_scan["overflow"] = True
                    return True
                if isinstance(obj, h5py.Group):
                    groups += 1
                elif isinstance(obj, h5py.Dataset):
                    datasets += 1
                return None

            product.visititems(visit)
            if object_scan["overflow"]:
                raise HsafH40bNativeContractError(
                    "H SAF H40B NetCDF bevat te veel objecten"
                )
            required = {
                "nx", "ny", PROJECTION_VARIABLE, RR_VARIABLE, QIND_VARIABLE
            }
            if not required.issubset(product.keys()):
                raise HsafH40bNativeContractError(
                    "H SAF H40B NetCDF mist verplichte PUM-variabelen"
                )
            nx = product["nx"]
            ny = product["ny"]
            projection = product[PROJECTION_VARIABLE]
            rr = product[RR_VARIABLE]
            qind = product[QIND_VARIABLE]
            if (
                tuple(nx.shape) != (GRID_SIZE,)
                or tuple(ny.shape) != (GRID_SIZE,)
                or tuple(rr.shape) != (GRID_SIZE, GRID_SIZE)
                or tuple(qind.shape) != (GRID_SIZE, GRID_SIZE)
                or str(nx.dtype) != "float32"
                or str(ny.dtype) != "float32"
                or str(rr.dtype) != RR_DTYPE
                or str(qind.dtype) != QIND_DTYPE
                or tuple(projection.shape) not in {(), (1,)}
            ):
                raise HsafH40bNativeContractError(
                    "H SAF H40B NetCDF-grid/dtype wijkt af van PUM 1.3"
                )
            _require_text_attribute(nx, "units", "m")
            _require_text_attribute(nx, "standard_name", "projection_x_coordinate")
            _require_text_attribute(ny, "units", "m")
            _require_text_attribute(ny, "standard_name", "projection_y_coordinate")
            for name, expected in PROJECTION_PARAMETERS.items():
                if isinstance(expected, str):
                    _require_text_attribute(projection, name, expected)
                else:
                    _require_number_attribute(
                        projection, name, expected, tolerance=0.25
                    )
            _require_text_attribute(rr, "long_name", "Instantaneous rain rate")
            _require_text_attribute(rr, "units", RR_UNIT)
            _require_number_attribute(rr, "valid_min", RR_VALID_RANGE[0])
            _require_number_attribute(rr, "valid_max", RR_VALID_RANGE[1])
            _require_number_attribute(
                rr, "scale_factor", RR_SCALE_FACTOR, tolerance=1e-12
            )
            _require_number_attribute(rr, "add_offset", RR_ADD_OFFSET)
            _require_number_attribute(rr, "missing_value", RR_MISSING)
            _require_text_attribute(rr, "grid_mapping", PROJECTION_VARIABLE)
            _require_text_attribute(qind, "long_name", "pixel quality index")
            _require_text_attribute(qind, "units", QIND_UNIT)
            _require_number_attribute(qind, "valid_min", QIND_VALID_RANGE[0])
            _require_number_attribute(qind, "valid_max", QIND_VALID_RANGE[1])
            _require_number_attribute(qind, "missing_value", QIND_MISSING)
            _require_text_attribute(qind, "grid_mapping", PROJECTION_VARIABLE)
            conventions = _require_text_attribute(product, "Conventions", "CF-1.7")
            _require_text_attribute(product, "title", "Instantaneous rain rate")
            license_text = _scalar_attribute(product, "license")
            if not isinstance(license_text, str) or "EUMETSAT" not in license_text:
                raise HsafH40bNativeContractError(
                    "H SAF H40B NetCDF mist EUMETSAT-licentie"
                )
            platform = _require_text_attribute(product, "platform", "MTG1")
            algorithm_version = _bounded_text(
                str(_scalar_attribute(product, "algorithm_version")),
                "algorithm_version",
                maximum=64,
            )
            parallax = _bounded_text(
                str(_scalar_attribute(product, "parallax_correction")),
                "parallax_correction",
                maximum=64,
            )
            if parallax.lower() not in {"mode_on", "mode_off"}:
                raise HsafH40bNativeContractError(
                    "H SAF H40B parallaxstatus wijkt af"
                )
    except HsafH40bNativeError:
        raise
    except (OSError, RuntimeError, ValueError) as exc:
        raise HsafH40bNativeContractError(
            "H SAF H40B NetCDF4-container is structureel ongeldig"
        ) from exc
    return HsafNetcdfIdentity(
        object_count=groups + datasets,
        group_count=groups,
        dataset_count=datasets,
        rr_shape=(GRID_SIZE, GRID_SIZE),
        rr_dtype=RR_DTYPE,
        qind_shape=(GRID_SIZE, GRID_SIZE),
        qind_dtype=QIND_DTYPE,
        nx_shape=(GRID_SIZE,),
        ny_shape=(GRID_SIZE,),
        conventions=conventions,
        platform=platform,
        algorithm_version=algorithm_version,
        parallax_correction=parallax,
    )


def _raw_store(raw_store: RawArtifactStore | None) -> RawArtifactStore:
    global _RAW_STORE
    if raw_store is not None:
        if not isinstance(raw_store, RawArtifactStore):
            raise TypeError("raw_store moet een RawArtifactStore zijn")
        raw_store.initialize()
        return raw_store
    with _RAW_STORE_LOCK:
        if _RAW_STORE is None:
            _RAW_STORE = RawArtifactStore(RAW_ARTIFACT_ROOT)
            _RAW_STORE.initialize()
        return _RAW_STORE


def _process_lock_path(store: RawArtifactStore) -> Path:
    root = Path(store.root)
    if root.is_symlink() or not root.is_dir():
        raise HsafH40bNativeCorruptionError(
            "H SAF H40B raw root is onveilig"
        )
    resolved_root = root.resolve()
    path = root / "hsaf_h40b_capture.lock"
    if path.exists() and (path.is_symlink() or not path.is_file()):
        raise HsafH40bNativeCorruptionError(
            "H SAF H40B proceslockpad is onveilig"
        )
    if path.resolve().parent != resolved_root:
        raise HsafH40bNativeCorruptionError(
            "H SAF H40B proceslock ontsnapt uit raw root"
        )
    return path


def _acquire_process_lock(store: RawArtifactStore, *, deadline: float) -> int:
    """Acquire one portable nonblocking byte lock under the shared deadline."""

    path = _process_lock_path(store)
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags, 0o600)
    except OSError as exc:
        raise HsafH40bNativeCorruptionError(
            "H SAF H40B proceslock kon niet veilig worden geopend"
        ) from exc
    acquired = False
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise HsafH40bNativeCorruptionError(
                "H SAF H40B proceslock is geen regulier bestand"
            )
        if info.st_size == 0:
            os.write(descriptor, b"\x00")
            os.fsync(descriptor)
        while not acquired:
            try:
                os.lseek(descriptor, 0, os.SEEK_SET)
                if os.name == "nt":  # pragma: no cover - platform selected
                    import msvcrt

                    msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
                else:  # pragma: no branch - platform selected
                    import fcntl

                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
            except OSError as exc:
                if exc.errno not in {
                    errno.EACCES,
                    errno.EAGAIN,
                    errno.EDEADLK,
                    errno.EWOULDBLOCK,
                }:
                    raise
                time.sleep(min(LOCK_POLL_SECONDS, _remaining(deadline)))
        _remaining(deadline)
        return descriptor
    except Exception:
        os.close(descriptor)
        raise


def _release_process_lock(descriptor: int) -> None:
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


def _receipt_id(store: RawArtifactStore, job_id: str, role: str) -> str:
    product = {
        "listing_nlst": LISTING_PRODUCT,
        "netcdf_gzip": NETCDF_GZIP_PRODUCT,
    }[role]
    return store._receipt_id(  # noqa: SLF001 - deterministic store contract
        provider=PROVIDER,
        product=product,
        fetch_id=f"{job_id}:{role}",
    )


def _capture_id(store: RawArtifactStore, job_id: str) -> str:
    return store._capture_id(  # noqa: SLF001 - deterministic store contract
        provider=PROVIDER,
        product=CAPTURE_PRODUCT,
        fetch_id=f"{job_id}:capture",
    )


def _listing_provenance(response: HsafFtpResponse) -> dict[str, object]:
    return {
        "representation": NATIVE_REPRESENTATION,
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": COLLECTOR,
        "collector_version": COLLECTOR_VERSION,
        "structure_inspector_version": LISTING_INSPECTOR_VERSION,
        "transport": "official_plain_ftp",
        "ftp_host": FTP_HOST,
        "ftp_port": FTP_PORT,
        "ftp_directory": FTP_DIRECTORY,
        "ftp_command": FTP_LIST_COMMAND,
        "ftp_completion": response.completion,
        "credentials_persisted": False,
    }


def _product_provenance(
    response: HsafFtpResponse, identity: HsafListingIdentity
) -> dict[str, object]:
    return {
        "representation": NATIVE_REPRESENTATION,
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": COLLECTOR,
        "collector_version": COLLECTOR_VERSION,
        "structure_inspector_version": CONTAINER_INSPECTOR_VERSION,
        "transport": "official_plain_ftp",
        "ftp_host": FTP_HOST,
        "ftp_port": FTP_PORT,
        "ftp_directory": FTP_DIRECTORY,
        "remote_path": identity.selected_remote_path,
        "basename": identity.selected_basename,
        "nominal_slot_time": identity.nominal_slot_time,
        "ftp_completion": response.completion,
        "ftp_declared_size": response.declared_size,
        "credentials_persisted": False,
        "product_id": PRODUCT_ID,
        "product_acronym": PRODUCT_ACRONYM,
        "collection_reference": COLLECTION_REFERENCE,
        "pum_reference": PUM_REFERENCE,
        "pum_revision": PUM_REVISION,
        "pixel_values_read": False,
        "coordinate_values_read": False,
        "quantitative_semantics_admitted": False,
    }


def _load_optional(
    store: RawArtifactStore, *, job_id: str, role: str
) -> tuple[RawArtifactReceipt | None, bytes | None]:
    loaded = store.load_receipt_bytes(_receipt_id(store, job_id, role))
    return (None, None) if loaded is None else loaded


def _validate_listing_receipt(
    receipt: RawArtifactReceipt, *, job_id: str
) -> None:
    provenance = receipt.provenance
    completion = provenance.get("ftp_completion")
    if (
        receipt.provider != PROVIDER
        or receipt.product != LISTING_PRODUCT
        or receipt.fetch_id != f"{job_id}:listing_nlst"
        or receipt.content_type != "text/plain"
        or not 1 <= receipt.size_bytes <= MAX_LISTING_BYTES
        or receipt.upstream_id
        != (
            f"ftp:{FTP_HOST}:{FTP_PORT}{FTP_DIRECTORY}:NLST"
            f"|content_sha256={receipt.content_sha256}"
        )
        or provenance.get("representation") != NATIVE_REPRESENTATION
        or provenance.get("capture_stage") != NATIVE_CAPTURE_STAGE
        or provenance.get("collector") != COLLECTOR
        or provenance.get("collector_version") != COLLECTOR_VERSION
        or provenance.get("structure_inspector_version")
        != LISTING_INSPECTOR_VERSION
        or provenance.get("transport") != "official_plain_ftp"
        or provenance.get("ftp_host") != FTP_HOST
        or provenance.get("ftp_port") != FTP_PORT
        or provenance.get("ftp_directory") != FTP_DIRECTORY
        or provenance.get("ftp_command") != FTP_LIST_COMMAND
        or provenance.get("credentials_persisted") is not False
        or not isinstance(completion, str)
        or COMPLETION_PATTERN.fullmatch(completion) is None
    ):
        raise HsafH40bNativeCorruptionError(
            "H SAF H40B listingreceipt wijkt af"
        )


def _identity_from_product_receipt(
    receipt: RawArtifactReceipt,
    *,
    listing: HsafListingIdentity,
) -> HsafListingIdentity:
    provenance = receipt.provenance
    if (
        provenance.get("basename") != listing.selected_basename
        or provenance.get("remote_path") != listing.selected_remote_path
        or provenance.get("nominal_slot_time") != listing.nominal_slot_time
    ):
        raise HsafH40bNativeCorruptionError(
            "H SAF H40B productreceipt/listingselectie wijkt af"
        )
    return listing


def _validate_product_receipt(
    receipt: RawArtifactReceipt,
    *,
    job_id: str,
    identity: HsafListingIdentity,
) -> None:
    provenance = receipt.provenance
    completion = provenance.get("ftp_completion")
    declared_size = provenance.get("ftp_declared_size")
    if (
        receipt.provider != PROVIDER
        or receipt.product != NETCDF_GZIP_PRODUCT
        or receipt.fetch_id != f"{job_id}:netcdf_gzip"
        or receipt.content_type != "application/gzip"
        or not 1 <= receipt.size_bytes <= MAX_COMPRESSED_BYTES
        or receipt.upstream_id
        != (
            f"{PRODUCT_ID}:{identity.selected_basename}"
            f"|content_sha256={receipt.content_sha256}"
        )
        or provenance.get("representation") != NATIVE_REPRESENTATION
        or provenance.get("capture_stage") != NATIVE_CAPTURE_STAGE
        or provenance.get("collector") != COLLECTOR
        or provenance.get("collector_version") != COLLECTOR_VERSION
        or provenance.get("structure_inspector_version")
        != CONTAINER_INSPECTOR_VERSION
        or provenance.get("transport") != "official_plain_ftp"
        or provenance.get("ftp_host") != FTP_HOST
        or provenance.get("ftp_port") != FTP_PORT
        or provenance.get("ftp_directory") != FTP_DIRECTORY
        or provenance.get("remote_path") != identity.selected_remote_path
        or provenance.get("basename") != identity.selected_basename
        or provenance.get("nominal_slot_time") != identity.nominal_slot_time
        or provenance.get("credentials_persisted") is not False
        or provenance.get("product_id") != PRODUCT_ID
        or provenance.get("product_acronym") != PRODUCT_ACRONYM
        or provenance.get("collection_reference") != COLLECTION_REFERENCE
        or provenance.get("pum_reference") != PUM_REFERENCE
        or provenance.get("pum_revision") != PUM_REVISION
        or provenance.get("pixel_values_read") is not False
        or provenance.get("coordinate_values_read") is not False
        or provenance.get("quantitative_semantics_admitted") is not False
        or not isinstance(completion, str)
        or COMPLETION_PATTERN.fullmatch(completion) is None
        or (
            declared_size is not None
            and (
                isinstance(declared_size, bool)
                or not isinstance(declared_size, int)
                or declared_size != receipt.size_bytes
            )
        )
    ):
        raise HsafH40bNativeCorruptionError(
            "H SAF H40B productreceipt wijkt af"
        )


def _upstream_capture_id(
    receipts: Mapping[str, RawArtifactReceipt], identity: HsafListingIdentity
) -> str:
    return (
        f"{PRODUCT_ID}|filename={identity.selected_basename}"
        f"|listing={receipts['listing_nlst'].content_sha256}"
        f"|netcdf_gzip={receipts['netcdf_gzip'].content_sha256}"
    )


def _validate_capture(
    capture: RawArtifactCaptureReceipt,
    *,
    job_id: str,
    receipts: Mapping[str, RawArtifactReceipt],
    identity: HsafListingIdentity,
) -> None:
    members = {member.role: member.receipt_id for member in capture.members}
    if (
        capture.raw_store_schema_version != RAW_ARTIFACT_SCHEMA_VERSION
        or capture.provider != PROVIDER
        or capture.product != CAPTURE_PRODUCT
        or capture.fetch_id != f"{job_id}:capture"
        or capture.product_timestamp != identity.nominal_slot_time
        or capture.upstream_capture_id != _upstream_capture_id(receipts, identity)
        or members
        != {role: receipt.receipt_id for role, receipt in receipts.items()}
    ):
        raise HsafH40bNativeCorruptionError(
            "H SAF H40B complete capture wijkt af"
        )


def _source_reference(receipt: RawArtifactReceipt, *, role: str) -> dict[str, object]:
    return {
        "role": role,
        "receipt_id": receipt.receipt_id,
        "content_sha256": receipt.content_sha256,
        "receipt_sha256": receipt.receipt_sha256,
        "size_bytes": receipt.size_bytes,
        "received_at": receipt.received_at,
    }


def _result(
    capture: RawArtifactCaptureReceipt,
    *,
    receipts: Mapping[str, RawArtifactReceipt],
    selection: HsafListingIdentity,
    container: HsafNetcdfIdentity,
) -> dict[str, object]:
    return {
        "method": "hsaf_h40b_official_ftp_native_capture_v1",
        "capture_state": "captured_not_decoded",
        "weather_state": "unknown",
        "decode_status": "not_decoded",
        "pixel_values_read": False,
        "coordinate_values_read": False,
        "forecast_eligible": False,
        "operational_admission_eligible": False,
        "admission_status": ADMISSION_STATUS,
        "raw_provenance": {
            "raw_store_schema_version": RAW_ARTIFACT_SCHEMA_VERSION,
            "raw_capture_id": capture.capture_id,
            "raw_capture_manifest_sha256": capture.manifest_sha256,
            "raw_capture_receipt_sha256": capture.capture_receipt_sha256,
            "upstream_capture_id": capture.upstream_capture_id,
            "provenance_grade": "exact_ftp_nlst_and_original_netcdf_gzip",
            "raw_member_count": len(receipts),
            "product_id": PRODUCT_ID,
            "product_acronym": PRODUCT_ACRONYM,
            "collection_reference": COLLECTION_REFERENCE,
            "pum_reference": PUM_REFERENCE,
            "pum_revision": PUM_REVISION,
            "selected_basename": selection.selected_basename,
            "selected_remote_path": selection.selected_remote_path,
            "nominal_slot_time": selection.nominal_slot_time,
            "timestamp_semantics": "filename_nominal_fci_slot_not_an_accumulation_interval",
            "candidate_count": selection.candidate_count,
            "first_slot_time": selection.first_slot_time,
            "last_slot_time": selection.last_slot_time,
            "transport_security": "plain_ftp_official_source_credentials_in_transit",
            "credentials_persisted": False,
            "licence": "CC-BY-4.0_and_EUMETSAT_SAF_credit_required",
            "required_credit": "Copyright (year) EUMETSAT",
            "quantity": "instantaneous_precipitation_rate",
            "unit": RR_UNIT,
            "rr_raw_valid_range": list(RR_VALID_RANGE),
            "rr_scale_factor": RR_SCALE_FACTOR,
            "rr_add_offset": RR_ADD_OFFSET,
            "rr_missing": RR_MISSING,
            "qind_unit": QIND_UNIT,
            "qind_valid_range": list(QIND_VALID_RANGE),
            "qind_missing": QIND_MISSING,
            "observing_cycle_minutes": OBSERVING_CYCLE_MINUTES,
            "approx_nadir_resolution_km": APPROX_NADIR_RESOLUTION_KM,
            "sources": {
                role: _source_reference(receipt, role=role)
                for role, receipt in sorted(receipts.items())
            },
            "container": {
                "inspector_version": CONTAINER_INSPECTOR_VERSION,
                "object_count": container.object_count,
                "group_count": container.group_count,
                "dataset_count": container.dataset_count,
                "rr_shape": list(container.rr_shape),
                "rr_dtype": container.rr_dtype,
                "qind_shape": list(container.qind_shape),
                "qind_dtype": container.qind_dtype,
                "nx_shape": list(container.nx_shape),
                "ny_shape": list(container.ny_shape),
                "conventions": container.conventions,
                "platform": container.platform,
                "algorithm_version": container.algorithm_version,
                "parallax_correction": container.parallax_correction,
                "pixel_values_read": False,
                "coordinate_values_read": False,
            },
            "scientific_caveats": [
                "near_real_time_run_is_first_guess_due_to_delayed_mw_inputs",
                "qind_is_a_quality_index_not_a_validated_wet_dry_threshold",
                "satellite_rate_is_not_ground_radar_truth",
                "parallax_mode_off_requires_separate_operational_policy",
            ],
        },
    }


def capture_latest_hsaf_h40b(
    job: Any,
    *,
    raw_store: RawArtifactStore | None = None,
    deadline_at: float | None = None,
    _fault_injector: Callable[[str], None] | None = None,
) -> dict[str, object]:
    """Capture the exact latest official H40B file without decoding pixels."""
    job_id = _job_id(job)
    now = time.monotonic()
    if deadline_at is None:
        deadline = now + CAPTURE_TOTAL_DEADLINE_SECONDS
    elif (
        isinstance(deadline_at, bool)
        or not isinstance(deadline_at, (int, float))
        or not math.isfinite(float(deadline_at))
    ):
        raise HsafH40bNativeContractError("H SAF H40B deadline_at is ongeldig")
    else:
        deadline = min(float(deadline_at), now + CAPTURE_TOTAL_DEADLINE_SECONDS)
    store = _raw_store(raw_store)
    acquired = _CAPTURE_LOCK.acquire(timeout=_remaining(deadline))
    if not acquired:
        raise HsafH40bNativeDeadlineError(
            "H SAF H40B capturelock overschrijdt de deadline"
        )
    process_descriptor: int | None = None
    try:
        process_descriptor = _acquire_process_lock(store, deadline=deadline)
        credentials: HsafCredentials | None = None

        def credential() -> HsafCredentials:
            nonlocal credentials
            if credentials is None:
                credentials = _validated_credentials(
                    _credentials_from_environment()
                )
                _assert_credentials_absent(job_id, credentials)
            return credentials

        complete = store.load_capture_bundle_bytes(_capture_id(store, job_id))
        if complete is not None:
            capture, receipts, payloads = complete
            if set(receipts) != {"listing_nlst", "netcdf_gzip"} or set(
                payloads
            ) != set(receipts):
                raise HsafH40bNativeCorruptionError(
                    "H SAF H40B capture mist verplichte rollen"
                )
            _validate_listing_receipt(receipts["listing_nlst"], job_id=job_id)
            selection = _inspect_listing(payloads["listing_nlst"])
            _identity_from_product_receipt(
                receipts["netcdf_gzip"], listing=selection
            )
            _validate_product_receipt(
                receipts["netcdf_gzip"], job_id=job_id, identity=selection
            )
            container = _inspect_netcdf_gzip(payloads["netcdf_gzip"])
            _validate_capture(
                capture,
                job_id=job_id,
                receipts=receipts,
                identity=selection,
            )
            return _result(
                capture,
                receipts=receipts,
                selection=selection,
                container=container,
            )

        receipts: dict[str, RawArtifactReceipt] = {}
        listing_receipt, listing_payload = _load_optional(
            store, job_id=job_id, role="listing_nlst"
        )
        if listing_receipt is None:
            response = _validated_ftp_response(
                _fetch_listing(credential(), deadline=deadline),
                credentials=credential(),
                maximum_bytes=MAX_LISTING_BYTES,
                declared_size_allowed=False,
            )
            digest = hashlib.sha256(response.payload).hexdigest()
            listing_receipt = store.store(
                provider=PROVIDER,
                product=LISTING_PRODUCT,
                fetch_id=f"{job_id}:listing_nlst",
                content_type="text/plain",
                received_at=response.received_at,
                upstream_id=(
                    f"ftp:{FTP_HOST}:{FTP_PORT}{FTP_DIRECTORY}:NLST"
                    f"|content_sha256={digest}"
                ),
                provenance=_listing_provenance(response),
                payload=response.payload,
            )
            listing_payload = response.payload
            if _fault_injector is not None:
                _fault_injector("hsaf_h40b.after_listing_store")
        else:
            _validate_listing_receipt(listing_receipt, job_id=job_id)
        assert listing_payload is not None
        receipts["listing_nlst"] = listing_receipt

        # Exact NLST evidence is durable before selecting the newest filename.
        selection = _inspect_listing(listing_payload)
        _remaining(deadline)

        product_receipt, product_payload = _load_optional(
            store, job_id=job_id, role="netcdf_gzip"
        )
        if product_receipt is None:
            response = _validated_ftp_response(
                _fetch_product(
                    credential(), selection.selected_remote_path, deadline=deadline
                ),
                credentials=credential(),
                maximum_bytes=MAX_COMPRESSED_BYTES,
                declared_size_allowed=True,
            )
            digest = hashlib.sha256(response.payload).hexdigest()
            product_receipt = store.store(
                provider=PROVIDER,
                product=NETCDF_GZIP_PRODUCT,
                fetch_id=f"{job_id}:netcdf_gzip",
                content_type="application/gzip",
                received_at=response.received_at,
                upstream_id=(
                    f"{PRODUCT_ID}:{selection.selected_basename}"
                    f"|content_sha256={digest}"
                ),
                provenance=_product_provenance(response, selection),
                payload=response.payload,
            )
            product_payload = response.payload
            if _fault_injector is not None:
                _fault_injector("hsaf_h40b.after_product_store")
        else:
            _identity_from_product_receipt(product_receipt, listing=selection)
            _validate_product_receipt(
                product_receipt, job_id=job_id, identity=selection
            )
        assert product_payload is not None
        receipts["netcdf_gzip"] = product_receipt

        # Exact compressed product is durable before gzip/NetCDF inspection.
        container = _inspect_netcdf_gzip(product_payload)
        _remaining(deadline)
        if _fault_injector is not None:
            _fault_injector("hsaf_h40b.before_capture_commit")
        capture = store.commit_capture(
            provider=PROVIDER,
            product=CAPTURE_PRODUCT,
            fetch_id=f"{job_id}:capture",
            upstream_capture_id=_upstream_capture_id(receipts, selection),
            product_timestamp=selection.nominal_slot_time,
            members={role: receipt.receipt_id for role, receipt in receipts.items()},
        )
        if _fault_injector is not None:
            _fault_injector("hsaf_h40b.after_capture_commit")
        _validate_capture(
            capture,
            job_id=job_id,
            receipts=receipts,
            identity=selection,
        )
        return _result(
            capture,
            receipts=receipts,
            selection=selection,
            container=container,
        )
    finally:
        if process_descriptor is not None:
            _release_process_lock(process_descriptor)
        _CAPTURE_LOCK.release()


__all__ = [
    "ADMISSION_STATUS",
    "CAPTURE_PRODUCT",
    "LISTING_PRODUCT",
    "NETCDF_GZIP_PRODUCT",
    "HsafCredentials",
    "HsafFtpResponse",
    "HsafH40bNativeContractError",
    "HsafH40bNativeCorruptionError",
    "HsafH40bNativeDeadlineError",
    "HsafH40bNativeError",
    "HsafListingIdentity",
    "HsafNetcdfIdentity",
    "capture_latest_hsaf_h40b",
]
