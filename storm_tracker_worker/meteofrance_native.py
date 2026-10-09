"""Exact, non-operational capture of the Météo-France DPRadar HDF5 mosaic.

The official catalogue response and the selected original HDF5 response are
stored as immutable schema-3 raw artifacts before either body is inspected.
The OAuth response and every credential remain transport-only and are never
written to the raw store or returned to callers.

This module deliberately stops at container/product identity.  Météo-France
documents the physical five-minute accumulation and quality concepts, but the
public product description does not define a complete HDF5 field, CRS,
geotransform, missing-value and QC mapping.  Consequently this candidate reads
no raster values and emits no rain, dry, cell, forecast or admission fact.
It is not imported by the production worker registry or scheduler.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import Message
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import threading
import time
from typing import Any, Callable, Mapping
from urllib.parse import parse_qsl, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

import h5py

from .raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RAW_ARTIFACT_SCHEMA_VERSION,
    RawArtifactCaptureReceipt,
    RawArtifactReceipt,
    RawArtifactStore,
)


API_ORIGIN = "https://public-api.meteofrance.fr"
API_HOST = "public-api.meteofrance.fr"
CATALOG_PATH = (
    "/public/DPRadar/v1/mosaiques/METROPOLE/observations/LAME_D_EAU"
)
PRODUCT_PATH = f"{CATALOG_PATH}/produit"
PRODUCT_QUERY = "maille=500"
CATALOG_URL = f"{API_ORIGIN}{CATALOG_PATH}"
PRODUCT_URL = f"{API_ORIGIN}{PRODUCT_PATH}?{PRODUCT_QUERY}"
TOKEN_URL = "https://portail-api.meteofrance.fr/token"
TOKEN_HOST = "portail-api.meteofrance.fr"
TOKEN_PATH = "/token"

PROVIDER = "meteofrance_radar"
PRODUCT_REFERENCE = "IPRN20_C_LFPW"
CATALOG_PRODUCT = "meteofrance_dpradar_metro_lame_eau_catalog_json"
HDF5_PRODUCT = "meteofrance_iprn20_metro_lame_eau_500m_hdf5"
CAPTURE_PRODUCT = "meteofrance_dpradar_catalog_hdf5_capture"
COLLECTOR = "meteofrance_dpradar_exact_native"
COLLECTOR_VERSION = "worker-meteofrance-dpradar-exact-native-v1"
CATALOG_INSPECTOR_VERSION = "meteofrance-dpradar-catalog-v1"
HDF5_INSPECTOR_VERSION = "meteofrance-iprn20-container-v1"

GRID_ROWS = 3_472
GRID_COLUMNS = 3_472
GRID_SCALE_METRES = 500
ACCUMULATION_MINUTES = 5
DECLARED_RAW_UNIT = "0.01_mm"
DECLARED_QC_RANGE = (0, 100)
DECLARED_MISSING_CODE = 65_535
ADMISSION_STATUS = (
    "exact_raw_shadow_captured_decoder_blocked_by_incomplete_hdf5_semantics"
)

MAX_CATALOG_BYTES = 512 * 1024
MAX_HDF5_BYTES = 40 * 1024 * 1024
MAX_TOKEN_RESPONSE_BYTES = 512 * 1024
MAX_HTTP_HEADER_CHARS = 8_192
MAX_JSON_NODES = 4_096
MAX_JSON_DEPTH = 18
MAX_HDF5_OBJECTS = 512
MAX_HDF5_RANK = 4
HTTP_IO_TIMEOUT_SECONDS = 20.0
CAPTURE_TOTAL_DEADLINE_SECONDS = 75.0
STREAM_CHUNK_BYTES = 128 * 1024

RAW_SHARE_ROOT = Path("/share/storm_tracker_worker")
RAW_ARTIFACT_ROOT = RAW_SHARE_ROOT / "raw" / "meteofrance_dpradar"

_JOB_ID_RE = re.compile(r"^[^\x00-\x1f\x7f]{1,480}$")
_FILENAME_RE = re.compile(
    r"^T_(?P<reference>IPRN20_C_LFPW)_"
    r"(?P<timestamp>\d{14})\.h5$"
)
_ALLOWED_CATALOG_CONTENT_TYPES = frozenset({
    "application/json",
    "application/hal+json",
})
_ALLOWED_HDF5_CONTENT_TYPES = frozenset({
    "application/x-hdf",
    "application/x-hdf5",
    "application/octet-stream",
    "binary/octet-stream",
})
_ALLOWED_CATALOG_LINK_PATHS = frozenset({
    PRODUCT_PATH,
    PRODUCT_PATH.replace("/DPRadar/v1/", "/DPRadar/"),
})

_RAW_STORE: RawArtifactStore | None = None
_RAW_STORE_LOCK = threading.Lock()
_CAPTURE_LOCK = threading.Lock()


class MeteoFranceNativeError(RuntimeError):
    """Base candidate failure; absence/failure is never a weather state."""


class MeteoFranceNativeContractError(MeteoFranceNativeError):
    """The official transport or product identity left the pinned profile."""


class MeteoFranceNativeDeadlineError(MeteoFranceNativeError):
    """The one bounded capture deadline elapsed."""


class MeteoFranceNativeCorruptionError(MeteoFranceNativeError):
    """Durable evidence conflicts with its immutable receipt identity."""


@dataclass(frozen=True, slots=True)
class MeteoFranceHttpResponse:
    payload: bytes
    content_type: str
    received_at: float
    content_length: int
    content_disposition: str | None = None
    etag: str | None = None
    last_modified: str | None = None


@dataclass(frozen=True, slots=True)
class MeteoFranceCatalogIdentity:
    validity_time: float
    link_href: str
    media_type: str
    attribution: str


@dataclass(frozen=True, slots=True)
class MeteoFranceProductIdentity:
    filename: str
    product_reference: str
    product_timestamp: float


@dataclass(frozen=True, slots=True)
class MeteoFranceHdf5Identity:
    group_count: int
    dataset_count: int
    raster_shaped_dataset_count: int
    dataset_shapes: tuple[tuple[int, ...], ...]
    dataset_dtypes: tuple[str, ...]
    pixel_values_read: bool = False


class _RejectRedirects(HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        raise MeteoFranceNativeContractError(
            "Météo-France HTTP-redirect is niet toegestaan"
        )


def _open_without_redirects(request: Request, *, timeout: float):
    return build_opener(ProxyHandler({}), _RejectRedirects()).open(
        request, timeout=timeout
    )


def _remaining(deadline: float) -> float:
    remaining = float(deadline) - time.monotonic()
    if not math.isfinite(remaining) or remaining <= 0:
        raise MeteoFranceNativeDeadlineError(
            "Météo-France capturedeadline is verstreken"
        )
    return remaining


def _job_id(job: Any) -> str:
    value = getattr(job, "job_id", None)
    payload = getattr(job, "payload", None)
    if not isinstance(value, str) or _JOB_ID_RE.fullmatch(value) is None:
        raise MeteoFranceNativeContractError(
            "Météo-France capturejob mist een begrensde job_id"
        )
    if type(payload) is not dict or payload:
        raise MeteoFranceNativeContractError(
            "Météo-France native capture vereist een exact lege jobpayload"
        )
    return value


def _header(
    headers: Any,
    name: str,
    *,
    required: bool = False,
    maximum: int = MAX_HTTP_HEADER_CHARS,
) -> str | None:
    value = headers.get(name) if headers is not None else None
    if value is None:
        if required:
            raise MeteoFranceNativeContractError(
                f"Météo-France HTTP-header {name} ontbreekt"
            )
        return None
    result = str(value).strip()
    if (
        not result
        or len(result) > maximum
        or any(ord(char) < 0x20 or ord(char) == 0x7F for char in result)
    ):
        raise MeteoFranceNativeContractError(
            f"Météo-France HTTP-header {name} is ongeldig"
        )
    return result


def _content_type(headers: Any) -> str:
    value = _header(headers, "Content-Type", required=True, maximum=256)
    assert value is not None
    return value.split(";", 1)[0].strip().lower()


def _content_length(headers: Any, *, maximum: int) -> int | None:
    """Return a valid declared length, or ``None`` for chunked responses.

    The official DPRadar endpoint may legitimately omit ``Content-Length``.
    The streaming reader remains independently bounded, so accepting that
    standard HTTP form does not weaken the hard response-size limit.
    """
    value = _header(headers, "Content-Length", maximum=32)
    if value is None:
        return None
    if not value.isascii() or not value.isdecimal():
        raise MeteoFranceNativeContractError(
            "Météo-France Content-Length is ongeldig"
        )
    result = int(value)
    if not 1 <= result <= maximum:
        raise MeteoFranceNativeContractError(
            "Météo-France Content-Length valt buiten de harde grens"
        )
    return result


def _read_bounded(
    response: Any, *, maximum: int, declared: int | None
) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while True:
        block = response.read(min(STREAM_CHUNK_BYTES, maximum + 1 - total))
        if not block:
            break
        if not isinstance(block, bytes):
            raise MeteoFranceNativeContractError(
                "Météo-France transport leverde geen bytes"
            )
        chunks.append(block)
        total += len(block)
        if total > maximum:
            raise MeteoFranceNativeContractError(
                "Météo-France response overschrijdt de bytegrens"
            )
    payload = b"".join(chunks)
    if not payload:
        raise MeteoFranceNativeContractError(
            "Météo-France response is leeg"
        )
    if declared is not None and len(payload) != declared:
        raise MeteoFranceNativeContractError(
            "Météo-France payloadlengte wijkt af van Content-Length"
        )
    return payload


def _fixed_url(url: str, *, path: str, query: str = "") -> None:
    parts = urlsplit(url)
    if (
        parts.scheme != "https"
        or parts.hostname != API_HOST
        or parts.port is not None
        or parts.username is not None
        or parts.password is not None
        or parts.path != path
        or parts.query != query
        or parts.fragment
    ):
        raise MeteoFranceNativeContractError(
            "Météo-France URL valt buiten het vaste endpoint"
        )


def _fetch(
    url: str,
    *,
    path: str,
    query: str,
    token: str,
    maximum: int,
    allowed_content_types: frozenset[str],
    product: bool,
    deadline: float,
) -> MeteoFranceHttpResponse:
    _fixed_url(url, path=path, query=query)
    if (
        not isinstance(token, str)
        or token != token.strip()
        or not 16 <= len(token) <= 8_192
        or any(ord(char) < 0x21 or ord(char) == 0x7F for char in token)
    ):
        raise MeteoFranceNativeContractError(
            "Météo-France access token is ongeldig"
        )
    request = Request(
        url,
        headers={
            "Accept": "*/*" if product else "application/json",
            "Accept-Encoding": "identity",
            "Authorization": f"Bearer {token}",
            "User-Agent": "StormTrackerWorker/meteofrance-native-candidate-v1",
        },
    )
    with _open_without_redirects(
        request, timeout=min(HTTP_IO_TIMEOUT_SECONDS, _remaining(deadline))
    ) as response:
        if int(getattr(response, "status", 0)) != 200:
            raise MeteoFranceNativeContractError(
                "Météo-France endpoint gaf geen HTTP 200"
            )
        response_url = str(getattr(response, "url", url))
        _fixed_url(response_url, path=path, query=query)
        headers = getattr(response, "headers", None)
        if _header(headers, "Content-Encoding") is not None:
            raise MeteoFranceNativeContractError(
                "Météo-France response gebruikt transportcompressie"
            )
        content_type = _content_type(headers)
        if content_type not in allowed_content_types:
            raise MeteoFranceNativeContractError(
                "Météo-France response heeft onverwacht mediatype"
            )
        declared = _content_length(headers, maximum=maximum)
        payload = _read_bounded(response, maximum=maximum, declared=declared)
        return MeteoFranceHttpResponse(
            payload=payload,
            content_type=content_type,
            received_at=time.time(),
            content_length=len(payload),
            content_disposition=_header(headers, "Content-Disposition"),
            etag=_header(headers, "ETag", maximum=256),
            last_modified=_header(headers, "Last-Modified", maximum=128),
        )


def _fetch_catalog(token: str, *, deadline: float) -> MeteoFranceHttpResponse:
    return _fetch(
        CATALOG_URL,
        path=CATALOG_PATH,
        query="",
        token=token,
        maximum=MAX_CATALOG_BYTES,
        allowed_content_types=_ALLOWED_CATALOG_CONTENT_TYPES,
        product=False,
        deadline=deadline,
    )


def _fetch_product(token: str, *, deadline: float) -> MeteoFranceHttpResponse:
    return _fetch(
        PRODUCT_URL,
        path=PRODUCT_PATH,
        query=PRODUCT_QUERY,
        token=token,
        maximum=MAX_HDF5_BYTES,
        allowed_content_types=_ALLOWED_HDF5_CONTENT_TYPES,
        product=True,
        deadline=deadline,
    )


def _json_without_duplicate_keys(payload: bytes) -> Any:
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise MeteoFranceNativeContractError(
            "Météo-France catalogus is geen UTF-8"
        ) from exc

    def pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in values:
            if key in result:
                raise MeteoFranceNativeContractError(
                    "Météo-France catalogus bevat dubbele JSON-sleutels"
                )
            result[key] = value
        return result

    try:
        return json.loads(text, object_pairs_hook=pairs)
    except (json.JSONDecodeError, RecursionError) as exc:
        raise MeteoFranceNativeContractError(
            "Météo-France catalogus is geen begrensbare JSON"
        ) from exc


def _bounded_json(value: Any, *, depth: int = 0, counter: list[int] | None = None) -> None:
    if counter is None:
        counter = [0]
    counter[0] += 1
    if counter[0] > MAX_JSON_NODES or depth > MAX_JSON_DEPTH:
        raise MeteoFranceNativeContractError(
            "Météo-France catalogus overschrijdt de JSON-grens"
        )
    if isinstance(value, dict):
        for key, nested in value.items():
            if not isinstance(key, str) or len(key) > 256:
                raise MeteoFranceNativeContractError(
                    "Météo-France catalogus bevat ongeldige sleutel"
                )
            _bounded_json(nested, depth=depth + 1, counter=counter)
    elif isinstance(value, list):
        for nested in value:
            _bounded_json(nested, depth=depth + 1, counter=counter)
    elif isinstance(value, str):
        if len(value) > 8_192:
            raise MeteoFranceNativeContractError(
                "Météo-France catalogus bevat te lange tekst"
            )
    elif value is not None:
        if not isinstance(value, (bool, int, float)):
            raise MeteoFranceNativeContractError(
                "Météo-France catalogus bevat onbekend JSON-type"
            )
        if isinstance(value, float) and not math.isfinite(value):
            raise MeteoFranceNativeContractError(
                "Météo-France catalogus bevat niet-eindig getal"
            )


def _rfc3339(value: object) -> float:
    if not isinstance(value, str) or not 1 <= len(value) <= 64:
        raise MeteoFranceNativeContractError(
            "Météo-France validity_time ontbreekt"
        )
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        moment = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise MeteoFranceNativeContractError(
            "Météo-France validity_time is ongeldig"
        ) from exc
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise MeteoFranceNativeContractError(
            "Météo-France validity_time mist tijdzone"
        )
    timestamp = moment.timestamp()
    utc = moment.astimezone(timezone.utc)
    if (
        not math.isfinite(timestamp)
        or utc.second != 0
        or utc.microsecond != 0
        or utc.minute % ACCUMULATION_MINUTES != 0
    ):
        raise MeteoFranceNativeContractError(
            "Météo-France validity_time valt niet op vijfminutenrooster"
        )
    return timestamp


def _catalog_link(value: object) -> str:
    if not isinstance(value, str) or len(value) > 2_048:
        raise MeteoFranceNativeContractError(
            "Météo-France cataloguslink is ongeldig"
        )
    parts = urlsplit(value)
    query = parse_qsl(parts.query, keep_blank_values=True)
    if (
        parts.scheme != "https"
        or parts.hostname != API_HOST
        or parts.port is not None
        or parts.username is not None
        or parts.password is not None
        or parts.path not in _ALLOWED_CATALOG_LINK_PATHS
        or query != [("maille", "500")]
        or parts.fragment
    ):
        raise MeteoFranceNativeContractError(
            "Météo-France cataloguslink wijkt af van DPRadar maille=500"
        )
    return value


def _inspect_catalog(payload: bytes) -> MeteoFranceCatalogIdentity:
    root = _json_without_duplicate_keys(payload)
    _bounded_json(root)
    if not isinstance(root, dict) or not isinstance(root.get("links"), list):
        raise MeteoFranceNativeContractError(
            "Météo-France catalogus mist links"
        )
    attribution = root.get("attribution")
    if not isinstance(attribution, str) or "Météo-France" not in attribution:
        raise MeteoFranceNativeContractError(
            "Météo-France catalogus mist bronvermelding"
        )
    candidates: list[MeteoFranceCatalogIdentity] = []
    for link in root["links"]:
        if not isinstance(link, dict):
            continue
        href = link.get("href")
        media = link.get("type")
        validity = link.get("validity_time")
        if not isinstance(href, str) or "maille=500" not in href:
            continue
        if media not in _ALLOWED_HDF5_CONTENT_TYPES:
            raise MeteoFranceNativeContractError(
                "Météo-France 500 m-link is niet als HDF5 gemarkeerd"
            )
        candidates.append(MeteoFranceCatalogIdentity(
            validity_time=_rfc3339(validity),
            link_href=_catalog_link(href),
            media_type=media,
            attribution=attribution,
        ))
    if len(candidates) != 1:
        raise MeteoFranceNativeContractError(
            "Météo-France catalogus bevat niet exact één 500 m HDF5-product"
        )
    return candidates[0]


def _filename_from_disposition(value: str | None) -> str:
    if value is None:
        raise MeteoFranceNativeContractError(
            "Météo-France HDF5 mist Content-Disposition"
        )
    message = Message()
    message["Content-Disposition"] = value
    filename = message.get_filename()
    if not isinstance(filename, str) or _FILENAME_RE.fullmatch(filename) is None:
        raise MeteoFranceNativeContractError(
            "Météo-France HDF5-bestandsnaam wijkt af"
        )
    return filename


def _product_identity(
    response: MeteoFranceHttpResponse,
    *,
    catalog: MeteoFranceCatalogIdentity,
) -> MeteoFranceProductIdentity:
    if response.content_type not in _ALLOWED_HDF5_CONTENT_TYPES:
        raise MeteoFranceNativeContractError(
            "Météo-France productresponse is niet als HDF5 gemarkeerd"
        )
    filename = _filename_from_disposition(response.content_disposition)
    match = _FILENAME_RE.fullmatch(filename)
    assert match is not None
    moment = datetime.strptime(match.group("timestamp"), "%Y%m%d%H%M%S").replace(
        tzinfo=timezone.utc
    )
    timestamp = moment.timestamp()
    if timestamp != catalog.validity_time:
        raise MeteoFranceNativeContractError(
            "Météo-France catalogus- en bestandsdatum verschillen"
        )
    if response.content_length != len(response.payload):
        raise MeteoFranceNativeContractError(
            "Météo-France HDF5-response is niet compleet"
        )
    return MeteoFranceProductIdentity(
        filename=filename,
        product_reference=match.group("reference"),
        product_timestamp=timestamp,
    )


def _inspect_hdf5(payload: bytes) -> MeteoFranceHdf5Identity:
    if len(payload) > MAX_HDF5_BYTES or not payload.startswith(b"\x89HDF\r\n\x1a\n"):
        raise MeteoFranceNativeContractError(
            "Météo-France product is geen begrensde HDF5-container"
        )
    groups = 0
    datasets = 0
    shapes: list[tuple[int, ...]] = []
    dtypes: list[str] = []
    try:
        with h5py.File(io.BytesIO(payload), "r") as handle:
            link_scan = {"count": 0, "unsafe": False, "overflow": False}

            def visit_link(name: str) -> bool | None:
                link_scan["count"] += 1
                if link_scan["count"] > MAX_HDF5_OBJECTS:
                    link_scan["overflow"] = True
                    return True
                link = handle.get(name, getlink=True)
                if not isinstance(link, h5py.HardLink):
                    link_scan["unsafe"] = True
                    return True
                return None

            handle.visit_links(visit_link)
            if link_scan["overflow"]:
                raise MeteoFranceNativeContractError(
                    "Météo-France HDF5 bevat te veel links"
                )
            if link_scan["unsafe"]:
                raise MeteoFranceNativeContractError(
                    "Météo-France HDF5 bevat externe of zachte links"
                )

            def visit(_name: str, obj: Any) -> None:
                nonlocal groups, datasets
                if groups + datasets >= MAX_HDF5_OBJECTS:
                    raise MeteoFranceNativeContractError(
                        "Météo-France HDF5 bevat te veel objecten"
                    )
                if isinstance(obj, h5py.Group):
                    groups += 1
                elif isinstance(obj, h5py.Dataset):
                    datasets += 1
                    shape = tuple(int(part) for part in obj.shape)
                    if not 1 <= len(shape) <= MAX_HDF5_RANK or any(
                        part <= 0 for part in shape
                    ):
                        raise MeteoFranceNativeContractError(
                            "Météo-France HDF5 bevat onbegrensde datasetvorm"
                        )
                    shapes.append(shape)
                    dtypes.append(str(obj.dtype))

            handle.visititems(visit)
    except MeteoFranceNativeError:
        raise
    except (OSError, RuntimeError, ValueError) as exc:
        raise MeteoFranceNativeContractError(
            "Météo-France HDF5-container is structureel ongeldig"
        ) from exc
    raster_count = sum(
        shape == (GRID_ROWS, GRID_COLUMNS) for shape in shapes
    )
    if datasets < 1 or raster_count < 1:
        raise MeteoFranceNativeContractError(
            "Météo-France HDF5 mist een bewezen 3472×3472 raster"
        )
    return MeteoFranceHdf5Identity(
        group_count=groups,
        dataset_count=datasets,
        raster_shaped_dataset_count=raster_count,
        dataset_shapes=tuple(sorted(set(shapes))),
        dataset_dtypes=tuple(sorted(set(dtypes))),
    )


def _token_from_environment(*, deadline: float) -> str:
    static = os.getenv("STV3_METEOFRANCE_API_TOKEN", "").strip()
    application_id = os.getenv(
        "STV3_METEOFRANCE_APPLICATION_ID", ""
    ).removeprefix("Basic ").strip()
    if application_id:
        if not 16 <= len(application_id) <= 8_192:
            raise MeteoFranceNativeContractError(
                "Météo-France application credential is ongeldig"
            )
        body = urlencode({"grant_type": "client_credentials"}).encode("ascii")
        request = Request(
            TOKEN_URL,
            method="POST",
            data=body,
            headers={
                "Accept": "application/json",
                "Accept-Encoding": "identity",
                "Authorization": f"Basic {application_id}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        with _open_without_redirects(
            request, timeout=min(HTTP_IO_TIMEOUT_SECONDS, _remaining(deadline))
        ) as response:
            parts = urlsplit(str(getattr(response, "url", TOKEN_URL)))
            if (
                int(getattr(response, "status", 0)) != 200
                or parts.scheme != "https"
                or parts.hostname != TOKEN_HOST
                or parts.path != TOKEN_PATH
                or parts.query
                or parts.fragment
            ):
                raise MeteoFranceNativeContractError(
                    "Météo-France tokenendpoint wijkt af"
                )
            headers = getattr(response, "headers", None)
            if _content_type(headers) not in _ALLOWED_CATALOG_CONTENT_TYPES:
                raise MeteoFranceNativeContractError(
                    "Météo-France tokenresponse is geen JSON"
                )
            declared = _content_length(
                headers, maximum=MAX_TOKEN_RESPONSE_BYTES
            )
            payload = _read_bounded(
                response, maximum=MAX_TOKEN_RESPONSE_BYTES, declared=declared
            )
        root = _json_without_duplicate_keys(payload)
        if not isinstance(root, dict):
            raise MeteoFranceNativeContractError(
                "Météo-France tokenresponse is ongeldig"
            )
        static = root.get("access_token", "")
    if (
        not isinstance(static, str)
        or static != static.strip()
        or not 16 <= len(static) <= 8_192
        or any(ord(char) < 0x21 or ord(char) == 0x7F for char in static)
    ):
        raise MeteoFranceNativeContractError(
            "Météo-France credential ontbreekt of is ongeldig"
        )
    return static


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


def _receipt_id(store: RawArtifactStore, job_id: str, role: str) -> str:
    product = {"catalog_json": CATALOG_PRODUCT, "hdf5": HDF5_PRODUCT}[role]
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


def _catalog_provenance() -> dict[str, object]:
    return {
        "representation": NATIVE_REPRESENTATION,
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": COLLECTOR,
        "collector_version": COLLECTOR_VERSION,
        "structure_inspector_version": CATALOG_INSPECTOR_VERSION,
        "operation": "DPRadar observation catalogue",
        "request_method": "GET",
        "request_path": CATALOG_PATH,
        "authorization_persisted": False,
    }


def _hdf5_provenance(
    response: MeteoFranceHttpResponse,
    identity: MeteoFranceProductIdentity,
) -> dict[str, object]:
    return {
        "representation": NATIVE_REPRESENTATION,
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": COLLECTOR,
        "collector_version": COLLECTOR_VERSION,
        "structure_inspector_version": HDF5_INSPECTOR_VERSION,
        "operation": "DPRadar product download",
        "request_method": "GET",
        "request_path": PRODUCT_PATH,
        "request_mesh_metres": GRID_SCALE_METRES,
        "filename": identity.filename,
        "product_reference": identity.product_reference,
        "product_timestamp": identity.product_timestamp,
        "content_type": response.content_type,
        "content_length": response.content_length,
        "etag": response.etag,
        "last_modified": response.last_modified,
        "authorization_persisted": False,
        "declared_accumulation_minutes": ACCUMULATION_MINUTES,
        "declared_raw_unit": DECLARED_RAW_UNIT,
        "declared_quality_min": DECLARED_QC_RANGE[0],
        "declared_quality_max": DECLARED_QC_RANGE[1],
        "declared_missing_code": DECLARED_MISSING_CODE,
        "pixel_values_read": False,
        "quantitative_semantics_admitted": False,
    }


def _load_optional(
    store: RawArtifactStore, *, job_id: str, role: str
) -> tuple[RawArtifactReceipt | None, bytes | None]:
    loaded = store.load_receipt_bytes(_receipt_id(store, job_id, role))
    return (None, None) if loaded is None else loaded


def _validate_catalog_receipt(receipt: RawArtifactReceipt, *, job_id: str) -> None:
    if (
        receipt.provider != PROVIDER
        or receipt.product != CATALOG_PRODUCT
        or receipt.fetch_id != f"{job_id}:catalog_json"
        or receipt.content_type not in _ALLOWED_CATALOG_CONTENT_TYPES
        or not 1 <= receipt.size_bytes <= MAX_CATALOG_BYTES
        or receipt.upstream_id
        != f"{API_HOST}{CATALOG_PATH}|content_sha256={receipt.content_sha256}"
        or receipt.provenance != _catalog_provenance()
    ):
        raise MeteoFranceNativeCorruptionError(
            "Météo-France catalogusreceipt wijkt af"
        )


def _identity_from_hdf5_receipt(
    receipt: RawArtifactReceipt,
    *,
    catalog: MeteoFranceCatalogIdentity,
) -> MeteoFranceProductIdentity:
    provenance = receipt.provenance
    identity = MeteoFranceProductIdentity(
        filename=provenance.get("filename"),
        product_reference=provenance.get("product_reference"),
        product_timestamp=provenance.get("product_timestamp"),
    )
    if (
        not isinstance(identity.filename, str)
        or _FILENAME_RE.fullmatch(identity.filename) is None
        or identity.product_reference != PRODUCT_REFERENCE
        or identity.product_timestamp != catalog.validity_time
    ):
        raise MeteoFranceNativeCorruptionError(
            "Météo-France HDF5-receipt mist productidentiteit"
        )
    return identity


def _validate_hdf5_receipt(
    receipt: RawArtifactReceipt,
    *,
    job_id: str,
    identity: MeteoFranceProductIdentity,
) -> None:
    provenance = receipt.provenance
    if (
        receipt.provider != PROVIDER
        or receipt.product != HDF5_PRODUCT
        or receipt.fetch_id != f"{job_id}:hdf5"
        or receipt.content_type not in _ALLOWED_HDF5_CONTENT_TYPES
        or not 1 <= receipt.size_bytes <= MAX_HDF5_BYTES
        or receipt.upstream_id
        != (
            f"{PRODUCT_REFERENCE}:{identity.filename}"
            f"|content_sha256={receipt.content_sha256}"
        )
        or provenance.get("representation") != NATIVE_REPRESENTATION
        or provenance.get("capture_stage") != NATIVE_CAPTURE_STAGE
        or provenance.get("collector") != COLLECTOR
        or provenance.get("collector_version") != COLLECTOR_VERSION
        or provenance.get("structure_inspector_version") != HDF5_INSPECTOR_VERSION
        or provenance.get("operation") != "DPRadar product download"
        or provenance.get("request_method") != "GET"
        or provenance.get("request_path") != PRODUCT_PATH
        or provenance.get("request_mesh_metres") != GRID_SCALE_METRES
        or provenance.get("filename") != identity.filename
        or provenance.get("product_reference") != PRODUCT_REFERENCE
        or provenance.get("product_timestamp") != identity.product_timestamp
        or provenance.get("content_type") != receipt.content_type
        or provenance.get("content_length") != receipt.size_bytes
        or provenance.get("authorization_persisted") is not False
        or provenance.get("pixel_values_read") is not False
        or provenance.get("quantitative_semantics_admitted") is not False
        or provenance.get("declared_accumulation_minutes")
        != ACCUMULATION_MINUTES
        or provenance.get("declared_raw_unit") != DECLARED_RAW_UNIT
        or provenance.get("declared_quality_min") != DECLARED_QC_RANGE[0]
        or provenance.get("declared_quality_max") != DECLARED_QC_RANGE[1]
        or provenance.get("declared_missing_code") != DECLARED_MISSING_CODE
        or (
            provenance.get("etag") is not None
            and (
                not isinstance(provenance.get("etag"), str)
                or not 1 <= len(provenance["etag"]) <= 256
            )
        )
        or (
            provenance.get("last_modified") is not None
            and (
                not isinstance(provenance.get("last_modified"), str)
                or not 1 <= len(provenance["last_modified"]) <= 128
            )
        )
    ):
        raise MeteoFranceNativeCorruptionError(
            "Météo-France HDF5-receipt wijkt af"
        )


def _upstream_capture_id(
    receipts: Mapping[str, RawArtifactReceipt],
    identity: MeteoFranceProductIdentity,
) -> str:
    return (
        f"{PRODUCT_REFERENCE}|filename={identity.filename}"
        f"|catalog={receipts['catalog_json'].content_sha256}"
        f"|hdf5={receipts['hdf5'].content_sha256}"
    )


def _validate_capture(
    capture: RawArtifactCaptureReceipt,
    *,
    job_id: str,
    receipts: Mapping[str, RawArtifactReceipt],
    identity: MeteoFranceProductIdentity,
) -> None:
    members = {member.role: member.receipt_id for member in capture.members}
    if (
        capture.raw_store_schema_version != RAW_ARTIFACT_SCHEMA_VERSION
        or capture.provider != PROVIDER
        or capture.product != CAPTURE_PRODUCT
        or capture.fetch_id != f"{job_id}:capture"
        or capture.product_timestamp != identity.product_timestamp
        or capture.upstream_capture_id != _upstream_capture_id(receipts, identity)
        or members
        != {role: receipt.receipt_id for role, receipt in receipts.items()}
    ):
        raise MeteoFranceNativeCorruptionError(
            "Météo-France complete capture wijkt af"
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
    catalog: MeteoFranceCatalogIdentity,
    product: MeteoFranceProductIdentity,
    container: MeteoFranceHdf5Identity,
) -> dict[str, object]:
    return {
        "method": "meteofrance_dpradar_native_capture_v1",
        "capture_state": "captured_not_decoded",
        "weather_state": "unknown",
        "decode_status": "blocked_incomplete_hdf5_semantics",
        "pixel_values_read": False,
        "forecast_eligible": False,
        "operational_admission_eligible": False,
        "admission_status": ADMISSION_STATUS,
        "raw_provenance": {
            "raw_store_schema_version": RAW_ARTIFACT_SCHEMA_VERSION,
            "raw_capture_id": capture.capture_id,
            "raw_capture_manifest_sha256": capture.manifest_sha256,
            "raw_capture_receipt_sha256": capture.capture_receipt_sha256,
            "upstream_capture_id": capture.upstream_capture_id,
            "provenance_grade": "exact_catalog_and_original_hdf5",
            "raw_member_count": len(receipts),
            "product_reference": PRODUCT_REFERENCE,
            "product_timestamp": product.product_timestamp,
            "filename": product.filename,
            "catalog_media_type": catalog.media_type,
            "catalog_attribution": catalog.attribution,
            "authorization_persisted": False,
            "declared_grid_rows": GRID_ROWS,
            "declared_grid_columns": GRID_COLUMNS,
            "declared_grid_scale_metres": GRID_SCALE_METRES,
            "declared_accumulation_minutes": ACCUMULATION_MINUTES,
            "declared_raw_unit": DECLARED_RAW_UNIT,
            "declared_quality_range": list(DECLARED_QC_RANGE),
            "declared_missing_code": DECLARED_MISSING_CODE,
            "unresolved_semantics": [
                "hdf5_field_paths_and_quantity_identifiers",
                "native_crs_and_geotransform",
                "gain_offset_per_hdf5_field",
                "missing_versus_undetect_mapping",
                "pixel_qc_field_mapping_and_network_status_effect",
            ],
            "sources": {
                role: _source_reference(receipt, role=role)
                for role, receipt in sorted(receipts.items())
            },
            "container": {
                "inspector_version": HDF5_INSPECTOR_VERSION,
                "group_count": container.group_count,
                "dataset_count": container.dataset_count,
                "raster_shaped_dataset_count": (
                    container.raster_shaped_dataset_count
                ),
                "dataset_shapes": [list(shape) for shape in container.dataset_shapes],
                "dataset_dtypes": list(container.dataset_dtypes),
                "pixel_values_read": False,
            },
        },
    }


def capture_latest_meteofrance_hdf5(
    job: Any,
    *,
    raw_store: RawArtifactStore | None = None,
    deadline_at: float | None = None,
    _fault_injector: Callable[[str], None] | None = None,
) -> dict[str, object]:
    """Capture one exact latest mainland 500 m HDF5 product, never decode it."""
    job_id = _job_id(job)
    now = time.monotonic()
    if deadline_at is None:
        deadline = now + CAPTURE_TOTAL_DEADLINE_SECONDS
    elif (
        isinstance(deadline_at, bool)
        or not isinstance(deadline_at, (int, float))
        or not math.isfinite(float(deadline_at))
    ):
        raise MeteoFranceNativeContractError(
            "Météo-France deadline_at is ongeldig"
        )
    else:
        deadline = min(float(deadline_at), now + CAPTURE_TOTAL_DEADLINE_SECONDS)
    store = _raw_store(raw_store)
    acquired = _CAPTURE_LOCK.acquire(timeout=_remaining(deadline))
    if not acquired:
        raise MeteoFranceNativeDeadlineError(
            "Météo-France capturelock overschrijdt de deadline"
        )
    try:
        token: str | None = None

        def credential() -> str:
            nonlocal token
            if token is None:
                token = _token_from_environment(deadline=deadline)
            return token

        complete = store.load_capture_bundle_bytes(_capture_id(store, job_id))
        if complete is not None:
            capture, receipts, payloads = complete
            if set(receipts) != {"catalog_json", "hdf5"} or set(payloads) != set(receipts):
                raise MeteoFranceNativeCorruptionError(
                    "Météo-France capture mist verplichte rollen"
                )
            _validate_catalog_receipt(receipts["catalog_json"], job_id=job_id)
            catalog = _inspect_catalog(payloads["catalog_json"])
            product = _identity_from_hdf5_receipt(
                receipts["hdf5"], catalog=catalog
            )
            _validate_hdf5_receipt(
                receipts["hdf5"], job_id=job_id, identity=product
            )
            container = _inspect_hdf5(payloads["hdf5"])
            _validate_capture(
                capture,
                job_id=job_id,
                receipts=receipts,
                identity=product,
            )
            return _result(
                capture,
                receipts=receipts,
                catalog=catalog,
                product=product,
                container=container,
            )

        receipts: dict[str, RawArtifactReceipt] = {}
        catalog_receipt, catalog_payload = _load_optional(
            store, job_id=job_id, role="catalog_json"
        )
        if catalog_receipt is None:
            response = _fetch_catalog(credential(), deadline=deadline)
            digest = hashlib.sha256(response.payload).hexdigest()
            catalog_receipt = store.store(
                provider=PROVIDER,
                product=CATALOG_PRODUCT,
                fetch_id=f"{job_id}:catalog_json",
                content_type=response.content_type,
                received_at=response.received_at,
                upstream_id=(
                    f"{API_HOST}{CATALOG_PATH}|content_sha256={digest}"
                ),
                provenance=_catalog_provenance(),
                payload=response.payload,
            )
            catalog_payload = response.payload
            if _fault_injector is not None:
                _fault_injector("meteofrance.after_catalog_store")
        else:
            _validate_catalog_receipt(catalog_receipt, job_id=job_id)
        assert catalog_payload is not None
        receipts["catalog_json"] = catalog_receipt

        # Selection happens only after the exact catalogue body is durable.
        catalog = _inspect_catalog(catalog_payload)
        _remaining(deadline)

        hdf5_receipt, hdf5_payload = _load_optional(
            store, job_id=job_id, role="hdf5"
        )
        if hdf5_receipt is None:
            response = _fetch_product(credential(), deadline=deadline)
            product = _product_identity(response, catalog=catalog)
            digest = hashlib.sha256(response.payload).hexdigest()
            hdf5_receipt = store.store(
                provider=PROVIDER,
                product=HDF5_PRODUCT,
                fetch_id=f"{job_id}:hdf5",
                content_type=response.content_type,
                received_at=response.received_at,
                upstream_id=(
                    f"{PRODUCT_REFERENCE}:{product.filename}"
                    f"|content_sha256={digest}"
                ),
                provenance=_hdf5_provenance(response, product),
                payload=response.payload,
            )
            hdf5_payload = response.payload
            if _fault_injector is not None:
                _fault_injector("meteofrance.after_hdf5_store")
        else:
            product = _identity_from_hdf5_receipt(
                hdf5_receipt, catalog=catalog
            )
            _validate_hdf5_receipt(
                hdf5_receipt, job_id=job_id, identity=product
            )
        assert hdf5_payload is not None
        receipts["hdf5"] = hdf5_receipt

        # Container metadata is inspected only after the exact HDF5 is durable.
        container = _inspect_hdf5(hdf5_payload)
        _remaining(deadline)
        if _fault_injector is not None:
            _fault_injector("meteofrance.before_capture_commit")
        capture = store.commit_capture(
            provider=PROVIDER,
            product=CAPTURE_PRODUCT,
            fetch_id=f"{job_id}:capture",
            upstream_capture_id=_upstream_capture_id(receipts, product),
            product_timestamp=product.product_timestamp,
            members={role: receipt.receipt_id for role, receipt in receipts.items()},
        )
        if _fault_injector is not None:
            _fault_injector("meteofrance.after_capture_commit")
        _validate_capture(
            capture,
            job_id=job_id,
            receipts=receipts,
            identity=product,
        )
        return _result(
            capture,
            receipts=receipts,
            catalog=catalog,
            product=product,
            container=container,
        )
    finally:
        _CAPTURE_LOCK.release()


__all__ = [
    "ADMISSION_STATUS",
    "CAPTURE_PRODUCT",
    "CATALOG_PRODUCT",
    "HDF5_PRODUCT",
    "MeteoFranceCatalogIdentity",
    "MeteoFranceHdf5Identity",
    "MeteoFranceHttpResponse",
    "MeteoFranceNativeContractError",
    "MeteoFranceNativeCorruptionError",
    "MeteoFranceNativeDeadlineError",
    "MeteoFranceNativeError",
    "MeteoFranceProductIdentity",
    "capture_latest_meteofrance_hdf5",
]
