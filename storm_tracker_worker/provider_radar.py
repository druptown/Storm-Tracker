"""Worker-owned image radar providers.

Home Assistant sends only small configuration values.  This module owns the
HTTP requests, image decoding, connected-component extraction and overlay
construction, then returns a checksummed artifact through ``/share``.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import tarfile
import threading
import time
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen

import numpy as np
from PIL import Image
from pyproj import Transformer

from .artifacts import write_json_artifact
from .raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RawArtifactCaptureReceipt,
    RawArtifactReceipt,
    RawArtifactStore,
)
from .radar import (
    _contains,
    _download_bounded,
    _grid_polygons,
    _intensity,
    _pixel_components,
    parse_odim_rainfall,
)

RAINVIEWER_MANIFEST = "https://api.rainviewer.com/public/weather-maps.json"
RAINVIEWER_ZOOM = 5
RAINVIEWER_TILE_SIZE = 256
RAINVIEWER_FRAME_MAX_AGE = 20 * 60
KMI_BASE_URL = "https://app.meteo.be/services/appv4/"
KMI_SECRET = "r9EnW374jkJ9acc"
KMI_BOUNDS = (-2.5, 46.5, 10.5, 53.0)
KMI_RAW_SHARE_ROOT = Path("/share/storm_tracker_worker")
KMI_RAW_ARTIFACT_ROOT = (
    KMI_RAW_SHARE_ROOT / "raw" / "kmi_appv4_visualisation"
)
KMI_PRODUCT = "kmi_appv4_visualisation"
KMI_FORECAST_PRODUCT = "kmi_appv4_forecast_manifest"
KMI_IMAGE_PRODUCT = "kmi_appv4_visualisation_png"
KMI_CAPTURE_PRODUCT = "kmi_appv4_visualisation_capture"
KMI_SEMANTIC_CLASS = "categorical_rendered_image"
KMI_RAW_COLLECTOR = "kmi_appv4_visualisation"
KMI_RAW_COLLECTOR_VERSION = "worker-kmi-appv4-exact-raw-v1"
KMI_FORECAST_PARSER_VERSION = "kmi-appv4-forecast-json-parser-v1"
KMI_IMAGE_PARSER_VERSION = "kmi-appv4-categorical-png-parser-v1"
KMI_PROVENANCE_GRADE = "exact_raw"
KMI_PROVENANCE_SCOPE = "upstream_bytes_only"
KMI_ADMISSION_STATUS = "uncertified_visualisation_only"
KMI_LEGACY_DECODER_QUALITY = 0.95
_KMI_RAW_STORE: RawArtifactStore | None = None
_KMI_RAW_STORE_LOCK = threading.Lock()
KNMI_WMS_BASE = "https://api.dataplatform.knmi.nl/wms/adaguc-server"
KNMI_BOUNDS = (0.0, 48.895, 10.856, 55.974)
MAX_JSON_BYTES = 4 * 1024 * 1024
MAX_IMAGE_BYTES = 8 * 1024 * 1024
AEMET_URL = "https://www.aemet.es/es/api-eltiempo/radar/download/compo"
AEMET_FRAME_RE = re.compile(r"^down_radw(\d{12})_4326\.tif$")
DPC_API = "https://radar-api.protezionecivile.it"
DPC_CRS = "+proj=lcc +lat_0=42 +lon_0=12.5 +lat_1=42 +lat_2=42 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
METEOFRANCE_PRODUCT = "https://public-api.meteofrance.fr/public/DPRadar/v1/mosaiques/METROPOLE/observations/LAME_D_EAU/produit?maille=500"
METEOFRANCE_TOKEN = "https://portail-api.meteofrance.fr/token"


def _download_with_headers(
    url: str,
    *,
    maximum_bytes: int,
    headers: dict[str, str] | None = None,
) -> bytes:
    if not url.startswith("https://"):
        raise ValueError("radarprovider accepteert uitsluitend HTTPS")
    request_headers = {
        "Accept-Encoding": "identity",
        "User-Agent": "StormTrackerWorker/1.16",
        **(headers or {}),
    }
    request = Request(url, headers=request_headers)
    with urlopen(request, timeout=25) as response:
        encoding = str(response.headers.get("Content-Encoding") or "identity")
        if encoding.lower() != "identity":
            raise ValueError("onverwachte transportcompressie")
        declared = response.headers.get("Content-Length")
        if declared is not None and int(declared) > maximum_bytes:
            raise ValueError("providerresponse overschrijdt bytecontract")
        value = response.read(maximum_bytes + 1)
        if len(value) > maximum_bytes:
            raise ValueError("providerresponse overschrijdt bytecontract")
        if declared is not None and len(value) != int(declared):
            raise ValueError("providerresponse is onvolledig")
        from .thin_raw_archive import capture_received
        capture_received(str(response.geturl()), value, received_at=time.time(),
                         content_type=response.headers.get('Content-Type'), http_status=getattr(response,'status',None))
        return value


@dataclass(frozen=True, slots=True)
class _KmiHttpResponse:
    """Exact KMI response bytes plus transport facts captured after read."""

    payload: bytes
    content_type: str
    received_at: float


def _download_kmi_response(
    url: str,
    *,
    maximum_bytes: int,
    headers: dict[str, str],
) -> _KmiHttpResponse:
    """Read one bounded KMI response without decoding its body."""
    if not url.startswith("https://"):
        raise ValueError("KMI-provider accepteert uitsluitend HTTPS")
    request = Request(
        url,
        headers={
            "Accept-Encoding": "identity",
            "User-Agent": "StormTrackerWorker/1.16",
            **headers,
        },
    )
    with urlopen(request, timeout=25) as response:
        encoding = str(response.headers.get("Content-Encoding") or "identity")
        if encoding.lower() != "identity":
            raise ValueError("onverwachte KMI-transportcompressie")
        declared = response.headers.get("Content-Length")
        if declared is not None and int(declared) > maximum_bytes:
            raise ValueError("KMI-response overschrijdt bytecontract")
        payload = response.read(maximum_bytes + 1)
        received_at = time.time()
        if len(payload) > maximum_bytes:
            raise ValueError("KMI-response overschrijdt bytecontract")
        if declared is not None and len(payload) != int(declared):
            raise ValueError("KMI-response is onvolledig")
        content_type = str(response.headers.get("Content-Type") or "")
        content_type = content_type.split(";", 1)[0].strip().lower()
        if not content_type:
            raise ValueError("KMI-response mist Content-Type")
        return _KmiHttpResponse(
            payload=payload,
            content_type=content_type,
            received_at=received_at,
        )


def _post(
    url: str,
    *,
    body: bytes,
    maximum_bytes: int,
    headers: dict[str, str],
) -> bytes:
    request = Request(
        url,
        data=body,
        method="POST",
        headers={
            "Accept-Encoding": "identity",
            "User-Agent": "StormTrackerWorker/1.16",
            **headers,
        },
    )
    with urlopen(request, timeout=25) as response:
        declared = response.headers.get("Content-Length")
        value = response.read(maximum_bytes + 1)
        if len(value) > maximum_bytes:
            raise ValueError("providerresponse overschrijdt bytecontract")
        if declared is not None and len(value) != int(declared):
            raise ValueError("providerresponse is onvolledig")
        return value


def _image_rgba(payload: bytes) -> np.ndarray:
    with Image.open(io.BytesIO(payload)) as image:
        return np.asarray(image.convert("RGBA"), dtype=np.uint8)


def _runs(
    grid: np.ndarray,
    corner_to_latlon,
    *,
    max_run: int = 16,
    include_point=None,
    maximum_results: int | None = None,
) -> list[dict]:
    result = []
    height, width = grid.shape
    for row in range(height):
        column = 0
        while column < width:
            intensity = int(grid[row, column])
            if intensity <= 0:
                column += 1
                continue
            center = corner_to_latlon(row + 0.5, column + 0.5)
            if (
                (
                    include_point is not None
                    and not include_point(float(center[0]), float(center[1]))
                )
            ):
                column += 1
                continue
            end = column + 1
            while (
                end < min(width, column + max_run)
                and int(grid[row, end]) == intensity
                and (
                    include_point is None
                    or include_point(
                        *corner_to_latlon(row + 0.5, end + 0.5)
                    )
                )
            ):
                end += 1
            result.append({
                "intensity": intensity,
                "ring": (
                    corner_to_latlon(row, column),
                    corner_to_latlon(row, end),
                    corner_to_latlon(row + 1, end),
                    corner_to_latlon(row + 1, column),
                ),
            })
            if maximum_results is not None and len(result) > maximum_results:
                raise ValueError("radaroverlay overschrijdt runbudget")
            column = end
    return result


def _component_observations(
    grid: np.ndarray,
    *,
    source: str,
    timestamp: float,
    quality: float,
    corner_to_latlon,
    minimum_pixels: int = 1,
    include_point=None,
    maximum_results: int | None = None,
    quality_grid: np.ndarray | None = None,
) -> list[dict]:
    observations = []
    components = [
        pixels
        for pixels in _pixel_components(
            grid,
            maximum_components=(
                maximum_results if minimum_pixels <= 1 else None
            ),
        )
        if len(pixels) >= max(1, int(minimum_pixels))
    ]
    if maximum_results is not None and len(components) > maximum_results:
        raise ValueError("radarframe overschrijdt componentbudget")
    for index, pixels in enumerate(components):
        center_row = sum(row + 0.5 for row, _ in pixels) / len(pixels)
        center_column = sum(column + 0.5 for _, column in pixels) / len(pixels)
        lat, lon = corner_to_latlon(center_row, center_column)
        if include_point is not None and not include_point(float(lat), float(lon)):
            continue
        polygons = tuple(
            tuple(
                tuple(corner_to_latlon(row, column) for row, column in ring)
                for ring in polygon
            )
            for polygon in _grid_polygons(pixels)
        )
        boundary = polygons[0][0] if polygons else ()
        # Benader het aardoppervlak van een native pixel met de kruisproduct-
        # oppervlakte van beide afzonderlijke rasterassen. Dit blijft correct
        # bij gedraaide/geostationaire rasters, waar een diagonale lat/lon-
        # sprong de oppervlakte sterk kan vertekenen.
        lat_00, lon_00 = corner_to_latlon(center_row, center_column)
        lat_x, lon_x = corner_to_latlon(center_row, center_column + 1)
        lat_y, lon_y = corner_to_latlon(center_row + 1, center_column)

        def local_vector(point_lat, point_lon):
            delta_lon = ((float(point_lon) - float(lon_00) + 180.0) % 360.0) - 180.0
            return (
                delta_lon
                * 111.320
                * math.cos(math.radians((float(point_lat) + float(lat_00)) / 2.0)),
                (float(point_lat) - float(lat_00)) * 110.574,
            )

        east_x, north_x = local_vector(lat_x, lon_x)
        east_y, north_y = local_vector(lat_y, lon_y)
        pixel_area_km2 = abs(east_x * north_y - north_x * east_y)
        area_km2 = len(pixels) * pixel_area_km2
        component_id = f"{source}:{timestamp:.0f}:c{index}"
        component_quality = float(quality)
        if quality_grid is not None:
            component_quality = float(np.mean([
                quality_grid[row, column] for row, column in pixels
            ]))
        observations.append({
            "obs_type": "radar",
            "lat": float(lat),
            "lon": float(lon),
            "timestamp": float(timestamp),
            "intensity": max(int(grid[row, column]) for row, column in pixels),
            "area_km2": float(area_km2),
            "quality": component_quality,
            "footprint_points": boundary,
            "footprint_polygons": polygons,
            "radar_cell_id": component_id,
            "parent_system_id": component_id,
            "parent_area_km2": float(area_km2),
            "parent_footprint_points": boundary,
            "parent_footprint_polygons": polygons,
            "source": source,
        })
    return observations


def _tile(lat: float, lon: float, zoom: int) -> tuple[int, int]:
    count = 2**zoom
    x = int((lon + 180.0) / 360.0 * count)
    latitude = math.radians(lat)
    y = int(
        (1.0 - math.log(math.tan(latitude) + 1.0 / math.cos(latitude)) / math.pi)
        / 2.0
        * count
    )
    return x, y


def _tile_bounds(x: int, y: int, zoom: int):
    count = 2**zoom
    return (
        math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / count)))),
        math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / count)))),
        x / count * 360.0 - 180.0,
        (x + 1) / count * 360.0 - 180.0,
    )


def _rainviewer_grid(rgba: np.ndarray) -> np.ndarray:
    rgb = rgba[..., :3].astype(np.int16)
    alpha = rgba[..., 3]
    maximum = rgb.max(axis=2)
    minimum = rgb.min(axis=2)
    saturation = maximum - minimum
    result = np.zeros(alpha.shape, dtype=np.uint8)
    translucent = (alpha >= 130) & (alpha < 255)
    estimated_dbz = 8 + np.rint((alpha.astype(np.float64) - 130.0) * 6.0 / 60.0)
    result[translucent] = np.clip(
        1 + np.maximum(0, estimated_dbz[translucent] - 8).astype(np.int16) // 6,
        1,
        2,
    ).astype(np.uint8)
    opaque_wet = (alpha == 255) & ~((saturation < 40) & (minimum < 240))
    result[opaque_wet & (maximum < 250)] = 3
    result[opaque_wet & (maximum >= 250)] = 8
    return result


def rainviewer_fetch(job) -> dict:
    regions = tuple(job.payload.get("regions") or ())
    manifest = json.loads(
        _download_with_headers(
            RAINVIEWER_MANIFEST, maximum_bytes=MAX_JSON_BYTES
        ).decode("utf-8")
    )
    frames = manifest.get("radar", {}).get("past", [])
    if not frames:
        raise ValueError("RainViewer-manifest bevat geen radarframe")
    latest = frames[-1]
    timestamp = float(latest["time"])
    if time.time() - timestamp > RAINVIEWER_FRAME_MAX_AGE:
        raise ValueError("RainViewer-frame is te oud")
    host = str(manifest.get("host") or "https://tilecache.rainviewer.com")
    path = str(latest["path"])
    observations = []
    runs = []
    seen_tiles: set[tuple[int, int]] = set()
    for region in regions:
        center_x, center_y = _tile(
            float(region["center_lat"]), float(region["center_lon"]),
            RAINVIEWER_ZOOM,
        )
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                tx, ty = center_x + dx, center_y + dy
                if (tx, ty) in seen_tiles:
                    continue
                seen_tiles.add((tx, ty))
                url = f"{host}{path}/256/{RAINVIEWER_ZOOM}/{tx}/{ty}/2/1_1.png"
                try:
                    rgba = _image_rgba(
                        _download_with_headers(url, maximum_bytes=MAX_IMAGE_BYTES)
                    )
                except Exception:
                    continue
                grid = _rainviewer_grid(rgba)
                lat_top, lat_bottom, lon_left, lon_right = _tile_bounds(
                    tx, ty, RAINVIEWER_ZOOM
                )

                def corner(row, column):
                    return (
                        lat_top - row / grid.shape[0] * (lat_top - lat_bottom),
                        lon_left + column / grid.shape[1] * (lon_right - lon_left),
                    )

                tile_source = f"rainviewer:t{tx}_{ty}"
                values = _component_observations(
                    grid,
                    source=tile_source,
                    timestamp=timestamp,
                    quality=0.70,
                    corner_to_latlon=corner,
                )
                for value in values:
                    # Preserve the public provider name while keeping tile IDs.
                    value["source"] = "rainviewer"
                    value["radar_cell_id"] = value["radar_cell_id"].replace(
                        tile_source, f"rainviewer:{timestamp:.0f}:t{tx}_{ty}", 1
                    )
                    value["parent_system_id"] = value["radar_cell_id"]
                observations.extend(values)
                runs.extend(_runs(grid, corner))
    result = {
        "provider": "rainviewer",
        "frame_timestamp": timestamp,
        "observations": observations,
        "overlay": {"source": "rainviewer", "timestamp": timestamp, "runs": runs},
        "diagnostics": {"tiles": len(seen_tiles), "regions": len(regions)},
    }
    return {
        **write_json_artifact(job.job_id, result),
        "provider": "rainviewer",
        "frame_timestamp": timestamp,
        "observation_count": len(observations),
        "diagnostics": result["diagnostics"],
    }


def _kmi_raw_store(injected: RawArtifactStore | None = None) -> RawArtifactStore:
    """Return the isolated KMI raw store, with explicit injection for tests."""
    if injected is not None:
        injected.initialize()
        return injected
    share_root = KMI_RAW_SHARE_ROOT.resolve(strict=False)
    raw_root = KMI_RAW_ARTIFACT_ROOT.resolve(strict=False)
    if raw_root == share_root or share_root not in raw_root.parents:
        raise RuntimeError("KMI raw-artifactpad ontsnapt uit worker-share")
    global _KMI_RAW_STORE
    with _KMI_RAW_STORE_LOCK:
        if _KMI_RAW_STORE is None:
            candidate = RawArtifactStore(raw_root)
            candidate.initialize()
            _KMI_RAW_STORE = candidate
        return _KMI_RAW_STORE


def _kmi_location_context(lat: float, lon: float) -> str:
    return f"{lat:.6f},{lon:.6f}"


def _kmi_uri_path_sha256(uri: str) -> str:
    parsed = urlsplit(uri)
    if parsed.scheme.lower() != "https" or not parsed.netloc or not parsed.path:
        raise ValueError("KMI-frame-URI is geen geldige HTTPS-resource")
    return hashlib.sha256(parsed.path.encode("utf-8")).hexdigest()


def _kmi_native_provenance(
    *,
    product: str,
    parser_version: str,
    semantic_class: str,
    location_context: str,
    extra: dict[str, object] | None = None,
) -> dict[str, object]:
    value: dict[str, object] = {
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": KMI_RAW_COLLECTOR,
        "collector_version": KMI_RAW_COLLECTOR_VERSION,
        "location_context": location_context,
        "parser_version": parser_version,
        "product": product,
        "quantitative_semantics_eligible": False,
        "representation": NATIVE_REPRESENTATION,
        "semantic_class": semantic_class,
    }
    if extra:
        value.update(extra)
    return value


def _kmi_raw_source_reference(
    role: str, receipt: RawArtifactReceipt,
) -> dict[str, object]:
    provenance = receipt.provenance
    return {
        "role": role,
        "raw_receipt_id": receipt.receipt_id,
        "raw_content_sha256": receipt.content_sha256,
        "fetch_id": receipt.fetch_id,
        "upstream_record_id": receipt.upstream_id,
        "received_at": receipt.received_at,
        "content_type": receipt.content_type,
        "parser_version": provenance["parser_version"],
        "collector_version": provenance["collector_version"],
        "provenance_grade": KMI_PROVENANCE_GRADE,
        "provenance_scope": KMI_PROVENANCE_SCOPE,
        "semantic_class": provenance["semantic_class"],
        "quantitative_semantics_eligible": False,
    }


def _kmi_capture_provenance(
    capture: RawArtifactCaptureReceipt,
    *,
    forecast_receipt: RawArtifactReceipt,
    image_receipt: RawArtifactReceipt,
) -> dict[str, object]:
    return {
        "raw_capture_id": capture.capture_id,
        "raw_capture_manifest_sha256": capture.manifest_sha256,
        "raw_capture_fetch_id": capture.fetch_id,
        "upstream_capture_id": capture.upstream_capture_id,
        "product_timestamp": capture.product_timestamp,
        "collector_version": KMI_RAW_COLLECTOR_VERSION,
        "parser_versions": {
            "forecast_json": KMI_FORECAST_PARSER_VERSION,
            "radar_png": KMI_IMAGE_PARSER_VERSION,
        },
        "provenance_grade": KMI_PROVENANCE_GRADE,
        "provenance_scope": KMI_PROVENANCE_SCOPE,
        "raw_sources": {
            "forecast_json": _kmi_raw_source_reference(
                "forecast_json", forecast_receipt
            ),
            "radar_png": _kmi_raw_source_reference("radar_png", image_receipt),
        },
    }


def _kmi_radar_cycle_bridge(
    capture: RawArtifactCaptureReceipt,
) -> dict[str, object]:
    """Expose the v1 set-provenance gap without promoting one member."""
    return {
        "status": "not_bridged_linked_capture",
        "current_contract": "stv3-radar-cycle-request-v1",
        "required_representation": "raw_capture_set",
        "raw_capture_id": capture.capture_id,
        "raw_capture_manifest_sha256": capture.manifest_sha256,
        "member_receipt_ids": [
            member.receipt_id for member in capture.members
        ],
        "single_receipt_complete_set": False,
        "exact_raw_artifacts_complete_in_v1": False,
    }


def _kmi_key(service: str) -> str:
    date = datetime.now().strftime("%d/%m/%Y")
    return hashlib.md5(f"{KMI_SECRET};{service};{date}".encode()).hexdigest()


def _kmi_grid(rgba: np.ndarray) -> np.ndarray:
    palette = np.asarray([
        (144, 238, 144), (0, 200, 100), (0, 150, 255), (0, 0, 255),
        (255, 255, 0), (255, 150, 0), (255, 0, 0), (180, 0, 180),
    ], dtype=np.int16)
    rgb = rgba[..., :3].astype(np.int16)
    distances = ((rgb[..., None, :] - palette) ** 2).sum(axis=3)
    nearest = distances.argmin(axis=2)
    minimum = distances.min(axis=2)
    result = (nearest + 1).astype(np.uint8)
    result[(rgba[..., 3] < 128) | (minimum >= 20000)] = 0
    return result


def kmi_fetch(
    job, *, raw_store: RawArtifactStore | None = None,
) -> dict:
    lat = float(job.payload["center_lat"])
    lon = float(job.payload["center_lon"])
    job_id = str(job.job_id)
    location_context = _kmi_location_context(lat, lon)
    store = _kmi_raw_store(raw_store)
    query = urlencode({
        "s": "getForecasts", "k": _kmi_key("getForecasts"),
        "lat": lat, "long": lon,
    })
    headers = {"User-Agent": "be.meteo.app"}
    forecast_response = _download_kmi_response(
        f"{KMI_BASE_URL}?{query}", maximum_bytes=MAX_JSON_BYTES, headers=headers
    )
    forecast_content_sha256 = hashlib.sha256(
        forecast_response.payload
    ).hexdigest()
    request_utc_date = datetime.fromtimestamp(
        forecast_response.received_at, timezone.utc
    ).date().isoformat()
    forecast_upstream_record_id = (
        f"{KMI_FORECAST_PRODUCT}|request_utc_date={request_utc_date}"
        f"|location={location_context}"
        f"|payload_sha256={forecast_content_sha256}"
    )
    forecast_receipt = store.store(
        provider="kmi",
        product=KMI_FORECAST_PRODUCT,
        fetch_id=f"{job_id}:forecast_json",
        content_type=forecast_response.content_type,
        received_at=forecast_response.received_at,
        upstream_id=forecast_upstream_record_id,
        provenance=_kmi_native_provenance(
            product=KMI_FORECAST_PRODUCT,
            parser_version=KMI_FORECAST_PARSER_VERSION,
            semantic_class="forecast_manifest",
            location_context=location_context,
            extra={"request_utc_date": request_utc_date},
        ),
        payload=forecast_response.payload,
    )
    forecast = json.loads(forecast_response.payload.decode("utf-8"))
    sequence = forecast.get("animation", {}).get("sequence", [])
    if not sequence:
        raise ValueError("KMI-animatie bevat geen frames")
    now = datetime.now().astimezone()
    historical = [item for item in sequence if datetime.fromisoformat(item["time"]) <= now]
    latest = (historical or sequence)[-1]
    timestamp = datetime.fromisoformat(latest["time"]).timestamp()
    latest_uri = str(latest["uri"])
    uri_path_sha256 = _kmi_uri_path_sha256(latest_uri)
    image_response = _download_kmi_response(
        latest_uri, maximum_bytes=MAX_IMAGE_BYTES, headers=headers
    )
    image_content_sha256 = hashlib.sha256(image_response.payload).hexdigest()
    image_upstream_record_id = (
        f"{KMI_IMAGE_PRODUCT}|sequence_time={timestamp:.6f}"
        f"|uri_path_sha256={uri_path_sha256}"
        f"|payload_sha256={image_content_sha256}"
        f"|location={location_context}"
    )
    image_receipt = store.store(
        provider="kmi",
        product=KMI_IMAGE_PRODUCT,
        fetch_id=f"{job_id}:radar_png",
        content_type=image_response.content_type,
        received_at=image_response.received_at,
        upstream_id=image_upstream_record_id,
        provenance=_kmi_native_provenance(
            product=KMI_IMAGE_PRODUCT,
            parser_version=KMI_IMAGE_PARSER_VERSION,
            semantic_class=KMI_SEMANTIC_CLASS,
            location_context=location_context,
            extra={
                "product_timestamp": timestamp,
                "uri_path_sha256": uri_path_sha256,
            },
        ),
        payload=image_response.payload,
    )
    upstream_capture_id = (
        f"{KMI_CAPTURE_PRODUCT}|sequence_time={timestamp:.6f}"
        f"|uri_path_sha256={uri_path_sha256}"
        f"|forecast_sha256={forecast_content_sha256}"
        f"|payload_sha256={image_content_sha256}"
        f"|location={location_context}"
    )
    capture = store.commit_capture(
        provider="kmi",
        product=KMI_CAPTURE_PRODUCT,
        fetch_id=f"{job_id}:capture",
        upstream_capture_id=upstream_capture_id,
        product_timestamp=timestamp,
        members={
            "forecast_json": forecast_receipt.receipt_id,
            "radar_png": image_receipt.receipt_id,
        },
    )
    raw_provenance = _kmi_capture_provenance(
        capture,
        forecast_receipt=forecast_receipt,
        image_receipt=image_receipt,
    )
    radar_cycle_bridge = _kmi_radar_cycle_bridge(capture)
    rgba = _image_rgba(image_response.payload)
    grid = _kmi_grid(rgba)
    left, bottom, right, top = KMI_BOUNDS

    def corner(row, column):
        return (
            top - row / grid.shape[0] * (top - bottom),
            left + column / grid.shape[1] * (right - left),
        )

    # The app-rendered colour image is useful as a visual overlay, but its
    # palette has no certified quantitative precipitation contract.  Never
    # emit these decoded categories as operational radar observations.
    observations: list[dict] = []
    current = forecast.get("obs") or {}
    overlay = {
        "source": "kmi",
        "product": KMI_PRODUCT,
        "semantic_class": KMI_SEMANTIC_CLASS,
        "intensity_semantics": "legacy_palette_category",
        "quantitative_semantics_eligible": False,
        "operational_use": "visualisation_only",
        "operational_admission_eligible": False,
        "admission_status": KMI_ADMISSION_STATUS,
        "timestamp": timestamp,
        "runs": _runs(grid, corner),
        "provenance_grade": KMI_PROVENANCE_GRADE,
        "provenance_scope": KMI_PROVENANCE_SCOPE,
        "raw_provenance": raw_provenance,
    }
    result = {
        "provider": "kmi", "product": KMI_PRODUCT,
        "semantic_class": KMI_SEMANTIC_CLASS,
        "intensity_semantics": "legacy_palette_category",
        "quantitative_semantics_eligible": False,
        "operational_use": "visualisation_only",
        "operational_admission_eligible": False,
        "admission_status": KMI_ADMISSION_STATUS,
        "legacy_decoder_quality_constant": KMI_LEGACY_DECODER_QUALITY,
        "legacy_decoder_quality_operational": False,
        "frame_timestamp": timestamp,
        "observations": observations,
        "overlay": overlay,
        "provenance_grade": KMI_PROVENANCE_GRADE,
        "provenance_scope": KMI_PROVENANCE_SCOPE,
        "raw_provenance": raw_provenance,
        "radar_cycle_bridge": radar_cycle_bridge,
        "diagnostics": {
            "product": KMI_PRODUCT,
            "semantic_class": KMI_SEMANTIC_CLASS,
            "raw_capture_id": capture.capture_id,
            "admission_status": KMI_ADMISSION_STATUS,
            "operational_admission_eligible": False,
            "radar_cycle_bridge": radar_cycle_bridge,
            "ww": current.get("ww"),
            "weather_timestamp": current.get("timestamp"), "temperature": current.get("temp"),
        },
    }
    return {
        **write_json_artifact(job_id, result), "provider": "kmi",
        "product": KMI_PRODUCT,
        "semantic_class": KMI_SEMANTIC_CLASS,
        "quantitative_semantics_eligible": False,
        "operational_use": "visualisation_only",
        "operational_admission_eligible": False,
        "admission_status": KMI_ADMISSION_STATUS,
        "legacy_decoder_quality_constant": KMI_LEGACY_DECODER_QUALITY,
        "legacy_decoder_quality_operational": False,
        "provenance_grade": KMI_PROVENANCE_GRADE,
        "provenance_scope": KMI_PROVENANCE_SCOPE,
        "raw_provenance": raw_provenance,
        "radar_cycle_bridge": radar_cycle_bridge,
        "frame_timestamp": timestamp, "observation_count": len(observations),
        "diagnostics": result["diagnostics"],
    }


def _knmi_grid(rgba: np.ndarray) -> np.ndarray:
    r, g, b, a = (rgba[..., index] for index in range(4))
    result = np.zeros(a.shape, dtype=np.uint8)
    visible = a >= 64
    result[visible & (r < 50) & (g < 50) & (b > 100)] = 2
    result[visible & (r < 100) & (g > 100) & (b > 150)] = 3
    result[visible & (r < 50) & (g > 150) & (b < 100)] = 4
    result[visible & (r > 200) & (g > 200) & (b < 50)] = 5
    result[visible & (r > 220) & (g > 100) & (g <= 200) & (b < 50)] = 6
    result[visible & (r > 200) & (g < 80) & (b < 80)] = 7
    result[visible & (r > 150) & (r <= 200) & (g < 50) & (b > 100)] = 8
    result[visible & (result == 0) & ((r.astype(np.uint16)+g+b) > 100)] = 1
    return result


def _knmi_frame(time_value: datetime, source: str, api_key: str) -> dict:
    timestamp_text = time_value.strftime("%Y-%m-%dT%H:%M:%SZ")
    dataset = "nl-rdr-data-rtcor-5m" if source == "knmi" else "radar_forecast_2.0"
    layer = "RAD_NL25_RAC_RT" if source == "knmi" else "precipitation_nowcast"
    params = {
        "DATASET": dataset, "SERVICE": "WMS", "VERSION": "1.3.0",
        "REQUEST": "GetMap", "LAYERS": layer, "CRS": "EPSG:4326",
        "BBOX": ",".join(str(value) for value in KNMI_BOUNDS),
        "WIDTH": 512, "HEIGHT": 512, "FORMAT": "image/png",
        "TIME": timestamp_text, "STYLES": "radar/nearest",
    }
    rgba = _image_rgba(_download_with_headers(
        f"{KNMI_WMS_BASE}?{urlencode(params)}", maximum_bytes=MAX_IMAGE_BYTES,
        headers={"Authorization": api_key},
    ))
    grid = _knmi_grid(rgba)
    left, bottom, right, top = KNMI_BOUNDS

    def corner(row, column):
        return (
            top - row / grid.shape[0] * (top - bottom),
            left + column / grid.shape[1] * (right - left),
        )

    timestamp = time_value.timestamp()
    return {
        "timestamp": timestamp,
        "observations": _component_observations(
            grid, source=source, timestamp=timestamp, quality=0.98,
            corner_to_latlon=corner,
        ),
        "runs": _runs(grid, corner) if source == "knmi" else [],
    }


def knmi_fetch(job) -> dict:
    if job.payload.get("api_key"):
        raise ValueError("KNMI-secret mag niet in workerjobpayload staan")
    api_key = os.getenv("STV3_KNMI_API_KEY", "").strip()
    if not api_key:
        raise ValueError("KNMI API-key ontbreekt")
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    now = now.replace(minute=(now.minute // 5) * 5)
    requests = [(now, "knmi")] + [
        (now + timedelta(minutes=step), "knmi" if step == 0 else "knmi_forecast")
        for step in (0, 30, 60, 90)
    ]
    # Twee netwerkthreads houden de worker begrensd maar voorkomen een extreem
    # lange nowcastjob; beelddecode en componentextractie blijven hierna serieel.
    with ThreadPoolExecutor(max_workers=2) as executor:
        frames = list(executor.map(lambda item: _knmi_frame(*item, api_key), requests))
    current = frames[0]
    observations = [item for frame in frames for item in frame["observations"]]
    result = {
        "provider": "knmi", "frame_timestamp": current["timestamp"],
        "observations": observations,
        "overlay": {"source": "knmi", "timestamp": current["timestamp"], "runs": current["runs"]},
        "diagnostics": {
            "frames": len(frames), "forecast_horizon_minutes": 90,
            "forecast_steps_minutes": [0, 30, 60, 90],
        },
    }
    return {
        **write_json_artifact(job.job_id, result), "provider": "knmi",
        "frame_timestamp": current["timestamp"],
        "observation_count": len(observations), "diagnostics": result["diagnostics"],
    }


def _geotiff_grid(
    frame: bytes,
    *,
    source: str,
    timestamp: float,
    areas: tuple[dict, ...],
) -> dict:
    with Image.open(io.BytesIO(frame)) as image:
        values = np.asarray(image)
        tie = image.tag_v2.get(33922)
        scale = image.tag_v2.get(33550)
    if not tie or not scale:
        raise ValueError(f"{source}-GeoTIFF mist georeferentie")
    if source == "aemet_radar":
        grid = np.where((values >= 1) & (values <= 8), values, 0).astype(np.uint8)
        lat_top, lon_left = float(tie[4]), float(tie[3])
        lat_scale, lon_scale = float(scale[1]), float(scale[0])

        def corner(row, column):
            return (
                round(lat_top - row * lat_scale, 5),
                round(lon_left + column * lon_scale, 5),
            )
    else:
        rate = np.asarray(values, dtype=np.float32)
        valid = (rate >= 0.1) & (rate < 500.0)
        grid = np.zeros(rate.shape, dtype=np.uint8)
        for row, column in np.argwhere(valid):
            grid[row, column] = _intensity(float(rate[row, column]))
        x0, y0 = float(tie[3]), float(tie[4])
        transformer = Transformer.from_crs(DPC_CRS, "EPSG:4326", always_xy=True)

        def corner(row, column):
            lon, lat = transformer.transform(
                x0 + column * float(scale[0]),
                y0 - row * float(scale[1]),
            )
            return round(float(lat), 5), round(float(lon), 5)

    observations = _component_observations(
        grid, source=source, timestamp=timestamp,
        quality=0.98 if source == "aemet_radar" else 0.99,
        corner_to_latlon=corner,
    )
    if areas:
        observations = [
            item for item in observations
            if any(_contains(area, item["lat"], item["lon"]) for area in areas)
        ]
    runs = _runs(grid, corner)
    # Region clipping is repeated by the regional frame worker; retaining the
    # exact source runs here avoids altering the original-resolution contract.
    return {
        "provider": source, "frame_timestamp": timestamp,
        "observations": observations,
        "overlay": {"source": source, "timestamp": timestamp, "runs": runs},
    }


def aemet_radar_fetch(job) -> dict:
    archive_payload = _download_with_headers(
        AEMET_URL, maximum_bytes=10 * 1024 * 1024
    )
    with tarfile.open(fileobj=io.BytesIO(archive_payload), mode="r:gz") as archive:
        candidates = []
        for member in archive.getmembers():
            match = AEMET_FRAME_RE.fullmatch(member.name)
            if match and member.isfile() and 0 < member.size <= 5 * 1024 * 1024:
                candidates.append((match.group(1), member))
        if not candidates:
            raise ValueError("AEMET-bundel bevat geen geldig radarframe")
        stamp, member = max(candidates, key=lambda item: item[0])
        stream = archive.extractfile(member)
        if stream is None:
            raise ValueError("AEMET-frame ontbreekt")
        frame = stream.read(member.size + 1)
    timestamp = datetime.strptime(stamp, "%Y%m%d%H%M").replace(
        tzinfo=timezone.utc
    ).timestamp()
    if time.time() - timestamp > 25 * 60:
        raise ValueError("AEMET-frame is te oud")
    result = _geotiff_grid(
        frame, source="aemet_radar", timestamp=timestamp,
        areas=tuple(job.payload.get("areas") or ()),
    )
    result["diagnostics"] = {"frame": member.name}
    return {
        **write_json_artifact(job.job_id, result), "provider": "aemet_radar",
        "frame_timestamp": timestamp,
        "observation_count": len(result["observations"]),
        "diagnostics": result["diagnostics"],
    }


def dpc_radar_fetch(job) -> dict:
    headers = {"Origin": "https://radar.protezionecivile.it"}
    catalog = json.loads(_download_with_headers(
        f"{DPC_API}/findLastProductByType?type=SRI",
        maximum_bytes=2 * 1024 * 1024, headers=headers,
    ).decode("utf-8"))
    products = catalog.get("lastProducts") or []
    if not products:
        raise ValueError("DPC API geeft geen SRI-product")
    timestamp_ms = int(products[0]["time"])
    timestamp = timestamp_ms / 1000.0
    if time.time() - timestamp > 20 * 60:
        raise ValueError("DPC-frame is te oud")
    download = json.loads(_post(
        f"{DPC_API}/downloadProduct",
        body=json.dumps({"productType": "SRI", "productDate": timestamp_ms}).encode(),
        maximum_bytes=2 * 1024 * 1024,
        headers={**headers, "Content-Type": "application/json"},
    ).decode("utf-8"))
    frame = _download_with_headers(
        str(download["url"]), maximum_bytes=25 * 1024 * 1024
    )
    result = _geotiff_grid(
        frame, source="dpc_radar", timestamp=timestamp,
        areas=tuple(job.payload.get("areas") or ()),
    )
    result["diagnostics"] = {"product": "SRI", "timestamp_ms": timestamp_ms}
    return {
        **write_json_artifact(job.job_id, result), "provider": "dpc_radar",
        "frame_timestamp": timestamp,
        "observation_count": len(result["observations"]),
        "diagnostics": result["diagnostics"],
    }


def meteofrance_radar_fetch(job) -> dict:
    if job.payload.get("token") or job.payload.get("application_id"):
        raise ValueError("Meteo-France-secret mag niet in workerjobpayload staan")
    static_token = os.getenv("STV3_METEOFRANCE_API_TOKEN", "").strip()
    application_id = os.getenv(
        "STV3_METEOFRANCE_APPLICATION_ID", ""
    ).removeprefix("Basic ").strip()
    if application_id:
        token_body = _post(
            METEOFRANCE_TOKEN,
            body=b"grant_type=client_credentials",
            maximum_bytes=512 * 1024,
            headers={
                "Authorization": f"Basic {application_id}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        token = str(json.loads(token_body.decode("utf-8"))["access_token"])
    elif static_token:
        token = static_token
    else:
        raise ValueError("Meteo-France credential ontbreekt")
    frame = _download_with_headers(
        METEOFRANCE_PRODUCT, maximum_bytes=40 * 1024 * 1024,
        headers={"Authorization": f"Bearer {token}"},
    )
    result = parse_odim_rainfall(
        frame, tuple(job.payload.get("areas") or ()),
        source="meteofrance_radar", quality=0.99,
        max_age_seconds=25 * 60, sample_stride=8,
        accumulation_minutes=5,
    )
    return {
        **write_json_artifact(job.job_id, result),
        "provider": "meteofrance_radar",
        "frame_timestamp": result["frame_timestamp"],
        "observation_count": len(result["observations"]),
    }
