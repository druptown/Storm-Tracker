"""NOAA Enterprise Rain Rate global satellite precipitation worker."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import io
import math
import time
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

import h5py
import numpy as np

from .artifacts import write_json_artifact
from .radar import (
    _contains,
    _grid_polygons,
    _intensity,
    _pixel_components,
)
from .tracking_core.engine.bounded_payload import read_limited_stream

BUCKET_URL = "https://noaa-enterprise-rainrate-pds.s3.amazonaws.com/"
PRODUCT_PREFIX = "BLEND/RainRate-Blend-INST"
MAX_FILE_BYTES = 16 * 1024 * 1024
MAX_LIST_RESPONSE_BYTES = 2 * 1024 * 1024
MAX_FRAME_AGE_SECONDS = 45 * 60
MAX_AREAS = 16
MIN_RAIN_RATE = 0.1
MAX_RUNS = 20_000
PUBLISHED_RUNS = 12_000


def _text(value) -> str:
    if isinstance(value, (bytes, np.bytes_)):
        return bytes(value).decode("ascii")
    return str(value)


def _scalar(value, default=0.0) -> float:
    values = np.asarray(value if value is not None else default).reshape(-1)
    return float(values[0]) if values.size else float(default)


def _list_prefix(prefix: str) -> list[dict]:
    query = urlencode({
        "list-type": "2",
        "prefix": prefix,
        "max-keys": "100",
    })
    request = Request(
        f"{BUCKET_URL}?{query}",
        headers={
            "Accept-Encoding": "identity",
            "User-Agent": "StormTrackerWorker/1.7",
        },
    )
    with urlopen(request, timeout=20) as response:
        headers = getattr(response, "headers", None)
        declared = (
            headers.get("Content-Length") if headers is not None else None
        )
        payload = read_limited_stream(
            response,
            maximum_bytes=MAX_LIST_RESPONSE_BYTES,
            declared_bytes=declared,
            require_declared_match=declared is not None,
        )
        root = ET.fromstring(payload)
    result = []
    for item in root.findall(".//{*}Contents"):
        key = item.findtext("{*}Key")
        modified = item.findtext("{*}LastModified")
        size = item.findtext("{*}Size")
        if not key or not key.endswith(".nc"):
            continue
        result.append({
            "key": key,
            "modified": modified or "",
            "size": int(size or 0),
        })
    return result


def latest_product(now: datetime | None = None) -> dict:
    """Return the newest corrected global blend object."""
    now = now or datetime.now(timezone.utc)
    objects = []
    for moment in (now - timedelta(hours=1), now):
        objects.extend(_list_prefix(
            f"{PRODUCT_PREFIX}/{moment:%Y/%m/%d/%H}/"
        ))
    if not objects:
        raise ValueError("NOAA ERR bevat geen recent globaal product")
    latest = max(objects, key=lambda item: (item["modified"], item["key"]))
    if latest["size"] <= 0 or latest["size"] > MAX_FILE_BYTES:
        raise ValueError("NOAA ERR-bestand heeft een onveilige grootte")
    return latest


def _download_product(product: dict) -> bytes:
    request = Request(
        BUCKET_URL + quote(product["key"], safe="/"),
        headers={
            "Accept-Encoding": "identity",
            "User-Agent": "StormTrackerWorker/1.7",
        },
    )
    with urlopen(request, timeout=30) as response:
        headers = getattr(response, "headers", None)
        declared = (
            headers.get("Content-Length") if headers is not None else None
        )
        return read_limited_stream(
            response,
            maximum_bytes=MAX_FILE_BYTES,
            declared_bytes=declared,
            require_declared_match=declared is not None,
        )


def _axis(product, size: int, prefix: str) -> np.ndarray:
    minimum = _scalar(product.attrs[f"geospatial_{prefix}_min"])
    maximum = _scalar(product.attrs[f"geospatial_{prefix}_max"])
    resolution = _scalar(product.attrs[f"geospatial_{prefix}_resolution"])
    if prefix == "lat":
        return maximum - np.arange(size, dtype=np.float64) * resolution
    return minimum + np.arange(size, dtype=np.float64) * resolution


def _axis_slice(axis: np.ndarray, minimum: float, maximum: float) -> slice:
    low, high = sorted((float(minimum), float(maximum)))
    indices = np.flatnonzero((axis >= low) & (axis <= high))
    if indices.size == 0:
        return slice(0, 0)
    return slice(int(indices[0]), int(indices[-1]) + 1)


def _windows(area: dict, latitudes: np.ndarray, longitudes: np.ndarray):
    lat_margin = float(area["horizon_km"]) / 110.574
    cosine = max(
        0.1,
        abs(math.cos(math.radians(float(area["center_lat"])))),
    )
    lon_margin = min(
        180.0,
        float(area["horizon_km"]) / (111.320 * cosine),
    )
    lat_slice = _axis_slice(
        latitudes,
        float(area["center_lat"]) - lat_margin,
        float(area["center_lat"]) + lat_margin,
    )
    center = ((float(area["center_lon"]) + 180.0) % 360.0) - 180.0
    low, high = center - lon_margin, center + lon_margin
    ranges = []
    if low < -180.0:
        ranges.extend(((low + 360.0, 180.0), (-180.0, high)))
    elif high > 180.0:
        ranges.extend(((low, 180.0), (-180.0, high - 360.0)))
    else:
        ranges.append((low, high))
    return [
        (lat_slice, _axis_slice(longitudes, left, right))
        for left, right in ranges
    ]


def parse_enterprise_rainrate(
    payload: bytes,
    areas: tuple[dict, ...],
    *,
    now: float | None = None,
) -> dict:
    """Decode only bounded active-region windows from the global NetCDF."""
    if len(payload) > MAX_FILE_BYTES:
        raise ValueError("NOAA ERR-bestand overschrijdt veiligheidslimiet")
    if not areas or len(areas) > MAX_AREAS:
        raise ValueError("NOAA ERR vereist 1 tot 16 actieve regio's")
    observations, runs, seen = [], [], set()
    with h5py.File(io.BytesIO(payload), "r") as product:
        timestamp = datetime.fromisoformat(
            _text(product.attrs["time_coverage_start"]).replace("Z", "+00:00")
        ).timestamp()
        reference_now = time.time() if now is None else float(now)
        if reference_now - timestamp > MAX_FRAME_AGE_SECONDS:
            raise ValueError("NOAA ERR-frame is ouder dan 45 minuten")
        rr, dqf = product["RRQPE"], product["DQF"]
        scale = _scalar(rr.attrs.get("scale_factor"), 1.0)
        offset = _scalar(rr.attrs.get("add_offset"), 0.0)
        fill = _scalar(rr.attrs.get("_FillValue"), -9990)
        latitudes = _axis(product, rr.shape[0], "lat")
        longitudes = _axis(product, rr.shape[1], "lon")
        lat_step = abs(float(np.median(np.diff(latitudes))))
        lon_step = abs(float(np.median(np.diff(longitudes))))

        for area_index, area in enumerate(areas):
            for window_index, (lat_slice, lon_slice) in enumerate(
                _windows(area, latitudes, longitudes)
            ):
                if lat_slice.stop == 0 or lon_slice.stop == 0:
                    continue
                raw = np.asarray(rr[lat_slice, lon_slice], dtype=np.float32)
                quality = np.asarray(
                    dqf[lat_slice, lon_slice],
                    dtype=np.int8,
                )
                rate = raw * scale + offset
                valid = (
                    (raw != fill)
                    & np.isfinite(rate)
                    & (rate >= MIN_RAIN_RATE)
                    & (quality == 0)
                )
                grid = np.zeros(raw.shape, dtype=np.uint8)
                for row, column in np.argwhere(valid):
                    grid[row, column] = _intensity(float(rate[row, column]))
                if not np.any(grid):
                    continue
                row0, column0 = lat_slice.start, lon_slice.start

                def corner_to_latlon(row, column):
                    lat = (
                        latitudes[row0]
                        - (float(row) - 0.5) * lat_step
                    )
                    lon = (
                        longitudes[column0]
                        + (float(column) - 0.5) * lon_step
                    )
                    lon = ((lon + 180.0) % 360.0) - 180.0
                    return round(float(lat), 5), round(float(lon), 5)

                for row in range(grid.shape[0]):
                    column = 0
                    while column < grid.shape[1] and len(runs) < MAX_RUNS:
                        intensity = int(grid[row, column])
                        if intensity <= 0:
                            column += 1
                            continue
                        end = column + 1
                        while (
                            end < grid.shape[1]
                            and int(grid[row, end]) == intensity
                            and end - column < 16
                        ):
                            end += 1
                        center = corner_to_latlon(row + 0.5, (column + end) / 2)
                        if _contains(area, *center):
                            runs.append({
                                "intensity": intensity,
                                "ring": (
                                    corner_to_latlon(row, column),
                                    corner_to_latlon(row, end),
                                    corner_to_latlon(row + 1, end),
                                    corner_to_latlon(row + 1, column),
                                ),
                            })
                        column = end

                for component in _pixel_components(grid):
                    if len(component) < 2:
                        continue
                    centroid_row = sum(r + 0.5 for r, _ in component) / len(component)
                    centroid_col = sum(c + 0.5 for _, c in component) / len(component)
                    lat, lon = corner_to_latlon(centroid_row, centroid_col)
                    if not _contains(area, lat, lon):
                        continue
                    key = (round(lat, 3), round(lon, 3))
                    if key in seen:
                        continue
                    seen.add(key)
                    polygons = tuple(
                        tuple(
                            tuple(
                                corner_to_latlon(
                                    float(row), float(column)
                                )
                                for row, column in ring
                            )
                            for ring in polygon
                        )
                        for polygon in _grid_polygons(component)
                    )
                    boundary = polygons[0][0] if polygons else ()
                    cell_id = (
                        f"noaa_err:{timestamp:.0f}:a{area_index}:"
                        f"w{window_index}:c{len(observations)}"
                    )
                    pixel_area = (
                        111.32 * lat_step
                        * 111.32 * lon_step
                        * max(0.05, math.cos(math.radians(lat)))
                    )
                    area_km2 = len(component) * pixel_area
                    observations.append({
                        "obs_type": "radar",
                        "lat": lat,
                        "lon": lon,
                        "timestamp": timestamp,
                        "intensity": max(
                            int(grid[row, column])
                            for row, column in component
                        ),
                        "area_km2": area_km2,
                        "quality": 0.72,
                        "footprint_points": boundary,
                        "footprint_polygons": polygons,
                        "radar_cell_id": cell_id,
                        "parent_system_id": cell_id,
                        "parent_area_km2": area_km2,
                        "parent_footprint_points": boundary,
                        "parent_footprint_polygons": polygons,
                        "source": "noaa_enterprise_rainrate",
                    })
    raw_run_count = len(runs)
    if raw_run_count > PUBLISHED_RUNS:
        step = math.ceil(raw_run_count / PUBLISHED_RUNS)
        runs = runs[::step]
    return {
        "provider": "noaa_enterprise_rainrate",
        "frame_timestamp": timestamp,
        "observations": observations,
        "overlay": {
            "source": "noaa_enterprise_rainrate",
            "timestamp": timestamp,
            "runs": runs,
        },
        "diagnostics": {
            "platforms": "GOES-19, GOES-18, Himawari-9, MSG2, MSG3",
            "quality_filter": "DQF=0",
            "region_window_count": len(areas),
            "run_count": len(runs),
            "raw_run_count": raw_run_count,
            "overlay_decimated": raw_run_count > len(runs),
        },
    }


def enterprise_rainrate_fetch(job) -> dict:
    areas = tuple(job.payload.get("areas") or ())
    product = latest_product()
    payload = _download_product(product)
    result = parse_enterprise_rainrate(payload, areas)
    artifact = write_json_artifact(job.job_id, result)
    return {
        **artifact,
        "provider": result["provider"],
        "frame_timestamp": result["frame_timestamp"],
        "observation_count": len(result["observations"]),
        "overlay_run_count": len(result["overlay"]["runs"]),
        "download_bytes": len(payload),
        "source_key": product["key"],
    }
