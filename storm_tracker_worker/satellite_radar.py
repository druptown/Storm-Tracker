"""Worker-owned satellite precipitation providers.

The Home Assistant integration only submits compact target windows and reads a
checksummed artifact.  FTP/S3 I/O, HDF5 decompression, projection transforms,
pixel classification and component extraction all stay in the isolated worker.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from ftplib import FTP
import gzip
import io
import math
import os
import re
import time
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

import h5py
import numpy as np
from pyproj import CRS, Transformer

from .artifacts import write_json_artifact
from .provider_radar import _component_observations, _runs
from .radar import _contains, _intensity

HSAF_HOST = "ftphsaf.meteoam.it"
HSAF_DIRECTORY = "/h40B/h40_cur_mon_data"
HSAF_PATTERN = re.compile(
    r"h40_(?P<date>\d{8})_(?P<time>\d{4})_fdk\.nc\.gz$", re.IGNORECASE
)
HSAF_MAX_COMPRESSED = 8 * 1024 * 1024
HSAF_MAX_UNCOMPRESSED = 160 * 1024 * 1024
HSAF_MAX_AGE = 90 * 60
HSAF_MIN_QUALITY = 15.0

GOES_SATELLITES = (18, 19)
GOES_PRODUCT = "ABI-L2-RRQPEF"
GOES_MAX_FILE = 8 * 1024 * 1024
GOES_MAX_LIST = 2 * 1024 * 1024
GOES_MAX_AGE = 45 * 60
MAX_FUTURE_SKEW = 5 * 60

_HSAF_CACHE: dict[tuple, dict] = {}
_GOES_CACHE: dict[tuple, dict] = {}
MAX_AREAS = 64
MAX_HORIZON_KM = 2_000.0
MAX_WINDOW_CELLS = 2_000_000
MAX_WET_PIXELS = 100_000
MAX_COMPONENTS = 10_000
MAX_RUNS = 25_000
MAX_AXIS_VALUES = 100_000


def _validated_areas(values) -> tuple[dict, ...]:
    areas = tuple(values or ())
    if not areas or len(areas) > MAX_AREAS:
        raise ValueError("ongeldig aantal satellietdoelgebieden")
    result = []
    seen = set()
    for area in areas:
        if not isinstance(area, dict):
            raise ValueError("satellietdoelgebied is geen object")
        lat = float(area["center_lat"])
        lon = float(area["center_lon"])
        horizon = float(area["horizon_km"])
        if (
            not math.isfinite(lat)
            or not math.isfinite(lon)
            or not math.isfinite(horizon)
            or not -90 <= lat <= 90
            or not -180 <= lon <= 180
            or not 0 < horizon <= MAX_HORIZON_KM
        ):
            raise ValueError("ongeldig satellietdoelgebied")
        normalized = {
            "center_lat": lat,
            "center_lon": lon,
            "horizon_km": horizon,
        }
        signature = (round(lat, 6), round(lon, 6), round(horizon, 3))
        if signature not in seen:
            seen.add(signature)
            result.append(normalized)
    return tuple(result)


def _bounded_grid(grid: np.ndarray) -> None:
    if grid.ndim != 2 or grid.size > MAX_WINDOW_CELLS:
        raise ValueError("satellietvenster overschrijdt rasterbudget")
    if int(np.count_nonzero(grid)) > MAX_WET_PIXELS:
        raise ValueError("satellietvenster overschrijdt nat-pixelbudget")


def _bounded_window(x_window: slice, y_window: slice) -> None:
    width = int(x_window.stop) - int(x_window.start)
    height = int(y_window.stop) - int(y_window.start)
    if width <= 0 or height <= 0 or width * height > MAX_WINDOW_CELLS:
        raise ValueError("satellietvenster overschrijdt rasterbudget")


def _validated_axis(dataset, *, expected_length: int) -> np.ndarray:
    """Load one bounded, finite and strictly monotonic projection axis."""
    shape = tuple(int(value) for value in dataset.shape)
    if (
        len(shape) != 1
        or shape[0] != int(expected_length)
        or shape[0] < 2
        or shape[0] > MAX_AXIS_VALUES
    ):
        raise ValueError("satellietproduct bevat een ongeldige projectie-as")
    values = np.asarray(dataset[...], dtype=np.float64)
    if values.shape != shape or not np.isfinite(values).all():
        raise ValueError("satellietproduct bevat een ongeldige projectie-as")
    differences = np.diff(values)
    if not (np.all(differences > 0.0) or np.all(differences < 0.0)):
        raise ValueError("satellietprojectie-as is niet strikt monotoon")
    return values


def _validated_raster_shape(rain, quality) -> tuple[int, int]:
    """Validate dataset topology before any HDF5 slice is materialised."""
    shape = tuple(int(value) for value in rain.shape)
    if (
        len(shape) != 2
        or min(shape) < 2
        or max(shape) > MAX_AXIS_VALUES
        or (quality is not None and tuple(quality.shape) != shape)
    ):
        raise ValueError("satellietproduct bevat een ongeldige rastertopologie")
    return shape


def _clip_grid_to_area(
    grid: np.ndarray,
    area: dict,
    inverse: Transformer,
    x_axis: np.ndarray,
    y_axis: np.ndarray,
    x_window: slice,
    y_window: slice,
    dx: float,
    dy: float,
) -> None:
    """Mask wet pixel centres outside the circular observation horizon."""
    rows, columns = np.nonzero(grid)
    if not rows.size:
        return
    xs = x_axis[x_window.start] + columns.astype(np.float64) * dx
    ys = y_axis[y_window.start] + rows.astype(np.float64) * dy
    longitudes, latitudes = inverse.transform(xs, ys)
    center_lat = math.radians(float(area["center_lat"]))
    center_lon = math.radians(float(area["center_lon"]))
    lat_radians = np.radians(np.asarray(latitudes, dtype=np.float64))
    lon_radians = np.radians(np.asarray(longitudes, dtype=np.float64))
    delta_lat = lat_radians - center_lat
    delta_lon = lon_radians - center_lon
    haversine = (
        np.sin(delta_lat / 2.0) ** 2
        + math.cos(center_lat)
        * np.cos(lat_radians)
        * np.sin(delta_lon / 2.0) ** 2
    )
    distances = 6371.0088 * 2.0 * np.arctan2(
        np.sqrt(haversine), np.sqrt(np.maximum(0.0, 1.0 - haversine))
    )
    outside = ~np.isfinite(distances) | (distances > float(area["horizon_km"]))
    grid[rows[outside], columns[outside]] = 0


def _merge_unique(target: list, values: list, seen: set) -> None:
    for value in values:
        key = (
            round(float(value["lat"]), 3),
            round(float(value["lon"]), 3),
            int(value["intensity"]),
        )
        if key not in seen:
            seen.add(key)
            target.append(value)
    if len(target) > MAX_COMPONENTS:
        raise ValueError("satellietframe overschrijdt componentbudget")


def _merge_runs(target: list, values: list, seen: set) -> None:
    for value in values:
        key = (
            int(value["intensity"]),
            tuple(tuple(float(point) for point in pair) for pair in value["ring"]),
        )
        if key not in seen:
            seen.add(key)
            target.append(value)
    if len(target) > MAX_RUNS:
        raise ValueError("satellietframe overschrijdt overlaybudget")


def _bounded_url(url: str, maximum: int) -> bytes:
    if not url.startswith("https://"):
        raise ValueError("satellietprovider accepteert uitsluitend HTTPS")
    request = Request(
        url,
        headers={
            "Accept-Encoding": "identity",
            "User-Agent": "StormTrackerWorker/1.16",
        },
    )
    with urlopen(request, timeout=45) as response:
        if str(response.headers.get("Content-Encoding") or "identity").lower() != "identity":
            raise ValueError("onverwachte transportcompressie")
        declared = response.headers.get("Content-Length")
        if declared is not None and int(declared) > maximum:
            raise ValueError("satellietresponse overschrijdt bytecontract")
        value = response.read(maximum + 1)
        if len(value) > maximum:
            raise ValueError("satellietresponse overschrijdt bytecontract")
        if declared is not None and len(value) != int(declared):
            raise ValueError("satellietresponse is onvolledig")
        from .thin_raw_archive import capture_received
        capture_received(str(response.geturl()), value, received_at=time.time(),
                         content_type=response.headers.get('Content-Type'), http_status=getattr(response,'status',None))
        return value


def _validate_product_time(timestamp: float, maximum_age: float, source: str) -> None:
    now = time.time()
    if not math.isfinite(float(timestamp)):
        raise ValueError(f"{source}-frame heeft geen eindige producttijd")
    if float(timestamp) > now + MAX_FUTURE_SKEW:
        raise ValueError(f"{source}-frame ligt te ver in de toekomst")
    if now - float(timestamp) > float(maximum_age):
        raise ValueError(f"{source}-frame is te oud")


def _slice(axis: np.ndarray, minimum: float, maximum: float) -> slice:
    low, high = sorted((float(minimum), float(maximum)))
    if axis[0] <= axis[-1]:
        start = int(np.searchsorted(axis, low, side="left"))
        end = int(np.searchsorted(axis, high, side="right"))
    else:
        reverse = axis[::-1]
        reverse_start = int(np.searchsorted(reverse, low, side="left"))
        reverse_end = int(np.searchsorted(reverse, high, side="right"))
        start, end = len(axis) - reverse_end, len(axis) - reverse_start
    start = max(0, min(len(axis) - 1, start))
    return slice(start, max(start + 1, min(len(axis), end)))


def _margins(area: dict) -> tuple[np.ndarray, np.ndarray]:
    lat = float(area["center_lat"])
    lon = float(area["center_lon"])
    horizon = float(area["horizon_km"])
    lat_margin = horizon / 110.574
    lon_margin = horizon / (111.320 * max(0.15, abs(math.cos(math.radians(lat)))))
    return (
        np.asarray([lon - lon_margin, lon + lon_margin] * 2),
        np.asarray([lat - lat_margin] * 2 + [lat + lat_margin] * 2),
    )


def _projection_window(area, transformer, x_axis, y_axis):
    lons, lats = _margins(area)
    xs, ys = transformer.transform(lons, lats)
    finite = np.isfinite(xs) & np.isfinite(ys)
    if not finite.any():
        raise ValueError("target ligt buiten satellietprojectie")
    return _slice(x_axis, np.min(xs[finite]), np.max(xs[finite])), _slice(
        y_axis, np.min(ys[finite]), np.max(ys[finite])
    )


def _attribute(dataset, name: str, default: float) -> float:
    value = np.asarray(dataset.attrs.get(name, default)).reshape(-1)
    return float(value[0]) if value.size else float(default)


def _timestamp_from_hsaf_name(name: str) -> float | None:
    match = HSAF_PATTERN.search(name.rsplit("/", 1)[-1])
    if not match:
        return None
    return datetime.strptime(
        match.group("date") + match.group("time"), "%Y%m%d%H%M"
    ).replace(tzinfo=timezone.utc).timestamp()


def _decode_gzip(payload: bytes) -> bytes:
    if len(payload) > HSAF_MAX_COMPRESSED:
        raise ValueError("H40B-download overschrijdt veiligheidslimiet")
    with gzip.GzipFile(fileobj=io.BytesIO(payload)) as stream:
        value = stream.read(HSAF_MAX_UNCOMPRESSED + 1)
    if len(value) > HSAF_MAX_UNCOMPRESSED:
        raise ValueError("H40B-product overschrijdt decompressielimiet")
    return value


def _hsaf_parse(payload: bytes, areas: tuple[dict, ...], timestamp: float) -> dict:
    _validate_product_time(timestamp, HSAF_MAX_AGE, "H40B")
    observations, runs, rain_samples = [], [], []
    seen_observations, seen_runs = set(), set()
    seen_samples = set()
    with h5py.File(io.BytesIO(_decode_gzip(payload)), "r") as product:
        rain = product["rr"]
        quality_dataset = product.get("qind")
        height, width = _validated_raster_shape(rain, quality_dataset)
        x_axis = _validated_axis(product["nx"], expected_length=width)
        y_axis = _validated_axis(product["ny"], expected_length=height)
        attributes = {}
        for key, raw in product["geostationary_projection"].attrs.items():
            value = raw.item() if hasattr(raw, "item") else raw
            attributes[key] = value.decode("ascii") if isinstance(value, bytes) else value
        projection = CRS.from_cf(attributes)
        forward = Transformer.from_crs("EPSG:4326", projection, always_xy=True)
        inverse = Transformer.from_crs(projection, "EPSG:4326", always_xy=True)
        scale = _attribute(rain, "scale_factor", 0.1)
        offset = _attribute(rain, "add_offset", 0.0)
        missing = _attribute(rain, "missing_value", -990)
        for index, area in enumerate(areas):
            try:
                x_window, y_window = _projection_window(
                    area, forward, x_axis, y_axis
                )
            except (ValueError, OverflowError):
                continue
            _bounded_window(x_window, y_window)
            raw = np.asarray(rain[y_window, x_window], dtype=np.float32)
            quality = (
                np.asarray(quality_dataset[y_window, x_window], dtype=np.float32)
                if quality_dataset is not None
                else np.full(raw.shape, 50.0, dtype=np.float32)
            )
            rate = raw * scale + offset
            valid = (
                (raw != missing)
                & np.isfinite(rate)
                & (rate >= 0.1)
                & (quality >= HSAF_MIN_QUALITY)
            )
            target_x, target_y = forward.transform(
                float(area["center_lon"]), float(area["center_lat"])
            )
            target_column = int(np.argmin(np.abs(x_axis - target_x)))
            target_row = int(np.argmin(np.abs(y_axis - target_y)))
            if (
                y_window.start <= target_row < y_window.stop
                and x_window.start <= target_column < x_window.stop
            ):
                local_row = target_row - y_window.start
                local_column = target_column - x_window.start
                sample_key = (
                    round(float(area["center_lat"]), 6),
                    round(float(area["center_lon"]), 6),
                )
                if valid[local_row, local_column] and sample_key not in seen_samples:
                    seen_samples.add(sample_key)
                    rain_samples.append({
                        "lat": float(area["center_lat"]),
                        "lon": float(area["center_lon"]),
                        "timestamp": float(timestamp),
                        "rain_rate_mm_h": float(rate[local_row, local_column]),
                        "quality": float(np.clip(
                            quality[local_row, local_column] / 100.0, 0.0, 1.0
                        )),
                        "source": "hsaf_h40b",
                        "unit": "mm/h",
                    })
            grid = np.zeros(raw.shape, dtype=np.uint8)
            if valid.any():
                grid[valid] = np.vectorize(_intensity)(rate[valid]).astype(np.uint8)
            _bounded_grid(grid)
            if not grid.any():
                continue
            dx = float(np.median(np.diff(x_axis)))
            dy = float(np.median(np.diff(y_axis)))
            _clip_grid_to_area(
                grid, area, inverse, x_axis, y_axis,
                x_window, y_window, dx, dy,
            )
            if not grid.any():
                continue

            def corner(row, column, *, x0=x_window.start, y0=y_window.start):
                lon, lat = inverse.transform(
                    x_axis[x0] + (float(column) - 0.5) * dx,
                    y_axis[y0] + (float(row) - 0.5) * dy,
                )
                return round(float(lat), 5), round(float(lon), 5)

            _merge_runs(
                runs,
                _runs(
                    grid,
                    corner,
                    include_point=lambda lat, lon, selected=area: _contains(
                        selected, lat, lon
                    ),
                    maximum_results=MAX_RUNS,
                ),
                seen_runs,
            )
            values = _component_observations(
                grid,
                source="hsaf_h40b",
                timestamp=timestamp,
                quality=0.0,
                corner_to_latlon=corner,
                minimum_pixels=2,
                include_point=lambda lat, lon, selected=area: _contains(
                    selected, lat, lon
                ),
                maximum_results=MAX_COMPONENTS,
                quality_grid=np.clip(quality / 100.0, 0.0, 1.0),
            )
            for component, value in enumerate(values):
                identity = f"hsaf_h40b:{timestamp:.0f}:a{index}:c{component}"
                value["radar_cell_id"] = identity
                value["parent_system_id"] = identity
            _merge_unique(observations, values, seen_observations)
    return {
        "provider": "hsaf_h40b",
        "frame_timestamp": timestamp,
        "rain_samples": rain_samples,
        "observations": observations,
        "overlay": {"source": "hsaf_h40b", "timestamp": timestamp, "runs": runs},
    }


def hsaf_h40b_fetch(job) -> dict:
    if job.payload.get("username") or job.payload.get("password"):
        raise ValueError("H SAF-secret mag niet in workerjobpayload staan")
    username = os.getenv("STV3_HSAF_USERNAME", "").strip()
    password = os.getenv("STV3_HSAF_PASSWORD", "").strip()
    areas = _validated_areas(job.payload.get("areas"))
    if not username or not password:
        raise ValueError("H SAF-referenties ontbreken")
    signature = tuple(
        (round(float(area["center_lat"]), 3), round(float(area["center_lon"]), 3),
         round(float(area["horizon_km"]), 1))
        for area in areas
    )
    with FTP(HSAF_HOST, timeout=45) as ftp:
        ftp.login(username, password)
        ftp.set_pasv(True)
        candidates = [
            (timestamp, name)
            for name in ftp.nlst(HSAF_DIRECTORY)
            if (timestamp := _timestamp_from_hsaf_name(name)) is not None
        ]
        if not candidates:
            raise ValueError("H SAF FTP bevat geen H40B-product")
        timestamp, remote_name = max(candidates)
        cache_key = (remote_name, signature)
        result = _HSAF_CACHE.get(cache_key)
        downloaded = 0
        if result is None:
            payload = bytearray()

            def receive(block):
                payload.extend(block)
                if len(payload) > HSAF_MAX_COMPRESSED:
                    raise ValueError("H40B-download overschrijdt veiligheidslimiet")

            ftp.retrbinary(f"RETR {remote_name}", receive, blocksize=64 * 1024)
            downloaded = len(payload)
            native_payload = bytes(payload)
            from .thin_raw_archive import capture_received, current_capture_references
            capture_received(f"ftp://{HSAF_HOST}/{remote_name.lstrip('/')}",
                             native_payload, received_at=time.time(), transport="ftp")
            result = _hsaf_parse(native_payload, areas, timestamp)
            references = current_capture_references()
            if references:
                result = {**result, "_thin_raw_capture_ids": references}
            _HSAF_CACHE.clear()
            _HSAF_CACHE[cache_key] = result
        else:
            from .thin_raw_archive import reference_captures
            reference_captures(result.get("_thin_raw_capture_ids", ()))
        _validate_product_time(
            float(result["frame_timestamp"]), HSAF_MAX_AGE, "H40B-cache"
        )
    result = {**result, "diagnostics": {"filename": remote_name.rsplit('/', 1)[-1], "download_bytes": downloaded}}
    return {
        **write_json_artifact(job.job_id, result),
        "provider": "hsaf_h40b",
        "frame_timestamp": timestamp,
        "observation_count": len(result["observations"]),
        "diagnostics": result["diagnostics"],
    }


def _goes_satellite(lon: float) -> int | None:
    longitude = ((float(lon) + 180.0) % 360.0) - 180.0
    if -105.0 <= longitude <= -15.0:
        return 19
    if -180.0 <= longitude < -105.0 or longitude >= 155.0:
        return 18
    return None


def _scalar(value, default=0.0) -> float:
    array = np.asarray(value if value is not None else default).reshape(-1)
    return float(array[0]) if array.size else float(default)


def _text(value) -> str:
    if isinstance(value, (bytes, np.bytes_)):
        return bytes(value).decode("ascii")
    return str(value)


def _goes_projection(dataset) -> tuple[CRS, float]:
    attrs = {
        key: (_text(value) if isinstance(value, (str, bytes, np.str_, np.bytes_)) else _scalar(value))
        for key, value in dataset.attrs.items()
    }
    height = float(attrs["perspective_point_height"])
    crs = CRS.from_proj4(
        "+proj=geos +lat_0={latitude_of_projection_origin} "
        "+lon_0={longitude_of_projection_origin} +h={perspective_point_height} "
        "+a={semi_major_axis} +b={semi_minor_axis} +sweep={sweep_angle_axis}".format(**attrs)
    )
    return crs, height


def _goes_axis(dataset) -> np.ndarray:
    return (
        np.asarray(dataset[...], dtype=np.float64)
        * _scalar(dataset.attrs.get("scale_factor"), 1.0)
        + _scalar(dataset.attrs.get("add_offset"), 0.0)
    )


def _goes_parse(payload: bytes, satellite: int, areas: tuple[dict, ...]) -> dict:
    if len(payload) > GOES_MAX_FILE:
        raise ValueError("GOES RRQPE-bestand overschrijdt veiligheidslimiet")
    observations, runs, rain_samples = [], [], []
    seen_observations, seen_runs = set(), set()
    seen_samples = set()
    with h5py.File(io.BytesIO(payload), "r") as product:
        timestamp = datetime.fromisoformat(
            _text(product.attrs["time_coverage_start"]).replace("Z", "+00:00")
        ).timestamp()
        _validate_product_time(timestamp, GOES_MAX_AGE, "GOES RRQPE")
        rain, quality = product["RRQPE"], product["DQF"]
        height_px, width_px = _validated_raster_shape(rain, quality)
        projection, height = _goes_projection(product["goes_imager_projection"])
        x_axis = _validated_axis(
            product["x"], expected_length=width_px
        )
        y_axis = _validated_axis(
            product["y"], expected_length=height_px
        )
        x_axis = (
            x_axis * _scalar(product["x"].attrs.get("scale_factor"), 1.0)
            + _scalar(product["x"].attrs.get("add_offset"), 0.0)
        ) * height
        y_axis = (
            y_axis * _scalar(product["y"].attrs.get("scale_factor"), 1.0)
            + _scalar(product["y"].attrs.get("add_offset"), 0.0)
        ) * height
        forward = Transformer.from_crs("EPSG:4326", projection, always_xy=True)
        inverse = Transformer.from_crs(projection, "EPSG:4326", always_xy=True)
        scale = _scalar(rain.attrs.get("scale_factor"), 1.0)
        offset = _scalar(rain.attrs.get("add_offset"), 0.0)
        fill = _scalar(rain.attrs.get("_FillValue"), -1.0)
        for index, area in enumerate(areas):
            try:
                x_window, y_window = _projection_window(area, forward, x_axis, y_axis)
            except (ValueError, OverflowError):
                continue
            _bounded_window(x_window, y_window)
            raw = np.asarray(rain[y_window, x_window], dtype=np.float32)
            dqf = np.asarray(quality[y_window, x_window], dtype=np.uint8)
            rate = raw * scale + offset
            valid = (raw != fill) & np.isfinite(rate) & (rate >= 0.1) & (dqf == 0)
            target_x, target_y = forward.transform(
                float(area["center_lon"]), float(area["center_lat"])
            )
            target_column = int(np.argmin(np.abs(x_axis - target_x)))
            target_row = int(np.argmin(np.abs(y_axis - target_y)))
            if (
                y_window.start <= target_row < y_window.stop
                and x_window.start <= target_column < x_window.stop
            ):
                local_row = target_row - y_window.start
                local_column = target_column - x_window.start
                sample_key = (
                    round(float(area["center_lat"]), 6),
                    round(float(area["center_lon"]), 6),
                )
                if valid[local_row, local_column] and sample_key not in seen_samples:
                    seen_samples.add(sample_key)
                    rain_samples.append({
                        "lat": float(area["center_lat"]),
                        "lon": float(area["center_lon"]),
                        "timestamp": float(timestamp),
                        "rain_rate_mm_h": float(rate[local_row, local_column]),
                        "quality": 1.0,
                        "source": f"noaa_goes{satellite}_rrqpe",
                        "unit": "mm/h",
                    })
            grid = np.zeros(raw.shape, dtype=np.uint8)
            if valid.any():
                grid[valid] = np.vectorize(_intensity)(rate[valid]).astype(np.uint8)
            _bounded_grid(grid)
            if not grid.any():
                continue
            dx = float(np.median(np.diff(x_axis)))
            dy = float(np.median(np.diff(y_axis)))
            _clip_grid_to_area(
                grid, area, inverse, x_axis, y_axis,
                x_window, y_window, dx, dy,
            )
            if not grid.any():
                continue

            def corner(row, column, *, x0=x_window.start, y0=y_window.start):
                lon, lat = inverse.transform(
                    x_axis[x0] + (float(column) - 0.5) * dx,
                    y_axis[y0] + (float(row) - 0.5) * dy,
                )
                return round(float(lat), 5), round(float(lon), 5)

            _merge_runs(
                runs,
                _runs(
                    grid,
                    corner,
                    include_point=lambda lat, lon, selected=area: _contains(
                        selected, lat, lon
                    ),
                    maximum_results=MAX_RUNS,
                ),
                seen_runs,
            )
            values = _component_observations(
                grid,
                source="noaa_goes_rrqpe",
                timestamp=timestamp,
                quality=0.9,
                corner_to_latlon=corner,
                minimum_pixels=2,
                include_point=lambda lat, lon, selected=area: _contains(
                    selected, lat, lon
                ),
                maximum_results=MAX_COMPONENTS,
            )
            for component, value in enumerate(values):
                identity = f"noaa_goes_rrqpe:{satellite}:{timestamp:.0f}:a{index}:c{component}"
                value["radar_cell_id"] = identity
                value["parent_system_id"] = identity
            _merge_unique(observations, values, seen_observations)
    return {
        "provider": "noaa_goes_rrqpe",
        "frame_timestamp": timestamp,
        "rain_samples": rain_samples,
        "observations": observations,
        "overlay": {"source": "noaa_goes_rrqpe", "timestamp": timestamp, "runs": runs},
    }


def _latest_goes_key(satellite: int) -> str:
    now = datetime.now(timezone.utc)
    keys = []
    base = f"https://noaa-goes{satellite}.s3.amazonaws.com/"
    for moment in (now - timedelta(hours=1), now):
        prefix = f"{GOES_PRODUCT}/{moment.year}/{moment.timetuple().tm_yday:03d}/{moment.hour:02d}/"
        query = urlencode({"list-type": "2", "prefix": prefix, "max-keys": "100"})
        root = ET.fromstring(_bounded_url(f"{base}?{query}", GOES_MAX_LIST))
        keys.extend(node.text for node in root.findall(".//{*}Key") if node.text)
    if not keys:
        raise ValueError(f"GOES-{satellite} RRQPE bevat geen recent frame")
    return max(keys)


def goes_rrqpe_fetch(job) -> dict:
    areas = _validated_areas(job.payload.get("areas"))
    requested_satellites = job.payload.get("satellites")
    if requested_satellites is None:
        selected = {
            satellite: tuple(area for area in areas if _goes_satellite(area["center_lon"]) == satellite)
            for satellite in GOES_SATELLITES
        }
    else:
        if (
            not isinstance(requested_satellites, list)
            or not requested_satellites
            or len(requested_satellites) > len(GOES_SATELLITES)
            or any(int(value) not in GOES_SATELLITES for value in requested_satellites)
        ):
            raise ValueError("ongeldige GOES-satellietselectie")
        selected = {int(satellite): areas for satellite in requested_satellites}
    selected = {satellite: values for satellite, values in selected.items() if values}
    if not selected:
        raise ValueError("targets liggen buiten GOES-dekking")
    observations, runs, timestamps, rain_samples = [], [], [], []
    overlays_by_satellite = {}
    downloaded = 0
    current_products = {}
    for satellite, satellite_areas in selected.items():
        key = _latest_goes_key(satellite)
        current_products[satellite] = key
        signature = tuple(
            (round(float(area["center_lat"]), 3), round(float(area["center_lon"]), 3),
             round(float(area["horizon_km"]), 1))
            for area in satellite_areas
        )
        cache_key = (satellite, key, signature)
        result = _GOES_CACHE.get(cache_key)
        if result is None:
            payload = _bounded_url(
                f"https://noaa-goes{satellite}.s3.amazonaws.com/{quote(key, safe='/')}",
                GOES_MAX_FILE,
            )
            downloaded += len(payload)
            result = _goes_parse(payload, satellite, satellite_areas)
            from .thin_raw_archive import current_capture_references
            references = current_capture_references()
            if references:
                result = {**result, "_thin_raw_capture_ids": references}
            _GOES_CACHE[cache_key] = result
        else:
            from .thin_raw_archive import reference_captures
            reference_captures(result.get("_thin_raw_capture_ids", ()))
        _validate_product_time(
            float(result["frame_timestamp"]),
            GOES_MAX_AGE,
            f"GOES-{satellite} RRQPE-cache",
        )
        _merge_unique(
            observations,
            result["observations"],
            {
                (
                    round(float(value["lat"]), 3),
                    round(float(value["lon"]), 3),
                    int(value["intensity"]),
                )
                for value in observations
            },
        )
        _merge_runs(
            runs,
            result["overlay"]["runs"],
            {
                (
                    int(value["intensity"]),
                    tuple(
                        tuple(float(point) for point in pair)
                        for pair in value["ring"]
                    ),
                )
                for value in runs
            },
        )
        timestamps.append(float(result["frame_timestamp"]))
        rain_samples.extend(result.get("rain_samples", ()))
        overlays_by_satellite[str(satellite)] = result["overlay"]
    # Bound the persistent process cache to only the latest product per satellite.
    live_keys = set(current_products.items())
    for cache_key in tuple(_GOES_CACHE):
        if (cache_key[0], cache_key[1]) not in live_keys:
            _GOES_CACHE.pop(cache_key, None)
    # Conservative compatibility timestamp: a fresh product must never hide
    # a stale second satellite. Regional processing below uses exact times.
    timestamp = min(timestamps)
    result = {
        "provider": "noaa_goes_rrqpe",
        "frame_timestamp": timestamp,
        "rain_samples": rain_samples,
        "observations": observations,
        "overlay": {"source": "noaa_goes_rrqpe", "timestamp": timestamp, "runs": runs},
        "overlays_by_satellite": overlays_by_satellite,
        "diagnostics": {"satellites": sorted(selected), "download_bytes": downloaded},
    }
    return {
        **write_json_artifact(job.job_id, result),
        "provider": "noaa_goes_rrqpe",
        "frame_timestamp": timestamp,
        "observation_count": len(observations),
        "diagnostics": result["diagnostics"],
    }
