"""H SAF H40B near-real-time satellietneerslagfallback.

H40B levert om de tien minuten een geprojecteerd full-disk NetCDF-raster.
De download wordt centraal gedeeld; alleen vensters rond actieve fallback-
RegionEngines worden uit het raster gelezen en naar Observations omgezet.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from ftplib import FTP
import gzip
import io
import logging
import re
import time

import h5py
import numpy as np
from pyproj import CRS, Transformer

from ..engine.observation import Observation, ObservationType
from .odim_hdf5 import rain_rate_to_intensity
from .raster_components import extract_components, extract_intensity_runs

_LOGGER = logging.getLogger(__name__)

FTP_HOST = "ftphsaf.meteoam.it"
FTP_DIRECTORY = "/h40B/h40_cur_mon_data"
FILE_PATTERN = re.compile(
    r"h40_(?P<date>\d{8})_(?P<time>\d{4})_fdk\.nc\.gz$",
    re.IGNORECASE,
)
MAX_COMPRESSED_BYTES = 8 * 1024 * 1024
MAX_UNCOMPRESSED_BYTES = 160 * 1024 * 1024
MAX_FRAME_AGE_SECONDS = 90 * 60
POLL_INTERVAL_SECONDS = 15 * 60
MIN_QUALITY_PERCENT = 15
MIN_RAIN_RATE = 0.1


def _timestamp_from_name(name: str) -> float | None:
    match = FILE_PATTERN.search(name.rsplit("/", 1)[-1])
    if not match:
        return None
    value = datetime.strptime(
        match.group("date") + match.group("time"), "%Y%m%d%H%M"
    ).replace(tzinfo=timezone.utc)
    return value.timestamp()


def _slice_for_axis(axis: np.ndarray, minimum: float, maximum: float) -> slice:
    """Geef een begrensde slice voor een stijgende of dalende coördinaatas."""
    low, high = sorted((float(minimum), float(maximum)))
    if axis[0] <= axis[-1]:
        start = int(np.searchsorted(axis, low, side="left"))
        end = int(np.searchsorted(axis, high, side="right"))
    else:
        reversed_axis = axis[::-1]
        reverse_start = int(np.searchsorted(reversed_axis, low, side="left"))
        reverse_end = int(np.searchsorted(reversed_axis, high, side="right"))
        start = len(axis) - reverse_end
        end = len(axis) - reverse_start
    start = max(0, min(len(axis) - 1, start))
    end = max(start + 1, min(len(axis), end))
    return slice(start, end)


def _projection_from_dataset(dataset) -> CRS:
    attrs = {}
    for key, value in dataset.attrs.items():
        normalized = value.item() if hasattr(value, "item") else value
        if isinstance(normalized, bytes):
            normalized = normalized.decode("ascii", errors="strict")
        attrs[key] = normalized
    return CRS.from_cf(attrs)


def _numeric_attribute(dataset, name: str, default: float) -> float:
    value = np.asarray(dataset.attrs.get(name, default)).reshape(-1)
    return float(value[0]) if value.size else float(default)


def _area_window(area, to_projection: Transformer, x_axis, y_axis):
    lat_margin = float(area.horizon_km) / 110.574
    cosine = max(0.15, abs(np.cos(np.radians(float(area.center_lat)))))
    lon_margin = float(area.horizon_km) / (111.320 * cosine)
    corners_lon = np.asarray([
        area.center_lon - lon_margin, area.center_lon + lon_margin,
        area.center_lon - lon_margin, area.center_lon + lon_margin,
    ])
    corners_lat = np.asarray([
        area.center_lat - lat_margin, area.center_lat - lat_margin,
        area.center_lat + lat_margin, area.center_lat + lat_margin,
    ])
    projected_x, projected_y = to_projection.transform(corners_lon, corners_lat)
    finite = np.isfinite(projected_x) & np.isfinite(projected_y)
    if not finite.any():
        raise ValueError("H40B target ligt buiten de Meteosat-projectie")
    return (
        _slice_for_axis(x_axis, np.min(projected_x[finite]), np.max(projected_x[finite])),
        _slice_for_axis(y_axis, np.min(projected_y[finite]), np.max(projected_y[finite])),
    )


def parse_h40b_netcdf(
    payload: bytes,
    areas: tuple,
    *,
    timestamp: float,
    now: float | None = None,
):
    """Decodeer één gecomprimeerd H40B-frame voor de gevraagde gebieden."""
    reference_now = time.time() if now is None else float(now)
    age = reference_now - float(timestamp)
    if age > MAX_FRAME_AGE_SECONDS:
        raise ValueError("H40B-frame is ouder dan 90 minuten")
    if len(payload) > MAX_COMPRESSED_BYTES:
        raise ValueError("H40B-download overschrijdt veiligheidslimiet")
    netcdf = gzip.decompress(payload)
    if len(netcdf) > MAX_UNCOMPRESSED_BYTES:
        raise ValueError("Uitgepakt H40B-frame overschrijdt veiligheidslimiet")

    observations = []
    overlay_runs = []
    seen_cells = set()
    with h5py.File(io.BytesIO(netcdf), "r") as product:
        rr_dataset = product["rr"]
        qind_dataset = product.get("qind")
        x_axis = np.asarray(product["nx"], dtype=np.float64)
        y_axis = np.asarray(product["ny"], dtype=np.float64)
        projection = _projection_from_dataset(product["geostationary_projection"])
        to_projection = Transformer.from_crs("EPSG:4326", projection, always_xy=True)
        to_latlon = Transformer.from_crs(projection, "EPSG:4326", always_xy=True)
        scale = _numeric_attribute(rr_dataset, "scale_factor", 0.1)
        offset = _numeric_attribute(rr_dataset, "add_offset", 0.0)
        missing = _numeric_attribute(rr_dataset, "missing_value", -990)

        for area_index, area in enumerate(areas):
            try:
                x_slice, y_slice = _area_window(
                    area, to_projection, x_axis, y_axis
                )
            except (ValueError, OverflowError):
                continue
            raw = np.asarray(rr_dataset[y_slice, x_slice], dtype=np.float32)
            quality = (
                np.asarray(qind_dataset[y_slice, x_slice], dtype=np.float32)
                if qind_dataset is not None
                else np.full(raw.shape, 50.0, dtype=np.float32)
            )
            rain_rate = raw * scale + offset
            valid = (
                (raw != missing)
                & np.isfinite(rain_rate)
                & (rain_rate >= MIN_RAIN_RATE)
                & (quality >= MIN_QUALITY_PERCENT)
            )
            intensity_grid = np.zeros(raw.shape, dtype=np.uint8)
            for row, col in np.argwhere(valid):
                intensity_grid[row, col] = rain_rate_to_intensity(
                    float(rain_rate[row, col])
                )
            if not np.any(intensity_grid):
                continue

            x_start, y_start = x_slice.start, y_slice.start
            dx = float(np.median(np.diff(x_axis)))
            dy = float(np.median(np.diff(y_axis)))

            def corner_to_latlon(row, col):
                x_value = x_axis[x_start] + (float(col) - 0.5) * dx
                y_value = y_axis[y_start] + (float(row) - 0.5) * dy
                lon, lat = to_latlon.transform(x_value, y_value)
                return round(float(lat), 5), round(float(lon), 5)

            overlay_runs.extend(extract_intensity_runs(
                intensity_grid,
                corner_to_latlon,
                include_point=lambda lat, lon, selected=area: selected.contains(lat, lon),
            ))
            components = extract_components(
                intensity_grid, corner_to_latlon, minimum_pixels=2
            )
            pixel_area_km2 = abs(dx * dy) / 1_000_000.0
            for component in components:
                row = int(round(component.centroid_row - 0.5))
                col = int(round(component.centroid_col - 0.5))
                row = min(max(row, 0), quality.shape[0] - 1)
                col = min(max(col, 0), quality.shape[1] - 1)
                lat, lon = corner_to_latlon(
                    component.centroid_row, component.centroid_col
                )
                if not area.contains(lat, lon):
                    continue
                cell_key = (
                    round(lat, 3), round(lon, 3), component.max_intensity
                )
                if cell_key in seen_cells:
                    continue
                seen_cells.add(cell_key)
                component_quality = float(np.mean([
                    quality[pixel_row, pixel_col]
                    for pixel_row, pixel_col in component.pixels
                ])) / 100.0
                component_id = (
                    f"hsaf_h40b:{timestamp:.0f}:a{area_index}:c{component.index}"
                )
                area_km2 = len(component.pixels) * pixel_area_km2
                observations.append(Observation(
                    obs_type=ObservationType.RADAR,
                    lat=lat,
                    lon=lon,
                    timestamp=timestamp,
                    intensity=component.max_intensity,
                    area_km2=area_km2,
                    quality=max(0.0, min(1.0, component_quality)),
                    footprint_points=component.boundary,
                    radar_cell_id=component_id,
                    parent_system_id=component_id,
                    parent_area_km2=area_km2,
                    parent_footprint_points=component.boundary,
                    source="hsaf_h40b",
                ))

    return observations, {
        "source": "hsaf_h40b",
        "timestamp": timestamp,
        "runs": overlay_runs,
    }


class HsafH40bProvider:
    """Gedeelde H40B-client; één download bedient alle fallback-engines."""

    plugin_id = "hsaf_h40b"

    def __init__(self, username: str, password: str):
        self._username = username
        self._password = password
        self._last_filename = None
        self._cached_observations = []
        self._last_success_ts = None
        self._last_poll_attempt = None
        self._last_area_signature = None
        self.overlay = None
        self.healthy = False
        self.diagnostics = {"status": "standby", "transport": "ftp"}

    @staticmethod
    def supports(area) -> bool:
        return -65.0 <= area.center_lat <= 65.0 and -65.0 <= area.center_lon <= 65.0

    def _fetch_sync(self, areas: tuple, area_signature: tuple):
        with FTP(FTP_HOST, timeout=45) as ftp:
            ftp.login(self._username, self._password)
            ftp.set_pasv(True)
            names = ftp.nlst(FTP_DIRECTORY)
            candidates = [
                (timestamp, name)
                for name in names
                if (timestamp := _timestamp_from_name(name)) is not None
            ]
            if not candidates:
                raise ValueError("H SAF FTP bevat geen H40B NetCDF-bestanden")
            timestamp, remote_name = max(candidates)
            filename = remote_name.rsplit("/", 1)[-1]
            if (
                filename == self._last_filename
                and area_signature == self._last_area_signature
            ):
                return self._cached_observations, self.overlay, filename, timestamp, 0
            payload = bytearray()

            def receive(block):
                payload.extend(block)
                if len(payload) > MAX_COMPRESSED_BYTES:
                    raise ValueError("H40B-download overschrijdt veiligheidslimiet")

            ftp.retrbinary(f"RETR {remote_name}", receive, blocksize=64 * 1024)
        observations, overlay = parse_h40b_netcdf(
            bytes(payload), areas, timestamp=timestamp
        )
        self._last_area_signature = area_signature
        return observations, overlay, filename, timestamp, len(payload)

    async def async_fetch(self, areas: tuple):
        selected = tuple(area for area in areas if self.supports(area))
        if not selected:
            self.diagnostics = {"status": "standby", "reason": "buiten H40B-dekking"}
            return []
        area_signature = tuple(sorted(
            (
                round(float(area.center_lat), 3),
                round(float(area.center_lon), 3),
                round(float(area.horizon_km), 1),
            )
            for area in selected
        ))
        now = time.time()
        if (
            self._last_poll_attempt is not None
            and now - self._last_poll_attempt < POLL_INTERVAL_SECONDS
            and self._cached_observations
            and area_signature == self._last_area_signature
        ):
            return list(self._cached_observations)
        self._last_poll_attempt = now
        try:
            observations, overlay, filename, timestamp, downloaded = (
                await asyncio.to_thread(self._fetch_sync, selected, area_signature)
            )
            self._cached_observations = list(observations)
            self._last_filename = filename
            self._last_success_ts = time.time()
            self.overlay = overlay
            self.healthy = True
            self.diagnostics = {
                "status": "active",
                "filename": filename,
                "frame_timestamp": timestamp,
                "frame_age_seconds": round(max(0.0, time.time() - timestamp), 1),
                "download_bytes": downloaded,
                "observations": len(observations),
                "quality_threshold_percent": MIN_QUALITY_PERCENT,
                "transport": "ftp",
                "shared_download": True,
            }
            return list(observations)
        except Exception as err:
            self.healthy = False
            self.diagnostics = {
                "status": "error", "error": str(err), "transport": "ftp"
            }
            _LOGGER.exception("H SAF H40B ophalen of decoderen mislukt")
            return []

    def sleep(self):
        self.diagnostics = {
            **self.diagnostics,
            "status": "standby",
            "reason": "bruikbare radar beschikbaar",
        }
