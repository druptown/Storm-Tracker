"""NOAA GOES-18/19 ABI RRQPE satellietneerslag voor Amerika."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import io
import logging
import math
import time
from urllib.parse import quote
import xml.etree.ElementTree as ET

import h5py
import numpy as np
from pyproj import CRS, Transformer

from ..engine.observation import Observation, ObservationType
from .odim_hdf5 import rain_rate_to_intensity
from .raster_components import extract_components, extract_intensity_runs

_LOGGER = logging.getLogger(__name__)

SATELLITES = (18, 19)
PRODUCT_PREFIX = "ABI-L2-RRQPEF"
MAX_FILE_BYTES = 8 * 1024 * 1024
MAX_FRAME_AGE_SECONDS = 45 * 60
POLL_INTERVAL_SECONDS = 10 * 60
MIN_RAIN_RATE = 0.1


def satellite_for_longitude(lon: float) -> int | None:
    """Kies GOES-East/West voor Amerika en slaap erbuiten."""
    lon = ((float(lon) + 180.0) % 360.0) - 180.0
    if -105.0 <= lon <= -15.0:
        return 19
    if -180.0 <= lon < -105.0 or lon >= 155.0:
        return 18
    return None


def _scalar(value, default=0.0) -> float:
    values = np.asarray(value if value is not None else default).reshape(-1)
    return float(values[0]) if values.size else float(default)


def _text(value) -> str:
    if isinstance(value, (bytes, np.bytes_)):
        return bytes(value).decode("ascii")
    return str(value)


def _decode_axis(dataset) -> np.ndarray:
    return (
        np.asarray(dataset[...], dtype=np.float64)
        * _scalar(dataset.attrs.get("scale_factor"), 1.0)
        + _scalar(dataset.attrs.get("add_offset"), 0.0)
    )


def _slice_for_axis(axis: np.ndarray, minimum: float, maximum: float) -> slice:
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


def _projection(dataset) -> tuple[CRS, float]:
    attrs = {key: (_text(value) if isinstance(value, (str, bytes, np.str_, np.bytes_)) else _scalar(value))
             for key, value in dataset.attrs.items()}
    height = float(attrs["perspective_point_height"])
    return CRS.from_proj4(
        "+proj=geos +lat_0={latitude_of_projection_origin} "
        "+lon_0={longitude_of_projection_origin} +h={perspective_point_height} "
        "+a={semi_major_axis} +b={semi_minor_axis} +sweep={sweep_angle_axis}".format(**attrs)
    ), height


def _timestamp(product) -> float:
    value = _text(product.attrs["time_coverage_start"]).replace("Z", "+00:00")
    return datetime.fromisoformat(value).timestamp()


def _area_window(area, to_projection, x_axis, y_axis):
    lat_margin = float(area.horizon_km) / 110.574
    cosine = max(0.15, abs(math.cos(math.radians(float(area.center_lat)))))
    lon_margin = float(area.horizon_km) / (111.320 * cosine)
    lons = np.asarray([area.center_lon - lon_margin, area.center_lon + lon_margin] * 2)
    lats = np.asarray([area.center_lat - lat_margin] * 2 + [area.center_lat + lat_margin] * 2)
    xs, ys = to_projection.transform(lons, lats)
    finite = np.isfinite(xs) & np.isfinite(ys)
    if not finite.any():
        raise ValueError("target ligt buiten de GOES-projectie")
    return (_slice_for_axis(x_axis, np.min(xs[finite]), np.max(xs[finite])),
            _slice_for_axis(y_axis, np.min(ys[finite]), np.max(ys[finite])))


def parse_rrqpe_netcdf(payload: bytes, satellite: int, areas: tuple, *, now=None):
    """Decodeer echte RRQPE-pixels en cellen voor actieve targetvensters."""
    if satellite not in SATELLITES:
        raise ValueError("onbekende GOES-satelliet")
    if len(payload) > MAX_FILE_BYTES:
        raise ValueError("GOES RRQPE-bestand overschrijdt veiligheidslimiet")
    observations, runs, seen = [], [], set()
    with h5py.File(io.BytesIO(payload), "r") as product:
        timestamp = _timestamp(product)
        reference_now = time.time() if now is None else float(now)
        if reference_now - timestamp > MAX_FRAME_AGE_SECONDS:
            raise ValueError("GOES RRQPE-frame is ouder dan 45 minuten")
        rr, dqf = product["RRQPE"], product["DQF"]
        projection, height = _projection(product["goes_imager_projection"])
        x_axis, y_axis = _decode_axis(product["x"]) * height, _decode_axis(product["y"]) * height
        to_projection = Transformer.from_crs("EPSG:4326", projection, always_xy=True)
        to_latlon = Transformer.from_crs(projection, "EPSG:4326", always_xy=True)
        scale, offset = _scalar(rr.attrs.get("scale_factor"), 1.0), _scalar(rr.attrs.get("add_offset"), 0.0)
        fill = _scalar(rr.attrs.get("_FillValue"), 65535)
        for area_index, area in enumerate(areas):
            if satellite_for_longitude(area.center_lon) != satellite:
                continue
            try:
                x_slice, y_slice = _area_window(area, to_projection, x_axis, y_axis)
            except (ValueError, OverflowError):
                continue
            raw = np.asarray(rr[y_slice, x_slice], dtype=np.float32)
            quality = np.asarray(dqf[y_slice, x_slice], dtype=np.uint8)
            rate = raw * scale + offset
            valid = (raw != fill) & np.isfinite(rate) & (rate >= MIN_RAIN_RATE) & (quality == 0)
            grid = np.zeros(raw.shape, dtype=np.uint8)
            for row, col in np.argwhere(valid):
                grid[row, col] = rain_rate_to_intensity(float(rate[row, col]))
            if not np.any(grid):
                continue
            dx, dy = float(np.median(np.diff(x_axis))), float(np.median(np.diff(y_axis)))
            x0, y0 = x_slice.start, y_slice.start

            def corner_to_latlon(row, col):
                lon, lat = to_latlon.transform(
                    x_axis[x0] + (float(col) - 0.5) * dx,
                    y_axis[y0] + (float(row) - 0.5) * dy,
                )
                return round(float(lat), 5), round(float(lon), 5)

            runs.extend(extract_intensity_runs(
                grid, corner_to_latlon,
                include_point=lambda lat, lon, selected=area: selected.contains(lat, lon),
            ))
            for component in extract_components(grid, corner_to_latlon, minimum_pixels=2):
                lat, lon = corner_to_latlon(component.centroid_row, component.centroid_col)
                if not area.contains(lat, lon):
                    continue
                key = (round(lat, 3), round(lon, 3), component.max_intensity)
                if key in seen:
                    continue
                seen.add(key)
                cell_id = f"noaa_goes{satellite}_rrqpe:{timestamp:.0f}:a{area_index}:c{component.index}"
                area_km2 = len(component.pixels) * abs(dx * dy) / 1_000_000.0
                observations.append(Observation(
                    obs_type=ObservationType.RADAR, lat=lat, lon=lon,
                    timestamp=timestamp, intensity=component.max_intensity,
                    area_km2=area_km2, quality=0.9,
                    footprint_points=component.boundary, radar_cell_id=cell_id,
                    parent_system_id=cell_id, parent_area_km2=area_km2,
                    parent_footprint_points=component.boundary,
                    source="noaa_goes_rrqpe",
                ))
    return observations, {"source": "noaa_goes_rrqpe", "timestamp": timestamp, "runs": runs}


class NoaaGoesRrqpeProvider:
    """Gedeelde anonieme S3-client voor GOES-East en GOES-West RRQPE."""

    plugin_id = "noaa_goes_rrqpe"

    def __init__(self, session):
        self._session = session
        self._cache = {}
        self._last_poll_attempt = {}
        self._last_success_ts = None
        self.overlay = None
        self.healthy = False
        self.diagnostics = {"status": "standby"}

    async def _latest_key(self, satellite: int) -> str:
        now = datetime.now(timezone.utc)
        keys = []
        bucket = f"https://noaa-goes{satellite}.s3.amazonaws.com/"
        for moment in (now - timedelta(hours=1), now):
            prefix = f"{PRODUCT_PREFIX}/{moment.year}/{moment.timetuple().tm_yday:03d}/{moment.hour:02d}/"
            async with self._session.get(bucket, params={"list-type": "2", "prefix": prefix, "max-keys": "100"}) as response:
                response.raise_for_status()
                root = ET.fromstring(await response.text())
            keys.extend(node.text for node in root.findall(".//{*}Key") if node.text)
        if not keys:
            raise ValueError(f"GOES-{satellite} RRQPE bevat geen recent frame")
        return max(keys)

    async def _download(self, satellite: int, key: str) -> bytes:
        url = f"https://noaa-goes{satellite}.s3.amazonaws.com/{quote(key, safe='/')}"
        async with self._session.get(url) as response:
            response.raise_for_status()
            if response.content_length and response.content_length > MAX_FILE_BYTES:
                raise ValueError("GOES RRQPE-bestand te groot")
            payload = await response.read()
        if len(payload) > MAX_FILE_BYTES:
            raise ValueError("GOES RRQPE-bestand te groot")
        return payload

    async def async_fetch(self, areas: tuple):
        selected = {sat: tuple(area for area in areas if satellite_for_longitude(area.center_lon) == sat) for sat in SATELLITES}
        selected = {sat: values for sat, values in selected.items() if values}
        if not selected:
            self.sleep("buiten GOES-dekking")
            return []
        observations, overlays, downloaded = [], [], 0
        now = time.time()
        try:
            for satellite, satellite_areas in selected.items():
                signature = tuple((round(a.center_lat, 3), round(a.center_lon, 3), round(a.horizon_km, 1)) for a in satellite_areas)
                cache = self._cache.get(satellite)
                if cache and now - self._last_poll_attempt.get(satellite, 0) < POLL_INTERVAL_SECONDS and cache[0] == signature:
                    sat_obs, overlay = cache[1], cache[2]
                else:
                    self._last_poll_attempt[satellite] = now
                    key = await self._latest_key(satellite)
                    payload = await self._download(satellite, key)
                    downloaded += len(payload)
                    # HDF5-decompressie, projectietransformaties en pixelclustering
                    # zijn CPU-intensief. Laat ze nooit de HA-eventloop blokkeren.
                    sat_obs, overlay = await asyncio.to_thread(
                        parse_rrqpe_netcdf,
                        payload,
                        satellite,
                        satellite_areas,
                    )
                    self._cache[satellite] = (signature, list(sat_obs), overlay, key)
                observations.extend(sat_obs)
                overlays.extend(overlay.get("runs", []))
            self._last_success_ts, self.healthy = time.time(), True
            self.overlay = {"source": "noaa_goes_rrqpe", "timestamp": max((item[2]["timestamp"] for item in self._cache.values()), default=now), "runs": overlays}
            self.diagnostics = {"status": "active", "satellites": sorted(selected), "download_bytes": downloaded, "observations": len(observations), "shared_download": True}
            return observations
        except Exception as err:
            self.healthy = False
            self.diagnostics = {"status": "error", "error": str(err)}
            _LOGGER.exception("NOAA GOES RRQPE ophalen of decoderen mislukt")
            return []

    def sleep(self, reason="bruikbare radar beschikbaar"):
        self.diagnostics = {**self.diagnostics, "status": "standby", "reason": reason}
