"""Italiaanse Protezione Civile SRI-radar: actuele 1 km GeoTIFF."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import io
import logging

import numpy as np
from pyproj import Transformer

from ..engine.observation import Observation, ObservationType
from .base import Capability, CoverageResult
from .odim_hdf5 import rain_rate_to_intensity
from .raster_components import extract_components

_LOGGER = logging.getLogger(__name__)
API_URL = "https://radar-api.protezionecivile.it"
ORIGIN = "https://radar.protezionecivile.it"
MAX_FILE_BYTES = 25 * 1024 * 1024
MAX_FRAME_AGE_SECONDS = 20 * 60
DPC_CRS = "+proj=lcc +lat_0=42 +lon_0=12.5 +lat_1=42 +lat_2=42 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"


def parse_sri_geotiff(payload: bytes, areas: tuple, *, timestamp: float, now: float | None = None):
    """Decodeer het actuele DPC-SRI-raster; waarden zijn rechtstreeks mm/u."""
    reference_now = datetime.now(timezone.utc).timestamp() if now is None else now
    if reference_now - timestamp > MAX_FRAME_AGE_SECONDS:
        raise ValueError("DPC SRI-frame is ouder dan 20 minuten")
    from PIL import Image
    with Image.open(io.BytesIO(payload)) as image:
        data = np.asarray(image, dtype=np.float32)
        tie = image.tag_v2.get(33922)
        scale = image.tag_v2.get(33550)
    if not tie or not scale:
        raise ValueError("DPC GeoTIFF mist georeferentie")
    rows, columns = np.nonzero((data >= 0.1) & (data < 500.0))
    if not len(rows):
        return []
    x0, y0 = float(tie[3]), float(tie[4])
    transformer = Transformer.from_crs(DPC_CRS, "EPSG:4326", always_xy=True)
    intensity_grid = np.zeros(data.shape, dtype=np.uint8)
    for row, column in zip(rows, columns):
        intensity_grid[row, column] = rain_rate_to_intensity(
            float(data[row, column])
        )

    def corner_to_latlon(row, column):
        lon, lat = transformer.transform(
            x0 + column * float(scale[0]),
            y0 - row * float(scale[1]),
        )
        return round(float(lat), 5), round(float(lon), 5)

    components = extract_components(intensity_grid, corner_to_latlon)
    observations = []
    frame_id = f"dpc_radar:{timestamp:.0f}"
    pixel_area_km2 = float(scale[0]) * float(scale[1]) / 1_000_000.0
    for component in components:
        lon, lat = transformer.transform(
            x0 + component.centroid_col * float(scale[0]),
            y0 - component.centroid_row * float(scale[1]),
        )
        if areas and not any(area.contains(float(lat), float(lon)) for area in areas):
            continue
        component_id = f"{frame_id}:c{component.index}"
        area_km2 = len(component.pixels) * pixel_area_km2
        observations.append(Observation(
            obs_type=ObservationType.RADAR, lat=float(lat), lon=float(lon),
            timestamp=timestamp, intensity=component.max_intensity,
            area_km2=area_km2, quality=0.99,
            footprint_points=component.boundary,
            radar_cell_id=component_id,
            parent_system_id=component_id,
            parent_area_km2=area_km2,
            parent_footprint_points=component.boundary,
            source="dpc_radar",
        ))
    return observations


class DpcRadarProvider:
    plugin_id = "dpc_radar"
    capabilities = frozenset({Capability.RADAR})
    priority = 100

    def __init__(self, session):
        self._session, self._areas, self._last_timestamp = session, (), None
        self.diagnostics = {}

    def supports(self, area):
        margin = area.horizon_km / 90.0
        ok = 35.0 - margin <= area.center_lat <= 48.0 + margin and 4.5 - margin <= area.center_lon <= 20.5 + margin
        return CoverageResult(ok, 1.0 if ok else 0.0, 0.99 if ok else 0.0, "DPC SRI 1 km Italië" if ok else "buiten DPC-dekking")

    async def async_start(self, context): self._areas = tuple(context.config.get("areas", (context.area,)))
    async def async_update_areas(self, areas): self._areas = tuple(areas)
    async def async_stop(self): self._areas = ()

    async def async_fetch(self):
        headers = {"Origin": ORIGIN}
        async with self._session.get(f"{API_URL}/findLastProductByType", params={"type": "SRI"}, headers=headers) as response:
            response.raise_for_status()
            catalog = await response.json()
        products = catalog.get("lastProducts") or []
        if not products:
            raise ValueError("DPC API geeft geen SRI-product")
        timestamp_ms = int(products[0]["time"])
        timestamp = timestamp_ms / 1000.0
        if timestamp_ms == self._last_timestamp:
            return []
        async with self._session.post(f"{API_URL}/downloadProduct", json={"productType": "SRI", "productDate": timestamp_ms}, headers=headers) as response:
            response.raise_for_status()
            download = await response.json()
        async with self._session.get(download["url"]) as response:
            response.raise_for_status()
            if response.content_length and response.content_length > MAX_FILE_BYTES:
                raise ValueError("DPC GeoTIFF overschrijdt veiligheidslimiet")
            payload = await response.read()
        if len(payload) > MAX_FILE_BYTES:
            raise ValueError("DPC GeoTIFF overschrijdt veiligheidslimiet")
        observations = await asyncio.to_thread(parse_sri_geotiff, payload, self._areas, timestamp=timestamp)
        self._last_timestamp = timestamp_ms
        self.diagnostics = {
            "product": "SRI", "frame_timestamp": timestamp,
            "frame_age_seconds": round(max(0.0, datetime.now(timezone.utc).timestamp() - timestamp), 1),
            "observations": len(observations), "resolution_km": 1,
            "source_role": "primary_official_radar",
        }
        _LOGGER.info("DPC SRI: %d observaties binnen actieve engines", len(observations))
        return observations
