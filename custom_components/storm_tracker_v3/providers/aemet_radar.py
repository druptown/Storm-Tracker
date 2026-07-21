"""Spaanse AEMET-composietradar uit de publieke GeoTIFF-bundel."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import io
import logging
import re
import tarfile

import numpy as np

from ..engine.observation import Observation, ObservationType
from .base import Capability, CoverageResult
from .raster_components import extract_components, extract_intensity_runs

_LOGGER = logging.getLogger(__name__)
DOWNLOAD_URL = "https://www.aemet.es/es/api-eltiempo/radar/download/compo"
MAX_ARCHIVE_BYTES = 10 * 1024 * 1024
MAX_FRAME_BYTES = 5 * 1024 * 1024
MAX_FRAME_AGE_SECONDS = 25 * 60
FRAME_RE = re.compile(r"^down_radw(\d{12})_4326\.tif$")


def latest_frame_from_archive(payload: bytes) -> tuple[bytes, float, str]:
    """Lees uitsluitend het nieuwste verwachte GeoTIFF-lid uit de bundel."""
    with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as archive:
        candidates = []
        for member in archive.getmembers():
            match = FRAME_RE.fullmatch(member.name)
            if match and member.isfile() and member.size <= MAX_FRAME_BYTES:
                candidates.append((match.group(1), member))
        if not candidates:
            raise ValueError("AEMET-bundel bevat geen geldig radarframe")
        stamp, member = max(candidates, key=lambda item: item[0])
        stream = archive.extractfile(member)
        if stream is None:
            raise ValueError("AEMET-frame kon niet worden gelezen")
        frame = stream.read(MAX_FRAME_BYTES + 1)
        if len(frame) > MAX_FRAME_BYTES:
            raise ValueError("AEMET-frame overschrijdt veiligheidslimiet")
    timestamp = datetime.strptime(stamp, "%Y%m%d%H%M").replace(
        tzinfo=timezone.utc
    ).timestamp()
    return frame, timestamp, member.name


def parse_aemet_geotiff(payload: bytes, areas: tuple, *, timestamp: float, now: float | None = None, overlay_out: list | None = None):
    reference_now = datetime.now(timezone.utc).timestamp() if now is None else now
    if reference_now - timestamp > MAX_FRAME_AGE_SECONDS:
        raise ValueError("AEMET-radarframe is ouder dan 25 minuten")
    from PIL import Image
    with Image.open(io.BytesIO(payload)) as image:
        data = np.asarray(image, dtype=np.uint8)
        tie = image.tag_v2.get(33922)
        scale = image.tag_v2.get(33550)
    if not tie or not scale:
        raise ValueError("AEMET GeoTIFF mist EPSG:4326-georeferentie")
    intensity_grid = np.where((data >= 1) & (data <= 8), data, 0).astype(np.uint8)
    lat_top, lon_left = float(tie[4]), float(tie[3])
    lat_scale, lon_scale = float(scale[1]), float(scale[0])
    components = extract_components(
        intensity_grid,
        lambda row, col: (
            round(lat_top - row * lat_scale, 5),
            round(lon_left + col * lon_scale, 5),
        ),
    )
    if overlay_out is not None:
        overlay_out.append({
            "source": "aemet_radar", "timestamp": timestamp,
            "runs": extract_intensity_runs(
                intensity_grid,
                lambda row, col: (
                    round(lat_top - row * lat_scale, 5),
                    round(lon_left + col * lon_scale, 5),
                ),
                include_point=(lambda lat, lon: not areas or any(
                    area.contains(lat, lon) for area in areas
                )),
            ),
        })
    observations = []
    frame_id = f"aemet_radar:{timestamp:.0f}"
    for component in components:
        lat = lat_top - component.centroid_row * lat_scale
        lon = lon_left + component.centroid_col * lon_scale
        if areas and not any(area.contains(float(lat), float(lon)) for area in areas):
            continue
        lat_km = lat_scale * 110.574
        lon_km = lon_scale * 111.320 * max(0.1, abs(np.cos(np.radians(lat))))
        area_km2 = len(component.pixels) * lat_km * lon_km
        component_id = f"{frame_id}:c{component.index}"
        observations.append(Observation(
            obs_type=ObservationType.RADAR, lat=float(lat), lon=float(lon),
            timestamp=timestamp, intensity=component.max_intensity,
            area_km2=area_km2, quality=0.98,
            footprint_points=component.boundary,
            radar_cell_id=component_id, parent_system_id=component_id,
            parent_area_km2=area_km2,
            parent_footprint_points=component.boundary,
            source="aemet_radar",
        ))
    return observations


class AemetRadarProvider:
    plugin_id = "aemet_radar"
    capabilities = frozenset({Capability.RADAR})
    priority = 99

    def __init__(self, session):
        self._session, self._areas, self._last_name = session, (), None
        self.diagnostics = {}
        self.overlay = None

    def supports(self, area):
        margin = area.horizon_km / 90.0
        ok = 33.0 - margin <= area.center_lat <= 46.5 + margin and -12.2 - margin <= area.center_lon <= 6.2 + margin
        return CoverageResult(ok, 1.0 if ok else 0.0, 0.98 if ok else 0.0, "AEMET Spanje" if ok else "buiten AEMET-dekking")

    async def async_start(self, context): self._areas = tuple(context.config.get("areas", (context.area,)))
    async def async_update_areas(self, areas): self._areas = tuple(areas)
    async def async_stop(self): self._areas = ()

    async def async_fetch(self):
        async with self._session.get(DOWNLOAD_URL) as response:
            response.raise_for_status()
            if response.content_length and response.content_length > MAX_ARCHIVE_BYTES:
                raise ValueError("AEMET-bundel overschrijdt veiligheidslimiet")
            payload = await response.read()
        if len(payload) > MAX_ARCHIVE_BYTES:
            raise ValueError("AEMET-bundel overschrijdt veiligheidslimiet")
        frame, timestamp, name = await asyncio.to_thread(latest_frame_from_archive, payload)
        if name == self._last_name:
            return []
        overlays = []
        observations = await asyncio.to_thread(
            parse_aemet_geotiff, frame, self._areas, timestamp=timestamp,
            overlay_out=overlays,
        )
        self.overlay = overlays[0] if overlays else None
        self._last_name = name
        self.diagnostics = {
            "frame": name, "frame_timestamp": timestamp,
            "frame_age_seconds": round(datetime.now(timezone.utc).timestamp() - timestamp, 1),
            "observations": len(observations), "source_role": "primary_official_radar",
        }
        _LOGGER.info("AEMET-composiet: %d observaties binnen actieve engines", len(observations))
        return observations
