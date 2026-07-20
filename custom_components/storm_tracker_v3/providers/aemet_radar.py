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

_LOGGER = logging.getLogger(__name__)
DOWNLOAD_URL = "https://www.aemet.es/es/api-eltiempo/radar/download/compo"
MAX_ARCHIVE_BYTES = 10 * 1024 * 1024
MAX_FRAME_BYTES = 5 * 1024 * 1024
MAX_FRAME_AGE_SECONDS = 25 * 60
SAMPLE_STRIDE = 2
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


def parse_aemet_geotiff(payload: bytes, areas: tuple, *, timestamp: float, now: float | None = None):
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
    sampled = data[::SAMPLE_STRIDE, ::SAMPLE_STRIDE]
    rows, columns = np.nonzero((sampled >= 1) & (sampled <= 8))
    latitudes = float(tie[4]) - (rows * SAMPLE_STRIDE + 0.5) * float(scale[1])
    longitudes = float(tie[3]) + (columns * SAMPLE_STRIDE + 0.5) * float(scale[0])
    observations = []
    for row, column, lat, lon in zip(rows, columns, latitudes, longitudes):
        if areas and not any(area.contains(float(lat), float(lon)) for area in areas):
            continue
        observations.append(Observation(
            obs_type=ObservationType.RADAR, lat=float(lat), lon=float(lon),
            timestamp=timestamp, intensity=int(sampled[row, column]),
            area_km2=25.0, quality=0.98, source="aemet_radar",
        ))
    return observations


class AemetRadarProvider:
    plugin_id = "aemet_radar"
    capabilities = frozenset({Capability.RADAR})
    priority = 99

    def __init__(self, session):
        self._session, self._areas, self._last_name = session, (), None
        self.diagnostics = {}

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
        observations = await asyncio.to_thread(
            parse_aemet_geotiff, frame, self._areas, timestamp=timestamp
        )
        self._last_name = name
        self.diagnostics = {
            "frame": name, "frame_timestamp": timestamp,
            "frame_age_seconds": round(datetime.now(timezone.utc).timestamp() - timestamp, 1),
            "observations": len(observations), "source_role": "primary_official_radar",
        }
        _LOGGER.info("AEMET-composiet: %d observaties binnen actieve engines", len(observations))
        return observations
