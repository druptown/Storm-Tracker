"""NOAA GOES-18/19 Geostationary Lightning Mapper fallback-provider."""
from __future__ import annotations

import asyncio
from collections import deque
from datetime import datetime, timedelta, timezone
import io
import logging
import math
import time
from urllib.parse import quote
import xml.etree.ElementTree as ET

import h5py
import numpy as np

from ..engine.observation import Observation, ObservationType

_LOGGER = logging.getLogger(__name__)

SATELLITES = (18, 19)
PRODUCT_PREFIX = "GLM-L2-LCFA"
MAX_FILE_BYTES = 5 * 1024 * 1024
MAX_FILES_PER_POLL = 12
MAX_FLASH_AGE_S = 5 * 60


def preferred_source_for_longitude(lon: float) -> str | None:
    """Verdeel overlappende GEO-dekking zonder dubbele satellietflashes."""
    lon = ((float(lon) + 180) % 360) - 180
    if -37.5 <= lon <= 80:
        return "eumetsat_li"
    if -106 <= lon < -37.5:
        return "noaa_goes19_glm"
    if lon < -106 or lon >= 145:
        return "noaa_goes18_glm"
    # 80E..145E: later op te vullen met FY-4 LMI.
    return None


def satellites_for_regions(
    regions: list[tuple[float, float, float]],
) -> set[int]:
    """Bepaal welke GOES-satellieten minstens één actieve regio bedienen."""
    satellites: set[int] = set()
    for lat, lon, radius_km in regions:
        cos_lat = max(0.05, abs(math.cos(math.radians(float(lat)))))
        margin = min(180.0, max(0.0, float(radius_km)) / (111.32 * cos_lat))
        steps = max(1, math.ceil((2 * margin) / 5.0))
        for step in range(steps + 1):
            sample_lon = float(lon) - margin + (2 * margin * step / steps)
            source = preferred_source_for_longitude(sample_lon)
            if source == "noaa_goes18_glm":
                satellites.add(18)
            elif source == "noaa_goes19_glm":
                satellites.add(19)
    return satellites


def _attr_scalar(value, default=0.0) -> float:
    if value is None:
        return float(default)
    array = np.asarray(value).reshape(-1)
    return float(array[0]) if array.size else float(default)


def _text_attr(value) -> str:
    if isinstance(value, bytes):
        return value.decode("ascii")
    if isinstance(value, np.bytes_):
        return bytes(value).decode("ascii")
    return str(value)


def _decode_cf(dataset) -> list[float]:
    raw = np.asarray(dataset[...]).reshape(-1)
    scale = _attr_scalar(dataset.attrs.get("scale_factor"), 1.0)
    offset = _attr_scalar(dataset.attrs.get("add_offset"), 0.0)
    unsigned = _text_attr(dataset.attrs.get("_Unsigned", "false")).lower() == "true"
    bit_count = dataset.dtype.itemsize * 8
    values = []
    for item in raw:
        scalar = item.item() if hasattr(item, "item") else item
        if unsigned and dataset.dtype.kind == "i" and scalar < 0:
            scalar += 1 << bit_count
        values.append(float(scalar) * scale + offset)
    return values


def _time_epoch_from_units(dataset) -> float:
    units = _text_attr(dataset.attrs.get("units", ""))
    marker = "seconds since "
    if not units.startswith(marker):
        raise ValueError(f"Onbekende GOES flashtijdeenheid: {units}")
    value = units[len(marker):].strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def parse_goes_flashes(payload: bytes, satellite: int) -> list[Observation]:
    """Parseer één GLM-L2-LCFA NetCDF-bestand naar flash-observaties."""
    if satellite not in SATELLITES:
        raise ValueError(f"Onbekende GOES-satelliet: {satellite}")
    with h5py.File(io.BytesIO(payload), "r") as nc:
        required = ("flash_lat", "flash_lon", "flash_time_offset_of_first_event")
        if any(name not in nc for name in required):
            raise ValueError("GOES GLM NetCDF mist vereiste flashvelden")
        latitudes = _decode_cf(nc["flash_lat"])
        longitudes = _decode_cf(nc["flash_lon"])
        time_ds = nc["flash_time_offset_of_first_event"]
        timestamps = [
            _time_epoch_from_units(time_ds) + offset
            for offset in _decode_cf(time_ds)
        ]
    if not (len(latitudes) == len(longitudes) == len(timestamps)):
        raise ValueError("GOES GLM flasharrays hebben ongelijke lengtes")
    source = f"noaa_goes{satellite}_glm"
    return [
        Observation(
            obs_type=ObservationType.LIGHTNING,
            lat=lat,
            lon=lon,
            timestamp=timestamp,
            source=source,
        )
        for lat, lon, timestamp in zip(latitudes, longitudes, timestamps)
        if -90 <= lat <= 90 and -180 <= lon <= 180
    ]


class NoaaGoesGlmProvider:
    """Anonieme client voor de openbare NOAA GOES S3-buckets."""

    def __init__(self, session) -> None:
        self._session = session
        self._processed: set[str] = set()
        self._processed_order: deque[str] = deque()
        self.status = {18: "initializing", 19: "initializing"}

    async def _list_recent_keys(self, satellite: int) -> list[str]:
        now = datetime.now(timezone.utc)
        keys: set[str] = set()
        bucket = f"https://noaa-goes{satellite}.s3.amazonaws.com/"
        for moment in (now - timedelta(hours=1), now):
            prefix = f"{PRODUCT_PREFIX}/{moment.year}/{moment.timetuple().tm_yday:03d}/{moment.hour:02d}/"
            async with self._session.get(
                bucket,
                params={"list-type": "2", "prefix": prefix, "max-keys": "1000"},
            ) as response:
                response.raise_for_status()
                root = ET.fromstring(await response.text())
            keys.update(
                node.text for node in root.findall(".//{*}Key") if node.text
            )
        return sorted(keys)[-MAX_FILES_PER_POLL:]

    async def _download(self, satellite: int, key: str) -> bytes:
        url = f"https://noaa-goes{satellite}.s3.amazonaws.com/{quote(key, safe='/')}"
        chunks = []
        total = 0
        async with self._session.get(url) as response:
            response.raise_for_status()
            if response.content_length and response.content_length > MAX_FILE_BYTES:
                raise ValueError(f"GOES GLM-bestand te groot: {response.content_length}")
            async for chunk in response.content.iter_chunked(64 * 1024):
                total += len(chunk)
                if total > MAX_FILE_BYTES:
                    raise ValueError("GOES GLM-bestand overschrijdt veiligheidslimiet")
                chunks.append(chunk)
        return b"".join(chunks)

    def _remember(self, key: str) -> None:
        if key in self._processed:
            return
        self._processed.add(key)
        self._processed_order.append(key)
        while len(self._processed_order) > 200:
            self._processed.discard(self._processed_order.popleft())

    async def _fetch_satellite(self, satellite: int) -> list[Observation]:
        try:
            keys = await self._list_recent_keys(satellite)
            new_keys = [key for key in keys if key not in self._processed]
            observations = []
            for key in new_keys:
                payload = await self._download(satellite, key)
                observations.extend(parse_goes_flashes(payload, satellite))
                self._remember(key)
            cutoff = time.time() - MAX_FLASH_AGE_S
            recent = [obs for obs in observations if obs.timestamp >= cutoff]
            self.status[satellite] = "active" if keys else "unavailable"
            if new_keys:
                _LOGGER.info(
                    "NOAA GOES-%d GLM: %d recente flashes uit %d bestanden",
                    satellite, len(recent), len(new_keys),
                )
            return recent
        except Exception:
            self.status[satellite] = "error"
            _LOGGER.exception("NOAA GOES-%d GLM ophalen mislukt", satellite)
            return []

    async def fetch_observations(
        self, satellites: set[int] | None = None,
    ) -> list[Observation]:
        selected = set(SATELLITES if satellites is None else satellites)
        for satellite in SATELLITES:
            if satellite not in selected:
                self.status[satellite] = "standby"
        results = await asyncio.gather(*(
            self._fetch_satellite(satellite)
            for satellite in SATELLITES
            if satellite in selected
        ))
        return [observation for result in results for observation in result]
