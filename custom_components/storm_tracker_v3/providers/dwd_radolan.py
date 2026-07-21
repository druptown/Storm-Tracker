"""DWD RADOLAN/RADVOR RV nationale radarprovider voor Duitsland."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import io
import logging
import tarfile

import h5py
import numpy as np
from pyproj import CRS, Transformer

from ..engine.observation import Observation, ObservationType
from .base import Capability, CoverageArea, CoverageResult
from .raster_components import extract_components, extract_intensity_runs

_LOGGER = logging.getLogger(__name__)

RV_LATEST_URL = (
    "https://opendata.dwd.de/weather/radar/composite/rv/"
    "composite_rv_LATEST.tar"
)
MAX_ARCHIVE_BYTES = 8 * 1024 * 1024
MAX_FRAME_AGE_SECONDS = 15 * 60


def _text(value) -> str:
    return value.decode("ascii") if isinstance(value, bytes) else str(value)


def _intensity(rain_rate: float) -> int:
    if rain_rate < 0.1:
        return 0
    for level, threshold in enumerate((0.1, 0.5, 1, 2, 5, 10, 25), start=1):
        if rain_rate < threshold:
            return max(1, level - 1)
    return 8


def parse_rv_archive(
    payload: bytes,
    areas: tuple[CoverageArea, ...],
    *,
    now: float | None = None,
    overlay_out: list | None = None,
) -> list[Observation]:
    """Parseer uitsluitend het actuele (+000) RV HDF5-frame."""
    with tarfile.open(fileobj=io.BytesIO(payload), mode="r:") as archive:
        members = [
            item for item in archive.getmembers()
            if item.isfile() and item.name.endswith("_000-hd5")
        ]
        if not members:
            raise ValueError("DWD RV-archief bevat geen actueel HDF5-frame")
        stream = archive.extractfile(sorted(members, key=lambda item: item.name)[-1])
        frame = stream.read()

    with h5py.File(io.BytesIO(frame), "r") as dataset:
        data = np.asarray(dataset["dataset1/data1/data"])
        data_what = dataset["dataset1/data1/what"].attrs
        frame_what = dataset["dataset1/what"].attrs
        where = dataset["where"].attrs
        timestamp = datetime.strptime(
            _text(frame_what["enddate"]) + _text(frame_what["endtime"]),
            "%Y%m%d%H%M%S",
        ).replace(tzinfo=timezone.utc).timestamp()
        reference_now = datetime.now(timezone.utc).timestamp() if now is None else now
        if reference_now - timestamp > MAX_FRAME_AGE_SECONDS:
            raise ValueError("DWD RV-frame is ouder dan 15 minuten")

        gain = float(data_what["gain"])
        offset = float(data_what["offset"])
        nodata = float(data_what["nodata"])
        undetect = float(data_what["undetect"])
        raw = data.astype(np.float64)
        decoded = raw * gain + offset
        rain_rate = decoded * 12.0  # ACRR is vijfminutenaccumulatie -> mm/u
        valid = (
            (raw != nodata)
            & (raw != undetect)
            & (rain_rate >= 0.1)
        )
        if not np.any(valid):
            return []
        xscale = float(where["xscale"])
        yscale = float(where["yscale"])
        ysize = int(where["ysize"])
        transformer = Transformer.from_crs(
            CRS.from_user_input(_text(where["projdef"])), "EPSG:4326",
            always_xy=True,
        )
        intensity_grid = np.zeros(data.shape, dtype=np.uint8)
        for row, column in np.argwhere(valid):
            intensity_grid[row, column] = _intensity(
                float(rain_rate[row, column])
            )

        def corner_to_latlon(row, column):
            lon, lat = transformer.transform(
                column * xscale,
                (ysize - row) * yscale,
            )
            return round(float(lat), 5), round(float(lon), 5)

        components = extract_components(intensity_grid, corner_to_latlon)
        if overlay_out is not None:
            overlay_out.append({
                "source": "dwd_radolan", "timestamp": timestamp,
                "runs": extract_intensity_runs(
                    intensity_grid, corner_to_latlon,
                    include_point=(lambda lat, lon: not areas or any(
                        area.contains(lat, lon) for area in areas
                    )),
                ),
            })

    observations = []
    frame_id = f"dwd_radolan:{timestamp:.0f}"
    pixel_area_km2 = xscale * yscale / 1_000_000.0
    for component in components:
        lon, lat = transformer.transform(
            component.centroid_col * xscale,
            (ysize - component.centroid_row) * yscale,
        )
        if areas and not any(area.contains(float(lat), float(lon)) for area in areas):
            continue
        area_km2 = len(component.pixels) * pixel_area_km2
        component_id = f"{frame_id}:c{component.index}"
        observations.append(Observation(
            obs_type=ObservationType.RADAR,
            lat=float(lat),
            lon=float(lon),
            timestamp=timestamp,
            intensity=component.max_intensity,
            area_km2=area_km2,
            quality=0.98,
            footprint_points=component.boundary,
            radar_cell_id=component_id,
            parent_system_id=component_id,
            parent_area_km2=area_km2,
            parent_footprint_points=component.boundary,
            source="dwd_radolan",
        ))
    return observations


class DwdRadolanProvider:
    plugin_id = "dwd_radolan"
    capabilities = frozenset({Capability.RADAR})
    priority = 100

    def __init__(self, session) -> None:
        self._session = session
        self._areas: tuple[CoverageArea, ...] = ()
        self.overlay = None

    def supports(self, area: CoverageArea) -> CoverageResult:
        lat_margin = area.horizon_km / 111.0
        lon_margin = area.horizon_km / max(40.0, 111.0)
        supported = (
            45.5 - lat_margin <= area.center_lat <= 56.0 + lat_margin
            and 1.0 - lon_margin <= area.center_lon <= 19.0 + lon_margin
        )
        return CoverageResult(
            supported=supported,
            coverage_fraction=1.0 if supported else 0.0,
            quality=0.98 if supported else 0.0,
            reason="DWD RV 1 km Duitsland" if supported else "buiten DWD-dekking",
        )

    async def async_start(self, context) -> None:
        self._areas = tuple(context.config.get("areas", (context.area,)))

    async def async_update_areas(self, areas) -> None:
        self._areas = tuple(areas)

    async def async_stop(self) -> None:
        self._areas = ()

    async def async_fetch(self) -> list[Observation]:
        async with self._session.get(RV_LATEST_URL) as response:
            response.raise_for_status()
            if response.content_length and response.content_length > MAX_ARCHIVE_BYTES:
                raise ValueError("DWD RV-archief overschrijdt veiligheidslimiet")
            payload = await response.read()
        if len(payload) > MAX_ARCHIVE_BYTES:
            raise ValueError("DWD RV-archief overschrijdt veiligheidslimiet")
        overlays = []
        observations = await asyncio.to_thread(
            parse_rv_archive, payload, self._areas, overlay_out=overlays
        )
        self.overlay = overlays[0] if overlays else None
        _LOGGER.info("DWD RADOLAN: %d observaties binnen actieve engines", len(observations))
        return observations
