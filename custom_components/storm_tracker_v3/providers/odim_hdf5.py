"""Gedeelde decoder voor nationale ODIM-HDF5 radarproducten."""
from __future__ import annotations

from datetime import datetime, timezone
import io

import h5py
import numpy as np
from pyproj import CRS, Transformer

from ..engine.observation import Observation, ObservationType
from .raster_components import extract_components


def _text(value) -> str:
    return value.decode("ascii") if isinstance(value, bytes) else str(value)


def rain_rate_to_intensity(rate: float) -> int:
    if rate < 0.1:
        return 0
    for level, threshold in enumerate((0.1, 0.5, 1, 2, 5, 10, 25), start=1):
        if rate < threshold:
            return max(1, level - 1)
    return 8


def parse_odim_rainfall(
    payload: bytes,
    areas: tuple,
    *,
    source: str,
    quality: float,
    max_age_seconds: float,
    sample_stride: int = 4,
    accumulation_minutes: float | None = None,
    now: float | None = None,
) -> list[Observation]:
    """Decodeer het eerste ODIM-raster naar echte neerslagcomponenten."""
    with h5py.File(io.BytesIO(payload), "r") as dataset:
        data = np.asarray(dataset["dataset1/data1/data"])
        data_what = dataset["dataset1/data1/what"].attrs
        frame_what = dataset["dataset1/what"].attrs
        where = dataset["where"].attrs
        timestamp = datetime.strptime(
            _text(frame_what["enddate"]) + _text(frame_what["endtime"]),
            "%Y%m%d%H%M%S",
        ).replace(tzinfo=timezone.utc).timestamp()
        reference_now = datetime.now(timezone.utc).timestamp() if now is None else now
        if reference_now - timestamp > max_age_seconds:
            raise ValueError(f"{source}-frame is te oud")

        raw = data.astype(np.float64)
        decoded = raw * float(data_what["gain"]) + float(data_what["offset"])
        rate = decoded if accumulation_minutes is None else decoded * (60 / accumulation_minutes)
        valid = (
            (raw != float(data_what["nodata"]))
            & (raw != float(data_what["undetect"]))
            & (rate >= 0.1)
        )
        if not np.any(valid):
            return []

        transformer = Transformer.from_crs(
            CRS.from_user_input(_text(where["projdef"])), "EPSG:4326", always_xy=True
        )
        inverse = Transformer.from_crs(
            "EPSG:4326", CRS.from_user_input(_text(where["projdef"])), always_xy=True
        )
        if "UL_lon" in where and "UL_lat" in where:
            ul_x, ul_y = inverse.transform(float(where["UL_lon"]), float(where["UL_lat"]))
        else:
            ul_x, ul_y = 0.0, int(where["ysize"]) * float(where["yscale"])
        xscale, yscale = float(where["xscale"]), float(where["yscale"])
        intensity_grid = np.zeros(data.shape, dtype=np.uint8)
        for row, column in np.argwhere(valid):
            intensity_grid[row, column] = rain_rate_to_intensity(
                float(rate[row, column])
            )

        def corner_to_latlon(row, column):
            lon, lat = transformer.transform(
                ul_x + column * xscale,
                ul_y - row * yscale,
            )
            return round(float(lat), 5), round(float(lon), 5)

        components = extract_components(intensity_grid, corner_to_latlon)

    observations = []
    frame_id = f"{source}:{timestamp:.0f}"
    pixel_area_km2 = xscale * yscale / 1_000_000.0
    for component in components:
        lon, lat = transformer.transform(
            ul_x + component.centroid_col * xscale,
            ul_y - component.centroid_row * yscale,
        )
        if areas and not any(area.contains(float(lat), float(lon)) for area in areas):
            continue
        area_km2 = len(component.pixels) * pixel_area_km2
        component_id = f"{frame_id}:c{component.index}"
        observations.append(Observation(
            obs_type=ObservationType.RADAR,
            lat=float(lat), lon=float(lon), timestamp=timestamp,
            intensity=component.max_intensity,
            area_km2=area_km2, quality=quality,
            footprint_points=component.boundary,
            radar_cell_id=component_id, parent_system_id=component_id,
            parent_area_km2=area_km2,
            parent_footprint_points=component.boundary,
            source=source,
        ))
    return observations
