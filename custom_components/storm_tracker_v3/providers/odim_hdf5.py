"""Gedeelde decoder voor nationale ODIM-HDF5 radarproducten."""
from __future__ import annotations

from datetime import datetime, timezone
import io

import h5py
import numpy as np
from pyproj import CRS, Transformer

from ..engine.observation import Observation, ObservationType


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
    """Decodeer het eerste ODIM-raster naar dun bemonsterde radarobservaties."""
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

        sampled = data[::sample_stride, ::sample_stride].astype(np.float64)
        decoded = sampled * float(data_what["gain"]) + float(data_what["offset"])
        rate = decoded if accumulation_minutes is None else decoded * (60 / accumulation_minutes)
        valid = (
            (sampled != float(data_what["nodata"]))
            & (sampled != float(data_what["undetect"]))
            & (rate >= 0.1)
        )
        rows, columns = np.nonzero(valid)
        if not len(rows):
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
        x = ul_x + (columns * sample_stride + 0.5) * float(where["xscale"])
        y = ul_y - (rows * sample_stride + 0.5) * float(where["yscale"])
        longitudes, latitudes = transformer.transform(x, y)

    observations = []
    for lat, lon, rain_rate in zip(latitudes, longitudes, rate[rows, columns]):
        if areas and not any(area.contains(float(lat), float(lon)) for area in areas):
            continue
        observations.append(Observation(
            obs_type=ObservationType.RADAR,
            lat=float(lat), lon=float(lon), timestamp=timestamp,
            intensity=rain_rate_to_intensity(float(rain_rate)),
            area_km2=float(sample_stride ** 2), quality=quality, source=source,
        ))
    return observations
