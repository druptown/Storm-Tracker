from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
import io
import json
import logging
from pathlib import Path
import runpy
import tarfile

import h5py
import numpy as np
import pytest
from pyproj import CRS

import storm_tracker_worker.thin_provider_adapters as provider_adapters
from storm_tracker_worker.desired_state import Target
from storm_tracker_worker.thin_observation import ObservationKind
from storm_tracker_worker.thin_provider_adapters import (
    AemetRadarAdapter,
    BlitzortungAdapter,
    DwdRadolanAdapter,
    KmiRadarAdapter,
    KnmiRadarAdapter,
    MetOfficeRadarAdapter,
    MetarAdapter,
    NetatmoAdapter,
    NetatmoOAuthTokenProvider,
    OpenMeteoAdapter,
    OperaRadarAdapter,
    ProviderBlocked,
    ProviderStale,
    RainViewerRadarAdapter,
    WundergroundAdapter,
    _component_polygon_geometry,
)
from storm_tracker_worker.thin_provider_catalog import ThinProviderRequest
from storm_tracker_worker.thin_region_planner import ThinAcquisitionRegion


PRODUCT_TIME = datetime(2026, 7, 12, 12, 0, tzinfo=timezone.utc).timestamp()


def test_component_polygon_splits_a_pinched_hole_into_a_valid_ring():
    """A one-point shell/hole contact must not create a self-intersection."""

    geometry = _component_polygon_geometry(
        [
            (0, 0), (0, 1), (0, 2),
            (1, 0),         (1, 2),
            (2, 0), (2, 1),
        ],
        lambda row, column: (float(column), float(row)),
    )

    assert geometry["type"] == "Polygon"
    assert len(geometry["coordinates"]) == 2
    for ring in geometry["coordinates"]:
        assert ring[0] == ring[-1]
        assert len({tuple(point) for point in ring[:-1]}) == len(ring) - 1


def _target(
    target_id: str,
    latitude: float,
    longitude: float,
    *,
    radius_km: float = 50.0,
) -> Target:
    return Target(
        target_id=target_id,
        display_name=target_id,
        kind="test",
        available=True,
        latitude=latitude,
        longitude=longitude,
        position_observed_at=PRODUCT_TIME,
        location_accuracy_m=5.0,
        observation_radius_km=radius_km,
        country_code="BE",
    )


def _request(provider_id: str, product_id: str, *targets: Target) -> ThinProviderRequest:
    return ThinProviderRequest(
        provider_id=provider_id,
        product_id=product_id,
        targets=targets,
        requested_at=PRODUCT_TIME + 60.0,
    )


class FixtureFetcher:
    def __init__(self, *, json_by_url=None, bytes_by_url=None, post_json_by_url=None):
        self.json_by_url = dict(json_by_url or {})
        self.bytes_by_url = dict(bytes_by_url or {})
        self.post_json_by_url = dict(post_json_by_url or {})
        self.calls = []

    async def get_json(self, url, *, params=None, headers=None, maximum_bytes=None):
        self.calls.append(("json", url, params, headers, maximum_bytes))
        return self.json_by_url[url]

    async def get_bytes(self, url, *, params=None, headers=None, maximum_bytes=None):
        self.calls.append(("bytes", url, params, headers, maximum_bytes))
        return self.bytes_by_url[url]

    async def post_json(
        self,
        url,
        *,
        data=None,
        json_data=None,
        headers=None,
        maximum_bytes=None,
    ):
        body = data if data is not None else json_data
        self.calls.append(("post_json", url, body, headers, maximum_bytes))
        return self.post_json_by_url[url]


def _aemet_archive(observed: datetime, *, wet: bool = True) -> bytes:
    from PIL import Image, TiffImagePlugin

    values = np.zeros((16, 16), dtype=np.uint8)
    if wet:
        values[7:10, 7:10] = 6
    image = Image.fromarray(values, mode="P")
    tags = TiffImagePlugin.ImageFileDirectory_v2()
    tags[33922] = (0.0, 0.0, 0.0, -4.0, 44.0, 0.0)
    tags[33550] = (1.0, 1.0, 0.0)
    frame = io.BytesIO()
    image.save(frame, format="TIFF", tiffinfo=tags)
    archive = io.BytesIO()
    name = f"down_radw{observed.strftime('%Y%m%d%H%M')}_4326.tif"
    with tarfile.open(fileobj=archive, mode="w:gz") as bundle:
        info = tarfile.TarInfo(name)
        info.size = len(frame.getvalue())
        bundle.addfile(info, io.BytesIO(frame.getvalue()))
    return archive.getvalue()


def _meteofrance_hdf5(observed: datetime, *, wet: bool = True) -> bytes:
    from pyproj import Transformer

    values = np.zeros((16, 16), dtype=np.uint16)
    if wet:
        values[7:10, 7:10] = 100
    center_x, center_y = Transformer.from_crs(
        "EPSG:4326", "EPSG:2154", always_xy=True
    ).transform(2.3522, 48.8566)
    upper_left_x = center_x - 8 * 500.0
    upper_left_y = center_y + 8 * 500.0
    upper_left_lon, upper_left_lat = Transformer.from_crs(
        "EPSG:2154", "EPSG:4326", always_xy=True
    ).transform(upper_left_x, upper_left_y)
    payload = io.BytesIO()
    with h5py.File(payload, "w") as product:
        where = product.create_group("where")
        where.attrs["projdef"] = "EPSG:2154"
        where.attrs["xscale"] = 500.0
        where.attrs["yscale"] = 500.0
        where.attrs["xsize"] = 16
        where.attrs["ysize"] = 16
        where.attrs["UL_lon"] = upper_left_lon
        where.attrs["UL_lat"] = upper_left_lat
        dataset = product.create_group("dataset1")
        dataset_what = dataset.create_group("what")
        dataset_what.attrs["enddate"] = observed.strftime("%Y%m%d")
        dataset_what.attrs["endtime"] = observed.strftime("%H%M%S")
        data = dataset.create_group("data1")
        data.create_dataset("data", data=values)
        data_what = data.create_group("what")
        data_what.attrs["gain"] = 0.01
        data_what.attrs["offset"] = 0.0
        data_what.attrs["nodata"] = 65535
        data_what.attrs["undetect"] = 0
    return payload.getvalue()


def _dpc_sri_geotiff(*, wet: bool = True) -> bytes:
    from PIL import Image, TiffImagePlugin
    from pyproj import Transformer

    crs = (
        "+proj=lcc +lat_0=42 +lon_0=12.5 +lat_1=42 +lat_2=42 "
        "+x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    )
    center_x, center_y = Transformer.from_crs(
        "EPSG:4326", crs, always_xy=True
    ).transform(12.4964, 41.9028)
    values = np.full((16, 16), -9999.0, dtype=np.float32)
    values[4:12, 4:12] = 0.0
    if wet:
        values[7:10, 7:10] = 12.0
    image = Image.fromarray(values, mode="F")
    tags = TiffImagePlugin.ImageFileDirectory_v2()
    tags[33922] = (
        0.0,
        0.0,
        0.0,
        center_x - 8_000.0,
        center_y + 8_000.0,
        0.0,
    )
    tags[33550] = (1_000.0, 1_000.0, 0.0)
    payload = io.BytesIO()
    image.save(payload, format="TIFF", tiffinfo=tags)
    return payload.getvalue()


def _metoffice_hdf5(observed: datetime) -> bytes:
    from pyproj import Transformer

    rows, columns = 2175, 1725
    origin_x, origin_y = -405_000.0, 1_550_000.0
    x, y = Transformer.from_crs(
        "EPSG:4326",
        "+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.999601 +x_0=400000 "
        "+y_0=-100000 +ellps=airy +units=m",
        always_xy=True,
    ).transform(-0.1278, 51.5074)
    row = int((origin_y - y) / 1_000.0)
    column = int((x - origin_x) / 1_000.0)
    values = np.zeros((rows, columns), dtype=np.float32)
    values[max(0, row - 2):row + 3, max(0, column - 2):column + 3] = 4.5
    payload = io.BytesIO()
    with h5py.File(payload, "w") as product:
        what = product.create_group("what")
        what.attrs["date"] = observed.strftime("%Y%m%d")
        what.attrs["time"] = observed.strftime("%H%M%S")
        where = product.create_group("where")
        where.attrs["projdef"] = (
            "+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.999601 +x_0=400000 "
            "+y_0=-100000 +ellps=airy +units=m"
        )
        where.attrs["xscale"] = 1_000.0
        where.attrs["yscale"] = 1_000.0
        where.attrs["xsize"] = columns
        where.attrs["ysize"] = rows
        dataset = product.create_group("dataset1")
        data = dataset.create_group("data1")
        data.create_dataset("data", data=values, compression="gzip")
        data_what = data.create_group("what")
        data_what.attrs["gain"] = 1.0
        data_what.attrs["offset"] = 0.0
        data_what.attrs["nodata"] = -1.0
        data_what.attrs["undetect"] = 0.0
        data_what.attrs["quantity"] = "RATE"
    return payload.getvalue()


def _empty_png() -> bytes:
    from PIL import Image

    payload = io.BytesIO()
    Image.new("RGBA", (256, 256), (0, 0, 0, 0)).save(payload, format="PNG")
    return payload.getvalue()


class StatusResponse:
    def __init__(self, status: int) -> None:
        self.status = status
        self.headers = {}
        self.content = self

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        return False

    async def iter_chunked(self, size):
        if False:
            yield b""


class StatusSession:
    def __init__(self, status: int) -> None:
        self.status = status

    def get(self, *args, **kwargs):
        return StatusResponse(self.status)


class StatusJsonFetcher(FixtureFetcher):
    def __init__(self, status: int) -> None:
        super().__init__()
        self.status = status

    async def get_json(self, url, *, params=None, headers=None, maximum_bytes=None):
        error_type = getattr(provider_adapters, "HttpStatusError", None)
        assert error_type is not None, "typed HTTP status errors are required"
        raise error_type(self.status)


def _dwd_archive(
    timestamp: datetime,
    *,
    wet: bool = True,
    l_shaped: bool = False,
) -> bytes:
    hdf = io.BytesIO()
    with h5py.File(hdf, "w") as root:
        dataset = root.create_group("dataset1")
        frame = dataset.create_group("what")
        frame.attrs["enddate"] = np.bytes_(timestamp.strftime("%Y%m%d").encode())
        frame.attrs["endtime"] = np.bytes_(timestamp.strftime("%H%M%S").encode())
        data1 = dataset.create_group("data1")
        values = np.zeros((16, 16), dtype=np.uint32)
        if l_shaped:
            values[0:10, 0:5] = 1000
            values[5:10, 0:10] = 1000
        elif wet:
            values[0:4, 0:4] = 1000
        data1.create_dataset("data", data=values)
        what = data1.create_group("what")
        what.attrs["gain"] = 0.001
        what.attrs["offset"] = -0.001
        what.attrs["nodata"] = float(2**32 - 1)
        what.attrs["undetect"] = 0.0
        where = root.create_group("where")
        where.attrs["projdef"] = np.bytes_(
            b"+proj=stere +lat_ts=60 +lat_0=90 +lon_0=10 "
            b"+x_0=543196.835217764 +y_0=3622588.861931002 "
            b"+units=m +a=6378137 +b=6356752.3142451802 +no_defs"
        )
        where.attrs["xscale"] = 1000.0
        where.attrs["yscale"] = 1000.0
        where.attrs["xsize"] = 16
        where.attrs["ysize"] = 16
        where.attrs["UL_lon"] = 4.35
        where.attrs["UL_lat"] = 50.85
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w") as output:
        payload = hdf.getvalue()
        info = tarfile.TarInfo(
            f"composite_rv_{timestamp:%Y%m%d}_{timestamp:%H%M}_000-hd5"
        )
        info.size = len(payload)
        output.addfile(info, io.BytesIO(payload))
    return archive.getvalue()


def _knmi_rtcor_hdf(timestamp: datetime, *, wet: bool = True) -> bytes:
    stream = io.BytesIO()
    with h5py.File(stream, "w") as root:
        overview = root.create_group("overview")
        overview.attrs["product_group_name"] = np.bytes_(b"RAD_NL25_COR_NA")
        overview.attrs["product_datetime_end"] = np.bytes_(
            timestamp.strftime("%d-%b-%Y;%H:%M:%S.000").upper().encode()
        )
        geographic = root.create_group("geographic")
        # The live KNMI product stores these numeric attributes as one-item
        # arrays rather than HDF scalar attributes.
        geographic.attrs["geo_number_columns"] = np.array([700], dtype=np.int32)
        geographic.attrs["geo_number_rows"] = np.array([765], dtype=np.int32)
        geographic.attrs["geo_pixel_size_x"] = np.array([1.0000035], dtype=np.float32)
        geographic.attrs["geo_pixel_size_y"] = np.array([-1.0000049], dtype=np.float32)
        geographic.attrs["geo_column_offset"] = np.array([0.0], dtype=np.float32)
        geographic.attrs["geo_row_offset"] = np.array([3649.9814], dtype=np.float32)
        projection = geographic.create_group("map_projection")
        projection.attrs["projection_proj4_params"] = np.bytes_(
            b"+proj=stere +lat_0=90 +lon_0=0 +lat_ts=60 "
            b"+a=6378137 +b=6356752 +x_0=0 +y_0=0 +units=km"
        )
        image = root.create_group("image1")
        image.attrs["image_product_name"] = np.bytes_(b"RAD_NL25_COR_NA")
        image.attrs["image_geo_parameter"] = np.bytes_(b"PRECIP_[MM]")
        calibration = image.create_group("calibration")
        calibration.attrs["calibration_formulas"] = np.bytes_(
            b"GEO=0.010000*PV+0.000000"
        )
        calibration.attrs["calibration_missing_data"] = np.array([65534], dtype=np.uint16)
        calibration.attrs["calibration_out_of_image"] = np.array([65535], dtype=np.uint16)
        values = np.full((765, 700), 100 if wet else 0, dtype=np.uint16)
        image.create_dataset("image_data", data=values, compression="gzip")
    return stream.getvalue()


@pytest.mark.asyncio
async def test_open_meteo_fixture_becomes_exact_target_observations():
    payload = {
        "latitude": 50.85,
        "longitude": 4.35,
        "current": {"time": "2026-07-12T12:00", "precipitation": 1.25},
        "minutely_15": {
            "time": ["2026-07-12T12:00", "2026-07-12T12:15"],
            "precipitation": [1.25, 0.5],
        },
        "hourly": {
            "time": ["2026-07-12T12:00"],
            "precipitation_probability": [70.0],
            "pressure_msl": [1008.2],
        },
    }
    fetcher = FixtureFetcher(json_by_url={"open-meteo": payload})
    adapter = OpenMeteoAdapter(endpoint="open-meteo")

    observations = await adapter.fetch(
        _request("open_meteo", "forecast", _target("home", 50.85, 4.35)),
        fetcher,
        now=PRODUCT_TIME + 60.0,
    )

    by_kind = {item.kind: item for item in observations}
    assert by_kind[ObservationKind.RAIN_RATE].value == 5.0
    assert by_kind[ObservationKind.RAIN_RATE].unit == "mm/h"
    assert by_kind[ObservationKind.RAIN_PROBABILITY].value == 70.0
    assert by_kind[ObservationKind.PRESSURE].value == 1008.2
    assert all(item.target_id == "home" for item in observations)


@pytest.mark.asyncio
async def test_kmi_aws_keeps_latest_nearby_station_values_and_normalizes_units():
    adapter_type = getattr(provider_adapters, "KmiAwsAdapter", None)
    assert adapter_type is not None, "public KMI AWS adapter is required"
    qc = json.dumps({
        "validated": {
            "PRECIP_QUANTITY": True,
            "TEMP_DRY_SHELTER_AVG": True,
            "WIND_SPEED_10M": True,
            "WIND_DIRECTION": True,
            "WIND_GUSTS_SPEED": True,
            "HUMIDITY_REL_SHELTER_AVG": True,
            "PRESSURE": True,
        }
    })
    payload = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [4.42, 51.05]},
                "properties": {
                    "code": 6438,
                    "timestamp": "2026-07-12T12:00:00Z",
                    "precip_quantity": 0.5,
                    "temp_dry_shelter_avg": 18.25,
                    "wind_speed_10m": 4.0,
                    "wind_direction": 225.0,
                    "wind_gusts_speed": 7.5,
                    "humidity_rel_shelter_avg": 82.0,
                    "pressure": 1007.4,
                    "qc_flags": qc,
                },
            },
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [4.42, 51.05]},
                "properties": {
                    "code": 6438,
                    "timestamp": "2026-07-12T11:50:00Z",
                    "precip_quantity": 9.0,
                },
            },
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [5.8, 50.0]},
                "properties": {
                    "code": 6472,
                    "timestamp": "2026-07-12T12:00:00Z",
                    "precip_quantity": 8.0,
                    "qc_flags": qc,
                },
            },
        ],
    }
    fetcher = FixtureFetcher(json_by_url={"kmi-aws": payload})
    adapter = adapter_type(endpoint="kmi-aws")

    observations = await adapter.fetch(
        _request(
            "kmi_aws",
            "aws_10min",
            _target("home", 51.05, 4.42, radius_km=50.0),
        ),
        fetcher,
        now=PRODUCT_TIME + 60.0,
    )

    assert {item.metadata["station_id"] for item in observations} == {"6438"}
    by_measurement = {
        item.metadata["measurement"]: item for item in observations
    }
    assert by_measurement["rain_accumulation_10min"].kind is ObservationKind.RAIN_ACCUMULATION
    assert by_measurement["rain_accumulation_10min"].value == 0.5
    assert by_measurement["rain_rate_10min_equivalent"].kind is ObservationKind.RAIN_RATE
    assert by_measurement["rain_rate_10min_equivalent"].value == 3.0
    assert by_measurement["temperature"].value == 18.25
    assert by_measurement["pressure"].value == 1007.4
    assert by_measurement["wind_speed"].value == 4.0
    assert by_measurement["wind_gust"].value == 7.5
    assert by_measurement["wind_direction"].value == 225.0
    assert by_measurement["relative_humidity"].value == 82.0
    assert all(item.quality.value == "good" for item in observations)
    assert fetcher.calls == [(
        "json",
        "kmi-aws",
        {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeNames": "aws:aws_10min",
            "count": "512",
            "outputFormat": "application/json",
            "sortBy": "timestamp D",
            "srsName": "EPSG:4326",
        },
        None,
        32 * 1024 * 1024,
    )]


@pytest.mark.asyncio
async def test_kmi_aws_unvalidated_live_value_is_provisional_not_suspect():
    payload = {
        "type": "FeatureCollection",
        "features": [{
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [4.42, 51.05]},
            "properties": {
                "code": 6438,
                "timestamp": "2026-07-12T12:00:00Z",
                "precip_quantity": 0.2,
                "qc_flags": json.dumps({
                    "validated": {"PRECIP_QUANTITY": False}
                }),
            },
        }],
    }
    fetcher = FixtureFetcher(json_by_url={"kmi-aws": payload})

    observations = await provider_adapters.KmiAwsAdapter(
        endpoint="kmi-aws"
    ).fetch(
        _request(
            "kmi_aws",
            "aws_10min",
            _target("home", 51.05, 4.42, radius_km=50.0),
        ),
        fetcher,
        now=PRODUCT_TIME + 60.0,
    )

    assert observations
    assert {item.quality.value for item in observations} == {"unknown"}


@pytest.mark.asyncio
async def test_italiameteo_icon2i_normalizes_current_target_guidance():
    adapter_type = getattr(provider_adapters, "ItaliaMeteoIcon2iAdapter", None)
    assert adapter_type is not None, "ItaliaMeteo ICON-2I adapter is required"
    payload = {
        "latitude": 41.9,
        "longitude": 12.5,
        "current_units": {
            "interval": "seconds",
            "precipitation": "mm",
            "pressure_msl": "hPa",
            "wind_speed_10m": "km/h",
            "wind_direction_10m": "°",
            "wind_gusts_10m": "km/h",
        },
        "current": {
            "time": "2026-07-12T12:00",
            "interval": 900,
            "precipitation": 1.2,
            "pressure_msl": 1006.4,
            "wind_speed_10m": 36.0,
            "wind_direction_10m": 210.0,
            "wind_gusts_10m": 54.0,
        },
    }
    fetcher = FixtureFetcher(json_by_url={"open-meteo-icon2i": payload})
    adapter = adapter_type(endpoint="open-meteo-icon2i")

    observations = await adapter.fetch(
        _request(
            "italiameteo_icon2i",
            "forecast",
            _target("rome", 41.9028, 12.4964),
        ),
        fetcher,
        now=PRODUCT_TIME + 60.0,
    )

    by_measurement = {
        item.metadata["measurement"]: item for item in observations
    }
    assert by_measurement["precipitation_rate"].kind is ObservationKind.RAIN_RATE
    assert by_measurement["precipitation_rate"].value == 4.8
    assert by_measurement["pressure_msl"].value == 1006.4
    assert by_measurement["wind_speed_10m"].value == 10.0
    assert by_measurement["wind_gusts_10m"].value == 15.0
    assert by_measurement["wind_direction_10m"].value == 210.0
    assert all(item.target_id == "rome" for item in observations)
    assert all(item.metadata["model"] == "italia_meteo_arpae_icon_2i" for item in observations)
    assert fetcher.calls == [(
        "json",
        "open-meteo-icon2i",
        {
            "latitude": "41.90280",
            "longitude": "12.49640",
            "models": "italia_meteo_arpae_icon_2i",
            "current": (
                "precipitation,pressure_msl,wind_speed_10m,"
                "wind_direction_10m,wind_gusts_10m"
            ),
            "timezone": "UTC",
        },
        None,
        2 * 1024 * 1024,
    )]


@pytest.mark.asyncio
async def test_geosphere_normalizes_first_inca_interval_for_target():
    adapter_type = getattr(provider_adapters, "GeoSphereNowcastAdapter", None)
    assert adapter_type is not None, "GeoSphere INCA adapter is required"
    payload = {
        "reference_time": "2026-07-12T12:00+00:00",
        "timestamps": [
            "2026-07-12T12:15+00:00",
            "2026-07-12T12:30+00:00",
        ],
        "features": [{
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [16.3747, 48.2065],
            },
            "properties": {
                "parameters": {
                    "rr": {
                        "name": "precipitation sum",
                        "unit": "kg m-2",
                        "data": [0.8, 0.2],
                    },
                },
            },
        }],
    }
    fetcher = FixtureFetcher(json_by_url={"geosphere-inca": payload})

    observations = await adapter_type(endpoint="geosphere-inca").fetch(
        _request(
            "geosphere",
            "inca_nowcast",
            _target("vienna", 48.2082, 16.3738),
        ),
        fetcher,
        now=PRODUCT_TIME + 60.0,
    )

    assert len(observations) == 1
    observation = observations[0]
    assert observation.kind is ObservationKind.RAIN_RATE
    assert observation.value == pytest.approx(3.2)
    assert observation.unit == "mm/h"
    assert observation.target_id == "vienna"
    assert observation.metadata["forecast_valid_at"] == "2026-07-12T12:15+00:00"
    assert observation.metadata["source_interval_seconds"] == 900
    assert fetcher.calls == [(
        "json",
        "geosphere-inca",
        {
            "parameters": "rr",
            "lat_lon": "48.20820,16.37380",
            "output_format": "geojson",
        },
        None,
        4 * 1024 * 1024,
    )]


@pytest.mark.asyncio
async def test_italiameteo_historical_catalog_validates_without_fusion_observations():
    adapter_type = getattr(
        provider_adapters,
        "ItaliaMeteoDpcHistoricalCatalogAdapter",
        None,
    )
    assert adapter_type is not None, "ItaliaMeteo historical catalog adapter is required"
    payload = [{
        "date": "2026-09-23",
        "run": None,
        "vars": ["GRIB2(00080, 000, 015, 017, 004, 000)"],
        "filename": "data-20260924T020000Z-example.grib",
    }]
    fetcher = FixtureFetcher(json_by_url={"italiameteo-catalog": payload})

    observations = await adapter_type(endpoint="italiameteo-catalog").fetch(
        _request(
            "italiameteo_dpc_catalog",
            "historical_daily_catalog",
            _target("rome", 41.9028, 12.4964),
        ),
        fetcher,
        now=PRODUCT_TIME + 60.0,
    )

    assert observations == ()
    assert fetcher.calls == [(
        "json",
        "italiameteo-catalog",
        None,
        None,
        4 * 1024 * 1024,
    )]


@pytest.mark.asyncio
async def test_italiameteo_historical_catalog_rejects_invalid_entries():
    adapter_type = getattr(
        provider_adapters,
        "ItaliaMeteoDpcHistoricalCatalogAdapter",
        None,
    )
    assert adapter_type is not None, "ItaliaMeteo historical catalog adapter is required"
    fetcher = FixtureFetcher(
        json_by_url={"italiameteo-catalog": [{"date": "2026-09-23"}]},
    )

    with pytest.raises(ValueError, match="italiameteo_catalog_entry_invalid"):
        await adapter_type(endpoint="italiameteo-catalog").fetch(
            _request(
                "italiameteo_dpc_catalog",
                "historical_daily_catalog",
                _target("rome", 41.9028, 12.4964),
            ),
            fetcher,
            now=PRODUCT_TIME + 60.0,
        )


@pytest.mark.asyncio
async def test_meteolux_validates_current_forecast_without_fusion_observations():
    adapter_type = getattr(provider_adapters, "MeteoLuxValidationAdapter", None)
    assert adapter_type is not None, "MeteoLux validation adapter is required"
    payload = {
        "city": {
            "id": 456,
            "name": "Luxembourg",
            "lat": 49.611567,
            "long": 6.133836,
        },
        "forecast": {
            "current": {
                "date": "2026-09-24T21:42:00",
                "type": "current",
                "rain": "0",
                "temperature": {"temperature": 14},
                "wind": {"direction": "N", "speed": "10-20", "gusts": None},
            },
        },
    }
    fetcher = FixtureFetcher(json_by_url={"meteolux": payload})

    observations = await adapter_type(endpoint="meteolux").fetch(
        _request(
            "meteolux",
            "location_weather_validation",
            _target("luxembourg", 49.6116, 6.1319),
        ),
        fetcher,
        now=PRODUCT_TIME + 60.0,
    )

    assert observations == ()
    assert fetcher.calls == [(
        "json",
        "meteolux",
        {"langcode": "en", "lat": "49.61160", "long": "6.13190"},
        None,
        4 * 1024 * 1024,
    )]


@pytest.mark.asyncio
async def test_meteolux_rejects_missing_current_forecast():
    adapter_type = getattr(provider_adapters, "MeteoLuxValidationAdapter", None)
    assert adapter_type is not None, "MeteoLux validation adapter is required"
    fetcher = FixtureFetcher(json_by_url={"meteolux": {"city": {}}})

    with pytest.raises(ValueError, match="meteolux_current_invalid"):
        await adapter_type(endpoint="meteolux").fetch(
            _request(
                "meteolux",
                "location_weather_validation",
                _target("luxembourg", 49.6116, 6.1319),
            ),
            fetcher,
            now=PRODUCT_TIME + 60.0,
        )


@pytest.mark.asyncio
async def test_opera_recorded_hdf_fixture_samples_target_reflectivity(tmp_path: Path):
    fixture_module = runpy.run_path(
        str(Path(__file__).parent / "fixtures" / "make_opera_fixture.py")
    )
    fixture_path = tmp_path / "opera.h5"
    fixture_module["build_fixture"](str(fixture_path))
    key = "2026/07/12/OPERA/COMP/OPERA@20260712T1200@0@DBZH.h5"
    listing = f"<ListBucketResult><Contents><Key>{key}</Key></Contents></ListBucketResult>"
    fetcher = FixtureFetcher(
        bytes_by_url={
            "opera-list": listing.encode(),
            f"opera-objects/{key}": fixture_path.read_bytes(),
        }
    )
    adapter = OperaRadarAdapter(
        listing_url="opera-list",
        object_base_url="opera-objects",
    )

    observations = await adapter.fetch(
        _request(
            "opera_radar",
            "odyssey_composite",
            _target("storm", 51.093280445864046, 4.6564265170345065),
        ),
        fetcher,
        now=PRODUCT_TIME + 60.0,
    )

    target_observations = [item for item in observations if item.target_id == "storm"]
    assert len(target_observations) == 1
    assert target_observations[0].kind is ObservationKind.REFLECTIVITY
    assert target_observations[0].value == 35.0
    assert target_observations[0].unit == "dBZ"
    assert target_observations[0].metadata["quality_index"] == 0.9


@pytest.mark.asyncio
async def test_opera_recorded_hdf_fixture_emits_spatial_cells_for_tracking(tmp_path: Path):
    fixture_module = runpy.run_path(
        str(Path(__file__).parent / "fixtures" / "make_opera_fixture.py")
    )
    fixture_path = tmp_path / "opera.h5"
    metadata = fixture_module["build_fixture"](str(fixture_path))
    key = "2026/07/12/OPERA/COMP/OPERA@20260712T1200@0@DBZH.h5"
    listing = f"<ListBucketResult><Contents><Key>{key}</Key></Contents></ListBucketResult>"
    fetcher = FixtureFetcher(bytes_by_url={
        "opera-list": listing.encode(),
        f"opera-objects/{key}": fixture_path.read_bytes(),
    })

    observations = await OperaRadarAdapter(
        listing_url="opera-list",
        object_base_url="opera-objects",
    ).fetch(
        _request(
            "opera_radar",
            "odyssey_composite",
            _target(
                "home",
                metadata["test_lat"],
                metadata["test_lon"],
                radius_km=50.0,
            ),
        ),
        fetcher,
        now=PRODUCT_TIME + 60.0,
    )

    spatial = [item for item in observations if item.target_id is None]
    assert spatial
    assert all(item.geometry["type"] == "Polygon" for item in spatial)
    assert max(item.value for item in spatial) >= 35.0


@pytest.mark.asyncio
async def test_opera_stale_listing_is_classified_before_large_object_download():
    key = "2026/07/12/OPERA/COMP/OPERA@20260712T1200@0@DBZH.h5"
    listing = f"<ListBucketResult><Contents><Key>{key}</Key></Contents></ListBucketResult>"
    fetcher = FixtureFetcher(bytes_by_url={"opera-list": listing.encode()})

    with pytest.raises(ProviderStale, match="product_stale"):
        await OperaRadarAdapter(
            listing_url="opera-list",
            object_base_url="opera-objects",
        ).fetch(
            _request(
                "opera_radar",
                "odyssey_composite",
                _target("home", 51.0498071, 4.4186241),
            ),
            fetcher,
            now=PRODUCT_TIME + 16 * 60.0,
        )

    assert fetcher.calls == [(
        "bytes",
        "opera-list",
        {"list-type": "2", "prefix": "2026/07/12/OPERA/COMP/"},
        None,
        4 * 1024 * 1024,
    )]


@pytest.mark.asyncio
async def test_aemet_public_bundle_yields_target_and_spatial_radar_levels():
    observed = datetime.fromtimestamp(PRODUCT_TIME, tz=timezone.utc)
    fetcher = FixtureFetcher(bytes_by_url={
        "aemet": _aemet_archive(observed),
    })

    observations = await AemetRadarAdapter(endpoint="aemet").fetch(
        _request(
            "aemet_radar",
            "radar_composite",
            _target("madrid", 36.0, 4.0, radius_km=300.0),
        ),
        fetcher,
        now=PRODUCT_TIME + 44 * 60.0,
    )

    target = [item for item in observations if item.target_id == "madrid"]
    spatial = [item for item in observations if item.target_id is None]
    assert len(target) == 1
    assert target[0].kind is ObservationKind.RADAR_LEVEL
    assert target[0].value == 6.0
    assert target[0].unit == "level"
    assert spatial
    assert all(item.geometry["type"] == "Polygon" for item in spatial)
    assert fetcher.calls == [("bytes", "aemet", None, None, 10 * 1024 * 1024)]


@pytest.mark.asyncio
async def test_aemet_dry_bundle_is_valid_but_empty():
    observed = datetime.fromtimestamp(PRODUCT_TIME, tz=timezone.utc)

    observations = await AemetRadarAdapter(endpoint="aemet").fetch(
        _request(
            "aemet_radar",
            "radar_composite",
            _target("madrid", 40.4, -3.7, radius_km=250.0),
        ),
        FixtureFetcher(bytes_by_url={
            "aemet": _aemet_archive(observed, wet=False),
        }),
        now=PRODUCT_TIME + 60.0,
    )

    assert observations == ()


@pytest.mark.asyncio
async def test_aemet_rejects_stale_frame():
    observed = datetime.fromtimestamp(PRODUCT_TIME, tz=timezone.utc)

    with pytest.raises(ValueError, match="aemet_frame_stale"):
        await AemetRadarAdapter(endpoint="aemet").fetch(
            _request(
                "aemet_radar",
                "radar_composite",
                _target("madrid", 40.4, -3.7, radius_km=250.0),
            ),
            FixtureFetcher(bytes_by_url={
                "aemet": _aemet_archive(observed),
            }),
            now=PRODUCT_TIME + 46 * 60.0,
        )


@pytest.mark.asyncio
async def test_meteofrance_hdf5_yields_target_and_spatial_rain_rate():
    adapter_type = getattr(provider_adapters, "MeteoFranceRadarAdapter", None)
    assert adapter_type is not None, "thin Météo-France adapter is required"
    fetcher = FixtureFetcher(bytes_by_url={
        "meteofrance": _meteofrance_hdf5(
            datetime.fromtimestamp(PRODUCT_TIME, tz=timezone.utc)
        ),
    })

    result = await adapter_type(
        access_token="test-token",
        endpoint="meteofrance",
    ).fetch(
        _request(
            "meteofrance_radar",
            "radar_accumulation",
            _target("paris", 48.8566, 2.3522, radius_km=100.0),
        ),
        fetcher,
        now=PRODUCT_TIME + 60.0,
    )

    target = [item for item in result if item.target_id == "paris"]
    spatial = [item for item in result if item.target_id is None]
    assert len(target) == 1
    assert target[0].kind is ObservationKind.RAIN_RATE
    assert target[0].value == pytest.approx(12.0)
    assert spatial
    assert all(item.geometry["type"] == "Polygon" for item in spatial)
    assert len(result.radar_fields) == 1
    assert fetcher.calls == [(
        "bytes",
        "meteofrance",
        None,
        {"Authorization": "Bearer test-token"},
        40 * 1024 * 1024,
    )]


@pytest.mark.asyncio
async def test_dpc_sri_geotiff_yields_target_spatial_and_tracking_field():
    adapter_type = getattr(provider_adapters, "DpcRadarAdapter", None)
    assert adapter_type is not None, "thin DPC SRI adapter is required"
    timestamp_ms = int(PRODUCT_TIME * 1_000)
    origin = "https://radar.protezionecivile.it"
    fetcher = FixtureFetcher(
        json_by_url={
            "dpc-catalog": {
                "total": 1,
                "lastProducts": [{
                    "productType": "SRI",
                    "time": timestamp_ms,
                    "period": "PT5M",
                }],
            },
        },
        post_json_by_url={
            "dpc-download": {"url": "https://dpc-frame.example/sri.tif"},
        },
        bytes_by_url={
            "https://dpc-frame.example/sri.tif": _dpc_sri_geotiff(),
        },
    )

    result = await adapter_type(
        catalog_endpoint="dpc-catalog",
        download_endpoint="dpc-download",
    ).fetch(
        _request(
            "dpc_radar",
            "surface_rainfall_intensity",
            _target("rome", 41.9028, 12.4964, radius_km=100.0),
        ),
        fetcher,
        now=PRODUCT_TIME + 60.0,
    )

    target = [item for item in result if item.target_id == "rome"]
    spatial = [item for item in result if item.target_id is None]
    assert len(target) == 1
    assert target[0].kind is ObservationKind.RAIN_RATE
    assert target[0].value == pytest.approx(12.0)
    assert target[0].unit == "mm/h"
    assert spatial
    assert all(item.geometry["type"] == "Polygon" for item in spatial)
    assert len(result.radar_fields) == 1
    assert result.radar_fields[0].observed_at == PRODUCT_TIME
    assert fetcher.calls == [
        (
            "json",
            "dpc-catalog",
            {"type": "SRI"},
            {"Origin": origin},
            2 * 1024 * 1024,
        ),
        (
            "post_json",
            "dpc-download",
            {"productType": "SRI", "productDate": timestamp_ms},
            {"Origin": origin},
            2 * 1024 * 1024,
        ),
        (
            "bytes",
            "https://dpc-frame.example/sri.tif",
            None,
            {},
            25 * 1024 * 1024,
        ),
    ]


@pytest.mark.asyncio
async def test_dpc_rejects_stale_sri_frame():
    adapter_type = getattr(provider_adapters, "DpcRadarAdapter", None)
    assert adapter_type is not None, "thin DPC SRI adapter is required"
    timestamp_ms = int(PRODUCT_TIME * 1_000)
    fetcher = FixtureFetcher(
        json_by_url={
            "catalog": {
                "lastProducts": [{"productType": "SRI", "time": timestamp_ms}],
            },
        },
        post_json_by_url={"download": {"url": "frame"}},
        bytes_by_url={"frame": _dpc_sri_geotiff()},
    )

    with pytest.raises(ValueError, match="dpc_frame_stale"):
        await adapter_type(
            catalog_endpoint="catalog",
            download_endpoint="download",
        ).fetch(
            _request(
                "dpc_radar",
                "surface_rainfall_intensity",
                _target("rome", 41.9028, 12.4964),
            ),
            fetcher,
            now=PRODUCT_TIME + 21 * 60.0,
        )


@pytest.mark.asyncio
async def test_metoffice_public_s3_rate_yields_target_shape_and_tracking_field():
    observed = datetime.fromtimestamp(PRODUCT_TIME, tz=timezone.utc)
    key = (
        "radar/2026/07/12/"
        "202607121200_ODIM_ng_radar_rainrate_composite_1km_UK.h5"
    )
    listing = (
        "<?xml version='1.0' encoding='UTF-8'?>"
        "<ListBucketResult xmlns='http://s3.amazonaws.com/doc/2006-03-01/'>"
        f"<Contents><Key>{key}</Key></Contents>"
        "</ListBucketResult>"
    ).encode()
    bucket = "https://met-office.example/"
    fetcher = FixtureFetcher(bytes_by_url={
        bucket: listing,
        bucket + key: _metoffice_hdf5(observed),
    })

    result = await MetOfficeRadarAdapter(bucket_endpoint=bucket).fetch(
        _request(
            "metoffice_radar",
            "uk_rate",
            _target("london", 51.5074, -0.1278, radius_km=100.0),
        ),
        fetcher,
        now=PRODUCT_TIME + 60.0,
    )

    target = [item for item in result if item.target_id == "london"]
    spatial = [item for item in result if item.target_id is None]
    assert len(target) == 1
    assert target[0].kind is ObservationKind.RAIN_RATE
    assert target[0].value == pytest.approx(4.5)
    assert spatial
    assert all(item.geometry["type"] == "Polygon" for item in spatial)
    assert len(result.radar_fields) == 1
    assert fetcher.calls[0][0] == "bytes"
    assert fetcher.calls[-1][1] == bucket + key


@pytest.mark.asyncio
async def test_rainviewer_public_tiles_yield_categorical_target_and_shape():
    from PIL import Image

    manifest_url = "https://rainviewer.example/manifest"
    host = "https://tilecache.rainviewer.com"
    path = "/v2/radar/0899dee158d6"
    image = Image.new("RGBA", (256, 256), (0, 0, 0, 0))
    for column in range(0, 5):
        for row in range(0, 5):
            image.putpixel((column, row), (0, 200, 100, 255))
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    tiles = {}
    for x in range(15, 18):
        for y in range(15, 18):
            url = f"{host}{path}/256/5/{x}/{y}/2/0_0.png"
            tiles[url] = buffer.getvalue() if (x, y) == (16, 16) else _empty_png()
    fetcher = FixtureFetcher(
        json_by_url={manifest_url: {
            "version": "2.0",
            "generated": int(PRODUCT_TIME + 60),
            "host": host,
            "radar": {"past": [{"time": int(PRODUCT_TIME), "path": path}]},
        }},
        bytes_by_url=tiles,
    )

    result = await RainViewerRadarAdapter(manifest_url=manifest_url).fetch(
        _request(
            "rainviewer",
            "visual",
            _target("equator", 0.0, 0.0, radius_km=100.0),
        ),
        fetcher,
        now=PRODUCT_TIME + 60.0,
    )

    target = [item for item in result if item.target_id == "equator"]
    spatial = [item for item in result if item.target_id is None]
    assert len(target) == 1
    assert target[0].kind is ObservationKind.RADAR_LEVEL
    assert target[0].metadata["quantitative_semantics_eligible"] is False
    assert spatial
    assert all(item.geometry["type"] == "Polygon" for item in spatial)
    assert len(fetcher.calls) == 10


@pytest.mark.asyncio
async def test_meteofrance_client_credentials_token_is_cached_in_memory():
    provider_type = getattr(
        provider_adapters,
        "MeteoFranceOAuthTokenProvider",
        None,
    )
    assert provider_type is not None, "Météo-France OAuth provider is required"
    fetcher = FixtureFetcher(post_json_by_url={
        "token": {"access_token": "fresh-token", "expires_in": 3600},
    })
    provider = provider_type(
        fetcher=fetcher,
        application_id="encoded-client-credential",
        endpoint="token",
        monotonic=lambda: 100.0,
    )

    assert await provider.get_access_token() == "fresh-token"
    assert await provider.get_access_token() == "fresh-token"
    assert fetcher.calls == [(
        "post_json",
        "token",
        {"grant_type": "client_credentials"},
        {
            "Authorization": "Basic encoded-client-credential",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        512 * 1024,
    )]


@pytest.mark.asyncio
async def test_meteofrance_client_credentials_override_stale_static_token():
    adapter_type = getattr(provider_adapters, "MeteoFranceRadarAdapter")
    fetcher = FixtureFetcher(bytes_by_url={
        "meteofrance": _meteofrance_hdf5(
            datetime.fromtimestamp(PRODUCT_TIME, tz=timezone.utc),
            wet=False,
        ),
    })

    async def fresh_token():
        return "fresh-token"

    await adapter_type(
        access_token="stale-token",
        access_token_provider=fresh_token,
        endpoint="meteofrance",
    ).fetch(
        _request(
            "meteofrance_radar",
            "radar_accumulation",
            _target("paris", 48.8566, 2.3522, radius_km=100.0),
        ),
        fetcher,
        now=PRODUCT_TIME + 60.0,
    )

    assert fetcher.calls[0][3] == {"Authorization": "Bearer fresh-token"}


@pytest.mark.asyncio
async def test_kmi_appv4_emits_categorical_target_and_spatial_shape():
    from PIL import Image

    image = Image.new("RGBA", (16, 16), (0, 0, 0, 0))
    for column, row in ((8, 5), (9, 5), (8, 6), (9, 6)):
        image.putpixel((column, row), (255, 0, 0, 255))
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    payload = {
        "animation": {
            "sequence": [
                {
                    "time": "2026-07-12T12:00:00+00:00",
                    "uri": "https://cdn.meteo.be/kmi-frame",
                }
            ]
        }
    }
    fetcher = FixtureFetcher(
        json_by_url={"kmi-forecast": payload},
        bytes_by_url={"https://cdn.meteo.be/kmi-frame": buffer.getvalue()},
    )
    adapter = KmiRadarAdapter(forecast_url="kmi-forecast")

    observations = await adapter.fetch(
        _request(
            "kmi_appv4",
            "appv4_visual",
            _target("home", 50.85, 4.35),
        ),
        fetcher,
        now=PRODUCT_TIME + 60.0,
    )

    target = next(item for item in observations if item.target_id == "home")
    spatial = next(item for item in observations if item.target_id is None)
    assert target.kind is ObservationKind.RADAR_LEVEL
    assert target.value == 7.0
    assert target.unit == "level"
    assert target.metadata["quantitative_semantics_eligible"] is False
    assert spatial.kind is ObservationKind.RADAR_LEVEL
    assert spatial.geometry["type"] == "Polygon"
    assert spatial.metadata["spatial_cell"] is True
    assert spatial.metadata["operational_use"] == "visualisation_only"


@pytest.mark.asyncio
async def test_kmi_appv4_builds_daily_mobile_app_request_without_external_api_key():
    from PIL import Image

    image = Image.new("RGBA", (16, 16), (0, 0, 0, 0))
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    endpoint = "https://app.meteo.be/services/appv4/"
    fetcher = FixtureFetcher(
        json_by_url={
            endpoint: {
                "animation": {
                    "sequence": [{
                        "time": "2026-07-12T12:00:00+00:00",
                        "uri": "https://cdn.meteo.be/frame.png",
                    }]
                }
            }
        },
        bytes_by_url={"https://cdn.meteo.be/frame.png": buffer.getvalue()},
    )

    observations = await KmiRadarAdapter().fetch(
        _request(
            "kmi_appv4",
            "appv4_visual",
            _target("home", 50.85, 4.35),
        ),
        fetcher,
        now=PRODUCT_TIME + 60.0,
    )

    assert observations == ()
    request = fetcher.calls[0]
    assert request[1] == endpoint
    assert request[2]["s"] == "getForecasts"
    assert request[2]["lat"] == "50.85000"
    assert request[2]["long"] == "4.35000"
    assert len(request[2]["k"]) == 32
    assert request[3] == {"User-Agent": "be.meteo.app"}


@pytest.mark.asyncio
async def test_metar_json_fixture_yields_ground_measurements():
    payload = [{
        "icaoId": "EBBR",
        "lat": 50.901,
        "lon": 4.484,
        "obsTime": "2026-07-12T12:00:00Z",
        "temp": 18.2,
        "altim": 1008.2,
    }]
    fetcher = FixtureFetcher(json_by_url={"metar": payload})

    observations = await MetarAdapter(endpoint="metar").fetch(
        _request("metar", "ground_observation", _target("home", 50.85, 4.35)),
        fetcher,
        now=PRODUCT_TIME + 1800.0,
    )

    assert {(item.kind, item.value) for item in observations} == {
        (ObservationKind.TEMPERATURE, 18.2),
        (ObservationKind.PRESSURE, 1008.2),
    }
    assert all(item.metadata["station_id"] == "EBBR" for item in observations)
    assert fetcher.calls[0][2]["bbox"] == "48.850,1.350,52.850,7.350"


@pytest.mark.asyncio
async def test_metar_catalog_selection_queries_only_station_ids():
    request = replace(
        _request("metar", "ground_observation", _target("home", 50.85, 4.35)),
        selected_station_ids=("EBBR", "EBAW"),
        station_selection_applied=True,
    )
    fetcher = FixtureFetcher(json_by_url={"metar": []})

    await MetarAdapter(endpoint="metar").fetch(request, fetcher, now=PRODUCT_TIME)

    assert fetcher.calls[0][2]["ids"] == "EBAW,EBBR"
    assert "bbox" not in fetcher.calls[0][2]


@pytest.mark.asyncio
async def test_http_fetcher_exposes_numeric_status_for_provider_specific_handling():
    error_type = getattr(provider_adapters, "HttpStatusError", None)
    assert error_type is not None, "typed HTTP status errors are required"
    fetcher = provider_adapters.AioHttpFetcher(session=StatusSession(204))

    with pytest.raises(error_type) as raised:
        await fetcher.get_bytes("https://example.invalid/metar")

    assert raised.value.status_code == 204
    assert str(raised.value) == "http_status_204"


@pytest.mark.asyncio
async def test_metar_treats_http_204_as_a_valid_empty_measurement():
    observations = await MetarAdapter(endpoint="metar").fetch(
        _request("metar", "ground_observation", _target("home", 50.85, 4.35)),
        StatusJsonFetcher(204),
        now=PRODUCT_TIME + 60.0,
    )

    assert observations == ()


@pytest.mark.asyncio
async def test_metar_keeps_other_http_statuses_as_errors():
    error_type = getattr(provider_adapters, "HttpStatusError", None)
    assert error_type is not None, "typed HTTP status errors are required"

    with pytest.raises(error_type) as raised:
        await MetarAdapter(endpoint="metar").fetch(
            _request("metar", "ground_observation", _target("home", 50.85, 4.35)),
            StatusJsonFetcher(503),
            now=PRODUCT_TIME + 60.0,
        )

    assert raised.value.status_code == 503


@pytest.mark.asyncio
async def test_blitzortung_direct_event_is_drained_for_nearby_target():
    adapter = BlitzortungAdapter()
    adapter.set_transport_state(True)
    adapter.feed_message(
        json.dumps({
            "lat": 50.86,
            "lon": 4.36,
            "time": int(PRODUCT_TIME * 1_000_000_000),
        }).encode(),
        received_at=PRODUCT_TIME + 1.0,
    )

    observations = await adapter.fetch(
        _request("blitzortung", "lightning_events", _target("home", 50.85, 4.35)),
        FixtureFetcher(),
        now=PRODUCT_TIME + 60.0,
    )

    assert len(observations) == 1
    assert observations[0].kind is ObservationKind.LIGHTNING_STRIKE
    assert observations[0].value == 1.0
    assert observations[0].target_id == "home"


@pytest.mark.asyncio
async def test_netatmo_recorded_fixture_yields_direct_rain_rate():
    payload = json.loads(
        (Path(__file__).parent / "fixtures" / "netatmo_getpublicdata_ground_golden.json")
        .read_text(encoding="utf-8")
    )
    fetcher = FixtureFetcher(json_by_url={"netatmo": payload})
    target = _target("home", 51.047, 4.418)
    request = ThinProviderRequest(
        "netatmo",
        "ground_observation",
        (target,),
        1722000060.0,
    )

    observations = await NetatmoAdapter(
        endpoint="netatmo",
        access_token="fixture-token",
    ).fetch(request, fetcher, now=1722000060.0)

    rain = [item for item in observations if item.kind is ObservationKind.RAIN_RATE]
    temperature = [item for item in observations if item.kind is ObservationKind.TEMPERATURE]
    humidity = [item for item in observations if item.kind is ObservationKind.RELATIVE_HUMIDITY]
    assert len(rain) == 1
    assert rain[0].value == 0.015
    assert rain[0].metadata["station_id"] == "70:ee:50:29:59:56"
    assert [item.value for item in temperature if item.metadata["station_id"] == "70:ee:50:29:59:56"] == [18.2]
    assert [item.value for item in humidity if item.metadata["station_id"] == "70:ee:50:29:59:56"] == [91.0]
    assert fetcher.calls[0][2]["filter"] == "true"


@pytest.mark.asyncio
async def test_netatmo_refresh_token_is_used_without_exposing_it_in_observation_call():
    fetcher = FixtureFetcher(
        post_json_by_url={"netatmo-token": {"access_token": "short-lived-token", "expires_in": 3600}},
        json_by_url={"netatmo": {"body": []}},
    )
    token_provider = NetatmoOAuthTokenProvider(
        fetcher=fetcher,
        client_id="client-id",
        client_secret="client-secret",
        refresh_token="refresh-token",
        endpoint="netatmo-token",
    )
    await NetatmoAdapter(
        endpoint="netatmo",
        access_token_provider=token_provider.get_access_token,
    ).fetch(
        _request("netatmo", "ground_observation", _target("home", 51.047, 4.418)),
        fetcher,
        now=PRODUCT_TIME + 60.0,
    )

    assert fetcher.calls[0][0] == "post_json"
    assert fetcher.calls[0][1] == "netatmo-token"
    assert fetcher.calls[1][3] == {"Authorization": "Bearer short-lived-token"}


@pytest.mark.asyncio
async def test_netatmo_rotated_refresh_token_is_reused_and_persisted_privately(tmp_path):
    class RotatingFetcher:
        def __init__(self):
            self.refresh_tokens = []

        async def post_json(self, _url, *, data, headers=None, maximum_bytes=None):
            del headers, maximum_bytes
            self.refresh_tokens.append(data["refresh_token"])
            if len(self.refresh_tokens) == 1:
                return {
                    "access_token": "access-one",
                    "refresh_token": "rotated-refresh",
                    "expires_in": 60,
                }
            return {"access_token": "access-two", "expires_in": 3600}

    ticks = iter((0.0, 61.0))
    state_path = tmp_path / "netatmo-token.json"
    fetcher = RotatingFetcher()
    provider = NetatmoOAuthTokenProvider(
        fetcher=fetcher,
        client_id="client-id",
        client_secret="client-secret",
        refresh_token="initial-refresh",
        endpoint="netatmo-token",
        monotonic=lambda: next(ticks),
        refresh_token_state_path=state_path,
    )

    assert await provider.get_access_token() == "access-one"
    assert await provider.get_access_token() == "access-two"

    assert fetcher.refresh_tokens == ["initial-refresh", "rotated-refresh"]
    assert json.loads(state_path.read_text(encoding="utf-8")) == {
        "refresh_token": "rotated-refresh",
    }
    assert state_path.stat().st_mode & 0o077 == 0


@pytest.mark.asyncio
async def test_netatmo_dry_station_uses_response_time_when_rain_timestamp_is_old():
    payload = json.loads(
        (Path(__file__).parent / "fixtures" / "netatmo_getpublicdata_ground_golden.json")
        .read_text(encoding="utf-8")
    )
    payload["body"][0]["measures"]["02:00:00:aa:bb:cc"]["rain_live"] = 0.0
    payload["body"][0]["measures"]["02:00:00:aa:bb:cc"]["rain_timeutc"] = PRODUCT_TIME - 3 * 3600
    observations = await NetatmoAdapter(
        endpoint="netatmo",
        access_token="fixture-token",
    ).fetch(
        _request("netatmo", "ground_observation", _target("home", 51.047, 4.418)),
        FixtureFetcher(json_by_url={"netatmo": payload}),
        now=PRODUCT_TIME,
    )

    dry_rain = [
        item for item in observations
        if item.kind is ObservationKind.RAIN_RATE
        and item.metadata["station_id"] == "70:ee:50:29:59:56"
    ]
    assert len(dry_rain) == 1
    assert dry_rain[0].observed_at == PRODUCT_TIME


@pytest.mark.asyncio
async def test_netatmo_skips_an_invalid_single_measurement_without_losing_station_data():
    payload = json.loads(
        (Path(__file__).parent / "fixtures" / "netatmo_getpublicdata_ground_golden.json")
        .read_text(encoding="utf-8")
    )
    payload["body"][0]["measures"]["02:00:00:aa:bb:cc"]["wind_angle"] = 999.0

    observations = await NetatmoAdapter(
        endpoint="netatmo",
        access_token="fixture-token",
    ).fetch(
        _request("netatmo", "ground_observation", _target("home", 51.047, 4.418)),
        FixtureFetcher(json_by_url={"netatmo": payload}),
        now=PRODUCT_TIME,
    )

    assert any(item.kind is ObservationKind.TEMPERATURE for item in observations)
    assert any(item.kind is ObservationKind.RAIN_RATE for item in observations)
    assert not any(item.kind is ObservationKind.WIND_DIRECTION for item in observations)


@pytest.mark.asyncio
async def test_netatmo_queries_each_target_in_its_own_small_area():
    payload = {"body": []}
    fetcher = FixtureFetcher(json_by_url={"netatmo": payload})
    await NetatmoAdapter(endpoint="netatmo", access_token="fixture-token").fetch(
        _request(
            "netatmo",
            "ground_observation",
            _target("home", 51.047, 4.418),
            _target("test", 51.221, 6.776),
        ),
        fetcher,
        now=PRODUCT_TIME,
    )

    assert len(fetcher.calls) == 2
    home_params = fetcher.calls[0][2]
    test_params = fetcher.calls[1][2]
    assert home_params["lat_ne"] == "51.22718"
    assert test_params["lon_sw"] == "6.59582"
    assert home_params["filter"] == test_params["filter"] == "true"


@pytest.mark.asyncio
async def test_netatmo_queries_one_shared_region_and_routes_each_station_once_per_target():
    payload = json.loads(
        (Path(__file__).parent / "fixtures" / "netatmo_getpublicdata_ground_golden.json")
        .read_text(encoding="utf-8")
    )
    home = _target("home", 51.047, 4.418)
    nearby = _target("nearby", 51.080, 4.420)
    region = ThinAcquisitionRegion(
        region_id="region:home",
        kind="home",
        center_latitude=51.047,
        center_longitude=4.418,
        observation_radius_km=70.0,
        targets=(home, nearby),
        stale_target_ids=(),
    )
    request = ThinProviderRequest(
        provider_id="netatmo",
        product_id="ground_observation",
        targets=(home, nearby),
        requested_at=PRODUCT_TIME,
        regions=(region,),
        ground_radius_km=20.0,
    )
    fetcher = FixtureFetcher(json_by_url={"netatmo": payload})

    observations = await NetatmoAdapter(
        endpoint="netatmo",
        access_token="fixture-token",
    ).fetch(request, fetcher, now=PRODUCT_TIME)

    assert len(fetcher.calls) == 1
    assert {item.target_id for item in observations} == {"home", "nearby"}
    identities = [
        (
            item.target_id,
            item.metadata["station_id"],
            item.kind,
            item.metadata["measurement"],
        )
        for item in observations
    ]
    assert len(identities) == len(set(identities))


@pytest.mark.asyncio
async def test_netatmo_catalog_selection_filters_unselected_stations():
    payload = json.loads(
        (Path(__file__).parent / "fixtures" / "netatmo_getpublicdata_ground_golden.json")
        .read_text(encoding="utf-8")
    )
    request = replace(
        _request("netatmo", "ground_observation", _target("home", 51.047, 4.418)),
        selected_station_ids=("70:ee:50:29:59:56",),
        station_selection_applied=True,
    )

    observations = await NetatmoAdapter(
        endpoint="netatmo",
        access_token="fixture-token",
    ).fetch(request, FixtureFetcher(json_by_url={"netatmo": payload}), now=PRODUCT_TIME)

    assert {item.metadata["station_id"] for item in observations} == {
        "70:ee:50:29:59:56"
    }


@pytest.mark.asyncio
async def test_wunderground_current_observation_normalizes_metric_fields_once():
    payload = {"observations": [{
        "stationID": "IMECHE17",
        "epoch": int(PRODUCT_TIME),
        "lat": 51.05,
        "lon": 4.42,
        "humidity": 88.0,
        "winddir": 245.0,
        "metric": {
            "temp": 12.5,
            "windSpeed": 18.0,
            "windGust": 27.0,
            "pressure": 1007.4,
            "precipRate": 1.2,
            "precipTotal": 4.8,
        },
    }]}
    fetcher = FixtureFetcher(json_by_url={"wu": payload})
    request = replace(
        _request("wunderground", "ground_observation", _target("home", 51.05, 4.42)),
        selected_station_ids=("IMECHE17", "IMECHE17"),
        station_selection_applied=True,
    )

    observations = await WundergroundAdapter(
        endpoint="wu",
        api_key="not-a-real-key",
    ).fetch(request, fetcher, now=PRODUCT_TIME + 10.0)

    values = {item.kind: item.value for item in observations}
    by_measurement = {item.metadata["measurement"]: item.value for item in observations}
    assert values[ObservationKind.RAIN_RATE] == 1.2
    assert values[ObservationKind.PRESSURE] == 1007.4
    assert by_measurement["wind_speed"] == 5.0
    assert by_measurement["wind_gust"] == 7.5
    assert values[ObservationKind.TEMPERATURE] == 12.5
    assert len(fetcher.calls) == 1
    assert fetcher.calls[0][2]["stationId"] == "IMECHE17"
    assert fetcher.calls[0][2]["apiKey"] == "not-a-real-key"
    assert all("api" not in "".join(item.metadata).lower() for item in observations)


@pytest.mark.asyncio
async def test_wunderground_without_key_is_ready_but_blocked():
    with pytest.raises(ProviderBlocked) as raised:
        await WundergroundAdapter(api_key="").fetch(
            replace(
                _request("wunderground", "ground_observation", _target("home", 51.05, 4.42)),
                selected_station_ids=("IMECHE17",),
                station_selection_applied=True,
            ),
            FixtureFetcher(),
            now=PRODUCT_TIME,
        )

    assert raised.value.ready is True
    assert raised.value.reason_code == "blocked_api_key"


@pytest.mark.asyncio
async def test_netatmo_can_report_a_user_selected_station_without_filtering_data(caplog):
    caplog.set_level(logging.INFO, logger="storm_tracker_worker.thin_provider_adapters")
    payload = json.loads(
        (Path(__file__).parent / "fixtures" / "netatmo_getpublicdata_ground_golden.json")
        .read_text(encoding="utf-8")
    )
    observations = await NetatmoAdapter(
        endpoint="netatmo",
        access_token="fixture-token",
        station_id_for_summary="70:ee:50:00:00:02",
    ).fetch(
        _request("netatmo", "ground_observation", _target("home", 51.047, 4.418)),
        FixtureFetcher(json_by_url={"netatmo": payload}),
        now=PRODUCT_TIME,
    )

    assert len(observations) == 1
    assert observations[0].kind is ObservationKind.TEMPERATURE
    assert observations[0].value == 18.0
    assert "geselecteerd station" in caplog.text
    assert "temperature=18" in caplog.text


@pytest.mark.asyncio
async def test_dwd_radolan_latest_frame_yields_target_and_spatial_rain():
    observed = datetime.fromtimestamp(PRODUCT_TIME, tz=timezone.utc)
    fetcher = FixtureFetcher(bytes_by_url={"dwd": _dwd_archive(observed)})

    observations = await DwdRadolanAdapter(endpoint="dwd").fetch(
        _request("dwd_radolan", "radolan_rw", _target("home", 50.85, 4.35)),
        fetcher,
        now=PRODUCT_TIME + 60.0,
    )

    target = [item for item in observations if item.target_id == "home"]
    spatial = [item for item in observations if item.target_id is None]
    assert len(target) == 1
    assert target[0].kind is ObservationKind.RAIN_RATE
    assert target[0].value == pytest.approx(11.988)
    assert target[0].unit == "mm/h"
    assert spatial
    assert all(item.geometry["type"] == "Polygon" for item in spatial)
    assert all(item.metadata["spatial_cell"] is True for item in spatial)
    assert len(observations.radar_fields) == 1
    field = observations.radar_fields[0]
    assert field.provider_id == "dwd_radolan"
    assert field.observed_at == PRODUCT_TIME
    assert field.rain_rate_mm_h.shape == (16, 16)
    assert field.valid_mask.shape == (16, 16)
    assert field.valid_mask[8, 8]
    assert field.rain_rate_mm_h[8, 8] == 0.0


@pytest.mark.asyncio
async def test_dwd_radolan_spatial_geometry_follows_non_rectangular_echo():
    observed = datetime.fromtimestamp(PRODUCT_TIME, tz=timezone.utc)
    fetcher = FixtureFetcher(bytes_by_url={
        "dwd": _dwd_archive(observed, l_shaped=True),
    })

    observations = await DwdRadolanAdapter(endpoint="dwd").fetch(
        _request("dwd_radolan", "radolan_rw", _target("home", 50.85, 4.35)),
        fetcher,
        now=PRODUCT_TIME + 60.0,
    )

    spatial = [item for item in observations if item.target_id is None]
    assert len(spatial) == 1
    assert spatial[0].geometry["type"] == "Polygon"
    assert len(spatial[0].geometry["coordinates"][0]) > 5


@pytest.mark.asyncio
async def test_dwd_radolan_dry_frame_is_valid_but_empty():
    observed = datetime.fromtimestamp(PRODUCT_TIME, tz=timezone.utc)
    fetcher = FixtureFetcher(bytes_by_url={"dwd": _dwd_archive(observed, wet=False)})

    observations = await DwdRadolanAdapter(endpoint="dwd").fetch(
        _request("dwd_radolan", "radolan_rw", _target("home", 50.85, 4.35)),
        fetcher,
        now=PRODUCT_TIME + 60.0,
    )

    assert observations == ()


@pytest.mark.asyncio
async def test_dwd_radolan_rejects_stale_frame():
    observed = datetime.fromtimestamp(PRODUCT_TIME, tz=timezone.utc)
    fetcher = FixtureFetcher(bytes_by_url={"dwd": _dwd_archive(observed)})

    with pytest.raises(ValueError, match="dwd_frame_stale"):
        await DwdRadolanAdapter(endpoint="dwd").fetch(
            _request("dwd_radolan", "radolan_rw", _target("home", 50.85, 4.35)),
            fetcher,
            now=PRODUCT_TIME + 16 * 60.0,
        )


@pytest.mark.asyncio
async def test_knmi_rtcor_latest_frame_yields_rain_without_leaking_auth():
    observed = datetime.fromtimestamp(PRODUCT_TIME, tz=timezone.utc)
    filename = "RAD_NL25_RAC_RT_202607121200.h5"
    descriptor_url = f"knmi-api/files/{filename}/url"
    download_url = "https://objects.knmi.nl/fixture.h5"
    fetcher = FixtureFetcher(
        json_by_url={
            "knmi-api/files": {"files": [{"filename": filename}]},
            descriptor_url: {"temporaryDownloadUrl": download_url},
        },
        bytes_by_url={download_url: _knmi_rtcor_hdf(observed)},
    )

    observations = await KnmiRadarAdapter(
        api_key="fixture-secret",
        open_data_base="knmi-api",
    ).fetch(
        _request("knmi_radar", "radar_composite", _target("home", 50.85, 4.35)),
        fetcher,
        now=PRODUCT_TIME + 60.0,
    )

    target = [item for item in observations if item.target_id == "home"]
    spatial = [item for item in observations if item.target_id is None]
    assert len(target) == 1
    assert target[0].kind is ObservationKind.RAIN_RATE
    assert target[0].value == pytest.approx(12.0)
    assert spatial
    assert len(observations.radar_fields) == 1
    field = observations.radar_fields[0]
    assert field.provider_id == "knmi_radar"
    assert field.observed_at == PRODUCT_TIME
    assert field.rain_rate_mm_h.shape == (765, 700)
    assert field.valid_mask.shape == (765, 700)
    assert field.valid_mask[0, 0]
    assert field.rain_rate_mm_h[0, 0] == pytest.approx(12.0)
    crs = CRS.from_user_input(field.crs)
    assert crs.is_projected
    assert crs.axis_info[0].unit_conversion_factor == pytest.approx(1.0)
    assert field.x_resolution_m == pytest.approx(1_000.0, abs=0.01)
    assert field.y_resolution_m == pytest.approx(1_000.0, abs=0.01)
    assert fetcher.calls[0][3] == {"Authorization": "fixture-secret"}
    assert fetcher.calls[1][3] == {"Authorization": "fixture-secret"}
    assert fetcher.calls[2][3] == {}


@pytest.mark.asyncio
async def test_knmi_rtcor_dry_frame_is_valid_but_empty():
    observed = datetime.fromtimestamp(PRODUCT_TIME, tz=timezone.utc)
    filename = "RAD_NL25_RAC_RT_202607121200.h5"
    download_url = "https://objects.knmi.nl/dry.h5"
    fetcher = FixtureFetcher(
        json_by_url={
            "knmi-api/files": {"files": [{"filename": filename}]},
            f"knmi-api/files/{filename}/url": {"temporaryDownloadUrl": download_url},
        },
        bytes_by_url={download_url: _knmi_rtcor_hdf(observed, wet=False)},
    )

    observations = await KnmiRadarAdapter(
        api_key="fixture-secret",
        open_data_base="knmi-api",
    ).fetch(
        _request("knmi_radar", "radar_composite", _target("home", 50.85, 4.35)),
        fetcher,
        now=PRODUCT_TIME + 60.0,
    )

    assert observations == ()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "adapter",
    [KnmiRadarAdapter(), NetatmoAdapter()],
)
async def test_adapter_without_required_access_or_decoder_is_explicitly_blocked(adapter):
    with pytest.raises(ProviderBlocked):
        await adapter.fetch(
            _request("blocked", "weather", _target("home", 50.85, 4.35)),
            FixtureFetcher(),
            now=PRODUCT_TIME + 60.0,
        )
