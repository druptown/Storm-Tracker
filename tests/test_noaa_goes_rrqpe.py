from __future__ import annotations

import io
from pathlib import Path

import h5py
import numpy as np


def _product_bytes():
    output = io.BytesIO()
    with h5py.File(output, "w") as product:
        product.attrs["time_coverage_start"] = "2026-07-22T01:40:20.3Z"
        x = product.create_dataset("x", data=np.arange(-10, 11, dtype=np.int16))
        y = product.create_dataset("y", data=np.arange(10, -11, -1, dtype=np.int16))
        for axis in (x, y):
            axis.attrs["scale_factor"] = 0.000056
            axis.attrs["add_offset"] = 0.0
        projection = product.create_dataset("goes_imager_projection", data=np.int32(0))
        projection.attrs["perspective_point_height"] = 35786023.0
        projection.attrs["semi_major_axis"] = 6378137.0
        projection.attrs["semi_minor_axis"] = 6356752.31414
        projection.attrs["latitude_of_projection_origin"] = 0.0
        projection.attrs["longitude_of_projection_origin"] = -75.0
        projection.attrs["sweep_angle_axis"] = "x"
        raw = np.zeros((21, 21), dtype=np.uint16)
        raw[9:12, 9:12] = 1000
        rr = product.create_dataset("RRQPE", data=raw)
        rr.attrs["scale_factor"] = 0.01
        rr.attrs["add_offset"] = 0.0
        rr.attrs["_FillValue"] = 65535
        product.create_dataset("DQF", data=np.zeros((21, 21), dtype=np.uint8))
    return output.getvalue()


def test_goes_satellite_selection(noaa_goes_rrqpe_module):
    module = noaa_goes_rrqpe_module
    assert module.satellite_for_longitude(-80) == 19
    assert module.satellite_for_longitude(-125) == 18
    assert module.satellite_for_longitude(4) is None
    assert module.satellite_for_longitude(120) is None


def test_goes_rrqpe_decodes_real_projection(noaa_goes_rrqpe_module, base_module):
    timestamp = 1784684420.3
    observations, overlay = noaa_goes_rrqpe_module.parse_rrqpe_netcdf(
        _product_bytes(), 19,
        (base_module.CoverageArea(0.0, -75.0, 100.0),),
        now=timestamp + 60,
    )
    assert observations
    assert observations[0].source == "noaa_goes_rrqpe"
    assert observations[0].quality == 0.9
    assert observations[0].footprint_points
    assert overlay["source"] == "noaa_goes_rrqpe"
    assert overlay["runs"]


def test_goes_rrqpe_real_sample_when_available(noaa_goes_rrqpe_module, base_module):
    samples = list(Path("testdata/goes").glob("*.nc"))
    if not samples:
        return
    payload = samples[-1].read_bytes()
    with h5py.File(io.BytesIO(payload), "r") as product:
        timestamp = noaa_goes_rrqpe_module._timestamp(product)
    observations, overlay = noaa_goes_rrqpe_module.parse_rrqpe_netcdf(
        payload, 19, (base_module.CoverageArea(25.7617, -80.1918, 300.0),),
        now=timestamp + 60,
    )
    assert overlay["source"] == "noaa_goes_rrqpe"
    assert isinstance(observations, list)


def test_goes_echo_replaces_empty_rainviewer(engine_radar_policy_module):
    module = engine_radar_policy_module
    states = {
        "rainviewer": module.SourceState(True, True, 980.0),
        "noaa_goes_rrqpe": module.SourceState(True, True, 970.0),
    }
    decision = module.apply_echo_availability(
        module.EngineRadarDecision("rainviewer", "fallback", ("US",), 20.0),
        states, opera_observations=0, rainviewer_observations=0,
        goes_observations=3, now=1_000.0,
    )
    assert decision.source == "noaa_goes_rrqpe"
