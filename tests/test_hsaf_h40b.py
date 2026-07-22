from __future__ import annotations

import gzip
import io
import os
from pathlib import Path

import h5py
import numpy as np
import pytest
from pyproj import CRS


def _product_bytes(*, quality=80, rain_raw=50):
    output = io.BytesIO()
    crs = CRS.from_proj4(
        "+proj=geos +lon_0=0 +h=35785831 +a=6378169 +b=6356584 +sweep=y"
    )
    with h5py.File(output, "w") as product:
        product.create_dataset("nx", data=np.linspace(-100_000, 100_000, 21))
        product.create_dataset("ny", data=np.linspace(-100_000, 100_000, 21))
        projection = product.create_dataset("geostationary_projection", data=np.uint8(0))
        for key, value in crs.to_cf().items():
            if isinstance(value, (str, int, float, np.number)):
                projection.attrs[key] = value
        rr = np.zeros((21, 21), dtype=np.int16)
        rr[9:12, 9:12] = rain_raw
        rr_dataset = product.create_dataset("rr", data=rr)
        rr_dataset.attrs["scale_factor"] = 0.1
        rr_dataset.attrs["add_offset"] = 0.0
        rr_dataset.attrs["missing_value"] = -990
        qind = np.full((21, 21), quality, dtype=np.int8)
        product.create_dataset("qind", data=qind)
    return gzip.compress(output.getvalue())


def test_h40b_filename_timestamp(hsaf_h40b_module):
    timestamp = hsaf_h40b_module._timestamp_from_name(
        "/h40B/h40_cur_mon_data/h40_20260722_0050_fdk.nc.gz"
    )
    assert timestamp == 1784681400.0


def test_h40b_decodes_projected_rain_and_quality(
    hsaf_h40b_module, base_module
):
    area = base_module.CoverageArea(0.0, 0.0, 100.0)
    observations, overlay = hsaf_h40b_module.parse_h40b_netcdf(
        _product_bytes(),
        (area,),
        timestamp=1_000.0,
        now=1_100.0,
    )
    assert observations
    assert observations[0].source == "hsaf_h40b"
    assert observations[0].quality == 0.8
    assert observations[0].intensity > 0
    assert observations[0].footprint_points
    assert overlay["source"] == "hsaf_h40b"
    assert overlay["runs"]


def test_h40b_rejects_low_quality_pixels(hsaf_h40b_module, base_module):
    area = base_module.CoverageArea(0.0, 0.0, 100.0)
    observations, overlay = hsaf_h40b_module.parse_h40b_netcdf(
        _product_bytes(quality=5),
        (area,),
        timestamp=1_000.0,
        now=1_100.0,
    )
    assert observations == []
    assert overlay["runs"] == []


def test_h40b_stale_frame_is_rejected(hsaf_h40b_module, base_module):
    area = base_module.CoverageArea(0.0, 0.0, 100.0)
    try:
        hsaf_h40b_module.parse_h40b_netcdf(
            _product_bytes(), (area,), timestamp=1_000.0, now=7_000.0
        )
    except ValueError as err:
        assert "ouder dan 90 minuten" in str(err)
    else:
        raise AssertionError("verouderd H40B-frame werd niet geweigerd")


def test_policy_uses_hsaf_after_radar_fallbacks_fail(engine_radar_policy_module):
    module = engine_radar_policy_module
    states = {
        "opera": module.SourceState(True, False),
        "rainviewer": module.SourceState(True, False),
        "hsaf_h40b": module.SourceState(True, True, 950.0),
    }
    decision = module.select_engine_radar_source(("GR",), states, now=1_000.0)
    assert decision.source == "hsaf_h40b"


def test_hsaf_echo_replaces_empty_rainviewer(engine_radar_policy_module):
    module = engine_radar_policy_module
    states = {
        "rainviewer": module.SourceState(True, True, 980.0),
        "hsaf_h40b": module.SourceState(True, True, 970.0),
    }
    initial = module.EngineRadarDecision("rainviewer", "fallback", ("GR",), 20.0)
    decision = module.apply_echo_availability(
        initial,
        states,
        opera_observations=0,
        rainviewer_observations=0,
        hsaf_observations=2,
        now=1_000.0,
    )
    assert decision.source == "hsaf_h40b"


def test_real_h40b_sample_when_available(hsaf_h40b_module, base_module):
    sample = os.environ.get("HSAF_H40B_SAMPLE")
    if not sample:
        pytest.skip("geen lokaal H40B-voorbeeld ingesteld")
    path = Path(sample)
    timestamp = hsaf_h40b_module._timestamp_from_name(path.name)
    observations, overlay = hsaf_h40b_module.parse_h40b_netcdf(
        path.read_bytes(),
        (base_module.CoverageArea(40.6401, 22.9444, 300.0),),
        timestamp=timestamp,
        now=timestamp + 60.0,
    )
    assert overlay["source"] == "hsaf_h40b"
    assert isinstance(overlay["runs"], list)
    assert all(item.source == "hsaf_h40b" for item in observations)
