"""Parity and safety contracts for worker-owned satellite providers."""
from __future__ import annotations

import gzip
import io
from pathlib import Path
from types import SimpleNamespace

import h5py
import numpy as np
import pytest
from pyproj import CRS

from storm_tracker_worker import lightning, satellite_radar
from storm_tracker_worker.service import (
    PRODUCTION_JOB_EFFECT_CONTRACTS,
    PRODUCTION_JOB_HANDLERS,
)

ROOT = Path(__file__).parents[1]


def _hsaf_body(*, quality=80, rain_raw=50):
    output = io.BytesIO()
    crs = CRS.from_proj4(
        "+proj=geos +lon_0=0 +h=35785831 +a=6378169 +b=6356584 +sweep=y"
    )
    with h5py.File(output, "w") as product:
        product.create_dataset("nx", data=np.linspace(-100_000, 100_000, 21))
        product.create_dataset("ny", data=np.linspace(-100_000, 100_000, 21))
        projection = product.create_dataset(
            "geostationary_projection", data=np.uint8(0)
        )
        for key, value in crs.to_cf().items():
            if isinstance(value, (str, int, float, np.number)):
                projection.attrs[key] = value
        values = np.zeros((21, 21), dtype=np.int16)
        values[9:12, 9:12] = rain_raw
        rain = product.create_dataset("rr", data=values)
        rain.attrs["scale_factor"] = 0.1
        rain.attrs["add_offset"] = 0.0
        rain.attrs["missing_value"] = -990
        product.create_dataset(
            "qind", data=np.full((21, 21), quality, dtype=np.int8)
        )
    return gzip.compress(output.getvalue())


def _goes_body():
    output = io.BytesIO()
    with h5py.File(output, "w") as product:
        product.attrs["time_coverage_start"] = "2026-07-22T01:40:20.3Z"
        x = product.create_dataset("x", data=np.arange(-10, 11, dtype=np.int16))
        y = product.create_dataset("y", data=np.arange(10, -11, -1, dtype=np.int16))
        for axis in (x, y):
            axis.attrs["scale_factor"] = 0.000056
            axis.attrs["add_offset"] = 0.0
        projection = product.create_dataset(
            "goes_imager_projection", data=np.int32(0)
        )
        projection.attrs["perspective_point_height"] = 35786023.0
        projection.attrs["semi_major_axis"] = 6378137.0
        projection.attrs["semi_minor_axis"] = 6356752.31414
        projection.attrs["latitude_of_projection_origin"] = 0.0
        projection.attrs["longitude_of_projection_origin"] = -75.0
        projection.attrs["sweep_angle_axis"] = "x"
        values = np.zeros((21, 21), dtype=np.uint16)
        values[9:12, 9:12] = 1000
        rain = product.create_dataset("RRQPE", data=values)
        rain.attrs["scale_factor"] = 0.01
        rain.attrs["add_offset"] = 0.0
        rain.attrs["_FillValue"] = 65535
        product.create_dataset(
            "DQF", data=np.zeros((21, 21), dtype=np.uint8)
        )
    return output.getvalue()


def _eumetsat_body():
    output = io.BytesIO()
    with h5py.File(output, "w") as product:
        group = product.create_group("data").create_group("lightning")
        latitude = group.create_dataset(
            "latitude", data=np.array([18545, 32767], dtype=np.int16)
        )
        latitude.attrs["scale_factor"] = 0.00275
        latitude.attrs["_FillValue"] = np.int16(32767)
        longitude = group.create_dataset(
            "longitude", data=np.array([1629, -1600], dtype=np.int16)
        )
        longitude.attrs["scale_factor"] = 0.00275
        group.create_dataset(
            "flash_time", data=np.array([837561600.5, 837561601.0])
        )
    return output.getvalue()


def test_satellite_jobs_are_registered_kill_safe_worker_jobs():
    expected = {"hsaf_h40b_fetch", "goes_rrqpe_fetch", "eumetsat_li_fetch"}
    assert expected <= set(PRODUCTION_JOB_HANDLERS)
    assert expected <= set(PRODUCTION_JOB_EFFECT_CONTRACTS)
    assert all(
        PRODUCTION_JOB_EFFECT_CONTRACTS[name].hard_kill_safe
        for name in expected
    )


def test_worker_hsaf_parser_preserves_quality_geometry_and_clipping(monkeypatch):
    timestamp = 1_784_684_420.3
    monkeypatch.setattr(satellite_radar.time, "time", lambda: timestamp + 60)
    area = {"center_lat": 0.0, "center_lon": 0.0, "horizon_km": 100.0}
    result = satellite_radar._hsaf_parse(_hsaf_body(), (area,), timestamp)
    assert result["observations"]
    assert result["observations"][0]["quality"] == pytest.approx(0.8)
    assert result["observations"][0]["footprint_points"]
    assert result["overlay"]["runs"]
    assert result["rain_samples"] == [{
        "lat": 0.0,
        "lon": 0.0,
        "timestamp": timestamp,
        "rain_rate_mm_h": pytest.approx(5.0),
        "quality": pytest.approx(0.8),
        "source": "hsaf_h40b",
        "unit": "mm/h",
    }]
    assert all(
        satellite_radar._contains(area, value["lat"], value["lon"])
        for value in result["observations"]
    )


def test_worker_hsaf_parser_keeps_low_quality_echo_out(monkeypatch):
    timestamp = 1_784_684_420.3
    monkeypatch.setattr(satellite_radar.time, "time", lambda: timestamp + 60)
    result = satellite_radar._hsaf_parse(
        _hsaf_body(quality=5),
        ({"center_lat": 0.0, "center_lon": 0.0, "horizon_km": 100.0},),
        timestamp,
    )
    assert result["observations"] == []
    assert result["overlay"]["runs"] == []


def test_component_quality_is_calculated_per_cell_and_noise_is_ignored():
    grid = np.zeros((7, 7), dtype=np.uint8)
    grid[1, 1:3] = 2
    grid[4, 4:6] = 5
    grid[6, 0] = 8  # isolated retrieval noise
    quality = np.zeros(grid.shape, dtype=np.float32)
    quality[1, 1:3] = 0.2
    quality[4, 4:6] = 0.9

    observations = satellite_radar._component_observations(
        grid,
        source="hsaf_h40b",
        timestamp=1_000.0,
        quality=0.0,
        quality_grid=quality,
        corner_to_latlon=lambda row, column: (float(row), float(column)),
        minimum_pixels=2,
        maximum_results=10,
    )

    assert len(observations) == 2
    assert sorted(value["quality"] for value in observations) == pytest.approx(
        [0.2, 0.9]
    )
    assert sorted(value["intensity"] for value in observations) == [2, 5]


def test_satellite_grid_is_clipped_to_true_circular_horizon():
    class IdentityProjection:
        @staticmethod
        def transform(xs, ys):
            return xs, ys

    grid = np.zeros((3, 3), dtype=np.uint8)
    grid[1, 1] = 3  # 0,0: inside
    grid[2, 2] = 3  # 1,1: outside a 50 km circle
    satellite_radar._clip_grid_to_area(
        grid,
        {"center_lat": 0.0, "center_lon": 0.0, "horizon_km": 50.0},
        IdentityProjection(),
        np.asarray([-1.0, 0.0, 1.0]),
        np.asarray([-1.0, 0.0, 1.0]),
        slice(0, 3),
        slice(0, 3),
        1.0,
        1.0,
    )
    assert grid[1, 1] == 3
    assert grid[2, 2] == 0


def test_satellite_product_times_reject_stale_and_future_frames(monkeypatch):
    monkeypatch.setattr(satellite_radar.time, "time", lambda: 10_000.0)
    with pytest.raises(ValueError, match="te oud"):
        satellite_radar._validate_product_time(1_000.0, 60.0, "test")
    with pytest.raises(ValueError, match="toekomst"):
        satellite_radar._validate_product_time(
            10_000.0 + satellite_radar.MAX_FUTURE_SKEW + 1.0,
            60.0,
            "test",
        )


def test_goes_fetch_preserves_each_satellite_product_time(monkeypatch):
    now = 20_000.0
    captured = {}
    satellite_radar._GOES_CACHE.clear()
    monkeypatch.setattr(satellite_radar.time, "time", lambda: now)
    monkeypatch.setattr(
        satellite_radar, "_latest_goes_key", lambda satellite: f"key-{satellite}"
    )
    monkeypatch.setattr(satellite_radar, "_bounded_url", lambda *args: b"body")

    def parse(_payload, satellite, _areas):
        timestamp = now - (120.0 if satellite == 18 else 60.0)
        return {
            "provider": "noaa_goes_rrqpe",
            "frame_timestamp": timestamp,
            "observations": [],
            "overlay": {
                "source": "noaa_goes_rrqpe",
                "timestamp": timestamp,
                "runs": [],
            },
        }

    def write(_job_id, value):
        captured.update(value)
        return {
            "artifact_path": "/share/storm_tracker_worker/test.json.gz",
            "artifact_sha256": "0" * 64,
            "artifact_bytes": 1,
            "content_type": "application/json+gzip",
        }

    monkeypatch.setattr(satellite_radar, "_goes_parse", parse)
    monkeypatch.setattr(satellite_radar, "write_json_artifact", write)
    result = satellite_radar.goes_rrqpe_fetch(SimpleNamespace(
        job_id="test",
        payload={"areas": [
            {"center_lat": 35.0, "center_lon": -130.0, "horizon_km": 100.0},
            {"center_lat": 35.0, "center_lon": -75.0, "horizon_km": 100.0},
        ]},
    ))

    assert result["frame_timestamp"] == now - 120.0
    assert captured["overlay"]["timestamp"] == now - 120.0
    assert captured["overlays_by_satellite"]["18"]["timestamp"] == now - 120.0
    assert captured["overlays_by_satellite"]["19"]["timestamp"] == now - 60.0


def test_worker_goes_parser_preserves_projection_and_deduplicates_overlap(monkeypatch):
    timestamp = 1_784_684_420.3
    monkeypatch.setattr(satellite_radar.time, "time", lambda: timestamp + 60)
    area = {"center_lat": 0.0, "center_lon": -75.0, "horizon_km": 100.0}
    result = satellite_radar._goes_parse(_goes_body(), 19, (area, dict(area)))
    assert result["observations"]
    identities = {
        (round(value["lat"], 3), round(value["lon"], 3), value["intensity"])
        for value in result["observations"]
    }
    assert len(identities) == len(result["observations"])
    assert result["observations"][0]["quality"] == pytest.approx(0.9)
    assert result["overlay"]["runs"]
    assert result["rain_samples"] == [{
        "lat": 0.0,
        "lon": -75.0,
        "timestamp": timestamp,
        "rain_rate_mm_h": pytest.approx(10.0),
        "quality": pytest.approx(1.0),
        "source": "noaa_goes19_rrqpe",
        "unit": "mm/h",
    }]


def test_worker_eumetsat_parser_matches_cf_semantics():
    result = lightning.parse_eumetsat_flashes(_eumetsat_body())
    assert len(result) == 1
    assert result[0]["lat"] == pytest.approx(50.99875)
    assert result[0]["lon"] == pytest.approx(4.47975)
    assert result[0]["timestamp"] == pytest.approx(1_784_246_400.5)


def test_live_satellite_adapters_are_worker_only_and_secret_free():
    paths = {
        "hsaf_h40b.py": "hsaf_h40b_fetch",
        "noaa_goes_rrqpe.py": "goes_rrqpe_fetch",
        "eumetsat_li.py": "eumetsat_li_fetch",
    }
    for filename, job_type in paths.items():
        source = (ROOT / "custom_components/storm_tracker_v3/providers" / filename).read_text(
            encoding="utf-8"
        )
        assert job_type in source
        assert "credential_ref" in source or job_type == "goes_rrqpe_fetch"
        assert "lokale verwerking blijft uit" in source
    runtime = (ROOT / "custom_components/storm_tracker_v3/__init__.py").read_text(
        encoding="utf-8"
    )
    assert "hsaf_username, hsaf_password, hass=hass" in runtime
    assert "http_session, hass=hass" in runtime
    assert "hass=hass," in runtime


def test_worker_secrets_come_from_addon_environment():
    config = (ROOT / "storm_tracker_worker/config.yaml").read_text(encoding="utf-8")
    runner = (ROOT / "storm_tracker_worker/run.sh").read_text(encoding="utf-8")
    for option, environment in (
        ("knmi_api_key", "STV3_KNMI_API_KEY"),
        ("hsaf_password", "STV3_HSAF_PASSWORD"),
        ("eumetsat_consumer_secret", "STV3_EUMETSAT_CONSUMER_SECRET"),
        ("meteofrance_api_token", "STV3_METEOFRANCE_API_TOKEN"),
    ):
        assert option in config
        assert environment in runner
