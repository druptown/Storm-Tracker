from __future__ import annotations

import json

import numpy as np

from storm_tracker_worker.thin_provider_adapters import ThinRadarField, ThinVisualRadarField
from types import SimpleNamespace
from pyproj import Transformer
from storm_tracker_worker.thin_radar_overlay import (
    build_radar_overlay,
    build_radar_overlays,
    crop_visual_radar_field,
)


def test_mercator_visual_pixel_is_cropped_and_drawn_at_miami_not_as_degrees():
    x, y = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True).transform(-80.1918, 25.7617)
    levels = np.zeros((3, 3), dtype=np.uint8)
    levels[1, 1] = 5
    field = ThinVisualRadarField("rainviewer", "visual", 100., x-1500, y+1500, 1000., 1000., levels, crs="EPSG:3857")
    region = SimpleNamespace(center_latitude=25.7617, center_longitude=-80.1918,
                             radar_analysis_radius_km=250., observation_radius_km=250.)
    cropped = crop_visual_radar_field(field, region)
    assert cropped is not None
    overlay = build_radar_overlay(cropped)
    assert len(overlay["runs"]) == 1
    assert overlay["quantity"] == "visual_class"
    latitude, longitude = overlay["runs"][0]["ring"][0]
    assert 25.765 < latitude < 25.767
    assert -80.197 < longitude < -80.195
    region.center_longitude = 4.42
    region.center_latitude = 51.05
    assert crop_visual_radar_field(field, region) is None


def test_fragmented_radar_overlay_coarsens_within_run_budget():
    rows, columns = np.indices((64, 64))
    values = np.where((rows + columns) % 2 == 0, 0.2, 0.6)
    field = ThinRadarField(
        provider_id="fusion:region:test",
        product_id="regional-rain-rate",
        observed_at=100.0,
        crs="EPSG:3857",
        origin_x_m=0.0,
        origin_y_m=64_000.0,
        x_resolution_m=1_000.0,
        y_resolution_m=1_000.0,
        rain_rate_mm_h=values,
        valid_mask=np.ones(values.shape, dtype=bool),
    )

    overlay = build_radar_overlay(field, maximum_runs=128)

    assert 0 < len(overlay["runs"]) <= 128
    assert overlay["downsample_factor"] > 1
    assert overlay["source_shape"] == [64, 64]
    assert len(json.dumps(overlay).encode("utf-8")) < 40_000


def test_multiple_overlays_share_the_total_budget_before_downsampling():
    """A busy region must not be coarsened by an artificial equal split."""
    rows, columns = np.indices((64, 64))
    fragmented = np.where((rows + columns) % 2 == 0, 0.2, 0.6)
    smooth = np.full((64, 64), 0.2)

    def field(provider_id, values):
        return ThinRadarField(
            provider_id=provider_id,
            product_id="regional-rain-rate",
            observed_at=100.0,
            crs="EPSG:3857",
            origin_x_m=0.0,
            origin_y_m=64_000.0,
            x_resolution_m=1_000.0,
            y_resolution_m=1_000.0,
            rain_rate_mm_h=values,
            valid_mask=np.ones(values.shape, dtype=bool),
        )

    overlays = build_radar_overlays(
        (
            ("region:smooth", field("fusion:smooth", smooth)),
            ("region:fragmented", field("fusion:fragmented", fragmented)),
        ),
        maximum_total_runs=5_000,
    )

    assert overlays["region:smooth"]["downsample_factor"] == 1
    assert overlays["region:fragmented"]["downsample_factor"] == 1
    assert sum(len(item["runs"]) for item in overlays.values()) <= 5_000


def test_multiple_overlays_use_one_common_downsample_factor_when_required():
    rows, columns = np.indices((64, 64))
    fragmented = np.where((rows + columns) % 2 == 0, 0.2, 0.6)
    fields = tuple(
        (
            f"region:{index}",
            ThinRadarField(
                provider_id=f"fusion:{index}",
                product_id="regional-rain-rate",
                observed_at=100.0,
                crs="EPSG:3857",
                origin_x_m=0.0,
                origin_y_m=64_000.0,
                x_resolution_m=1_000.0,
                y_resolution_m=1_000.0,
                rain_rate_mm_h=fragmented,
                valid_mask=np.ones(fragmented.shape, dtype=bool),
            ),
        )
        for index in range(2)
    )

    overlays = build_radar_overlays(fields, maximum_total_runs=1_000)

    factors = {item["downsample_factor"] for item in overlays.values()}
    assert len(factors) == 1
    assert next(iter(factors)) > 1
    assert sum(len(item["runs"]) for item in overlays.values()) <= 1_000


def test_shared_four_thousand_run_budget_stays_within_mqtt_payload_headroom():
    """The live resolution budget must leave room for non-radar GeoJSON."""
    row_numbers = np.indices((2_000, 2))[0]
    striped = np.where(row_numbers % 2 == 0, 0.2, 0.6)

    def field(index):
        return ThinRadarField(
            provider_id=f"fusion:{index}",
            product_id="regional-rain-rate",
            observed_at=100.0,
            crs="EPSG:3857",
            origin_x_m=0.0,
            origin_y_m=2_000_000.0,
            x_resolution_m=1_000.0,
            y_resolution_m=1_000.0,
            rain_rate_mm_h=striped,
            valid_mask=np.ones(striped.shape, dtype=bool),
        )

    overlays = build_radar_overlays(
        tuple((f"region:{index}", field(index)) for index in range(2)),
        maximum_total_runs=4_000,
    )

    encoded_size = len(
        json.dumps(overlays, separators=(",", ":")).encode("utf-8")
    )
    assert sum(len(item["runs"]) for item in overlays.values()) == 4_000
    assert encoded_size < 500_000
