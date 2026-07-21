"""Storm Tracker V3 — tests/test_rainviewer.py v0.1.0"""
from __future__ import annotations

import asyncio
import io

import pytest


def test_latlon_to_tile_and_back_is_consistent(rainviewer_module):
    """De tile die een punt oplevert moet dat punt ook binnen zijn grenzen bevatten."""
    lat, lon = 51.026, 4.478
    x, y = rainviewer_module._latlon_to_tile(lat, lon, zoom=5)
    lat_top, lat_bottom, lon_left, lon_right = rainviewer_module._tile_bounds(x, y, zoom=5)

    assert lat_bottom <= lat <= lat_top
    assert lon_left <= lon <= lon_right


def test_tile_bounds_equator_greenwich(rainviewer_module):
    """Tile (0,0) op zoom 1 dekt het NW-kwadrant van de wereldkaart."""
    lat_top, lat_bottom, lon_left, lon_right = rainviewer_module._tile_bounds(0, 0, zoom=1)
    assert lon_left == pytest.approx(-180.0)
    assert lon_right == pytest.approx(0.0)
    assert lat_top > 0  # noordelijk halfrond


def test_pixel_to_latlon_tile_corners(rainviewer_module):
    lat, lon = rainviewer_module._pixel_to_latlon_tile(
        0, 0, tile_size=256, lat_top=52.0, lat_bottom=50.0, lon_left=4.0, lon_right=6.0
    )
    assert lat == pytest.approx(52.0)
    assert lon == pytest.approx(4.0)

    lat2, lon2 = rainviewer_module._pixel_to_latlon_tile(
        256, 256, tile_size=256, lat_top=52.0, lat_bottom=50.0, lon_left=4.0, lon_right=6.0
    )
    assert lat2 == pytest.approx(50.0)
    assert lon2 == pytest.approx(6.0)


def test_rainviewer_factory_always_supports(rainviewer_module):
    assert rainviewer_module.RainViewerProviderFactory.supports(0.0, 0.0, 100.0) is True
    assert rainviewer_module.RainViewerProviderFactory.supports(89.9, 179.9, 100.0) is True


def test_rainviewer_factory_create_returns_provider(rainviewer_module):
    factory = rainviewer_module.RainViewerProviderFactory()
    provider = factory.create(hass=None, center_lat=51.026, center_lon=4.478, radius_km=200.0)
    assert isinstance(provider, rainviewer_module.RainViewerProvider)


def test_extract_observations_from_synthetic_tile(rainviewer_module):
    from PIL import Image

    img = Image.new("RGBA", (256, 256), (0, 0, 0, 0))  # droog
    for x in range(100, 120):
        for y in range(100, 120):
            img.putpixel((x, y), (182, 169, 126, 130))  # Universal Blue, 8 dBZ

    buf = io.BytesIO()
    img.save(buf, format="PNG")

    provider = rainviewer_module.RainViewerProvider(51.026, 4.478)
    obs = provider._extract_observations(buf.getvalue(), tx=16, ty=10)

    assert obs, "verwachtte minstens één RADAR-observatie uit het heldere blok"
    assert all(o.source == "rainviewer" for o in obs)
    assert all(o.intensity >= 1 for o in obs)
    assert len(obs) == 1
    assert obs[0].footprint_points[0] == obs[0].footprint_points[-1]
    assert obs[0].radar_cell_id.startswith("rainviewer:")


def test_opaque_grey_pixels_do_not_count_as_rain(rainviewer_module):
    assert rainviewer_module._universal_blue_intensity(200, 200, 200, 255) == 0


def test_current_tile_url_contains_required_size_and_unsmoothed_palette(
    rainviewer_module,
):
    assert rainviewer_module._tile_url("https://tiles/frame", 16, 10) == (
        "https://tiles/frame/256/5/16/10/2/0_0.png"
    )


def test_extract_observations_all_dry_gives_empty_list(rainviewer_module):
    from PIL import Image

    img = Image.new("RGBA", (256, 256), (0, 0, 0, 0))
    buf = io.BytesIO()
    img.save(buf, format="PNG")

    provider = rainviewer_module.RainViewerProvider(51.026, 4.478)
    obs = provider._extract_observations(buf.getvalue(), tx=16, ty=10)
    assert obs == []


def test_same_frame_reuses_last_observations(rainviewer_module, observation_module):
    async def _run():
        provider = rainviewer_module.RainViewerProvider(48.3904, -4.4861)
        calls = 0
        expected = [
            observation_module.Observation(
                obs_type=observation_module.ObservationType.RADAR,
                lat=48.4,
                lon=-4.5,
                timestamp=1_000.0,
                intensity=4,
                source="rainviewer",
            )
        ]

        async def _frame():
            return rainviewer_module.RainViewerFrame(
                "https://example.test/frame", 1_000.0
            )

        async def _observations(path, frame_timestamp):
            nonlocal calls
            calls += 1
            assert frame_timestamp == 1_000.0
            return expected

        provider._fetch_latest_frame = _frame
        provider._fetch_tile_observations = _observations
        rainviewer_module.time.time = lambda: 1_100.0
        first = await provider.fetch_observations()
        second = await provider.fetch_observations()
        return first, second, calls, provider.diagnostics

    first, second, calls, diagnostics = asyncio.run(_run())
    assert first == second
    assert calls == 1
    assert diagnostics["healthy"] is True
    assert diagnostics["last_frame_ts"] == 1_000.0


def test_stale_frame_is_rejected(rainviewer_module):
    async def _run():
        provider = rainviewer_module.RainViewerProvider(51.026, 4.478)

        async def _frame():
            return rainviewer_module.RainViewerFrame(
                "https://example.test/stale", 1_000.0
            )

        async def _must_not_fetch(path, frame_timestamp):
            raise AssertionError("stale frame mag geen tiles ophalen")

        provider._fetch_latest_frame = _frame
        provider._fetch_tile_observations = _must_not_fetch
        rainviewer_module.time.time = lambda: 2_201.0
        observations = await provider.fetch_observations()
        return observations, provider.diagnostics

    observations, diagnostics = asyncio.run(_run())
    assert observations == []
    assert diagnostics["healthy"] is False
    assert diagnostics["frame_age_minutes"] == 20.0
    assert "20.0 minuten oud" in diagnostics["last_error"]


def test_frame_timestamp_is_used_for_observations(rainviewer_module):
    from PIL import Image

    image = Image.new("RGBA", (256, 256), (0, 0, 0, 0))
    image.putpixel((0, 0), (255, 255, 255, 255))
    buf = io.BytesIO()
    image.save(buf, format="PNG")

    provider = rainviewer_module.RainViewerProvider(51.026, 4.478)
    observations = provider._extract_observations(
        buf.getvalue(), tx=16, ty=10, frame_timestamp=12_345.0
    )
    assert observations
    assert all(observation.timestamp == 12_345.0 for observation in observations)
