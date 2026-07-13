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
            img.putpixel((x, y), (200, 200, 200, 255))  # helder = nat/intens

    buf = io.BytesIO()
    img.save(buf, format="PNG")

    provider = rainviewer_module.RainViewerProvider(51.026, 4.478)
    obs = provider._extract_observations(buf.getvalue(), tx=16, ty=10)

    assert obs, "verwachtte minstens één RADAR-observatie uit het heldere blok"
    assert all(o.source == "rainviewer" for o in obs)
    assert all(o.intensity >= 1 for o in obs)


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

        async def _path():
            return "https://example.test/frame"

        async def _observations(path):
            nonlocal calls
            calls += 1
            return expected

        provider._fetch_latest_path = _path
        provider._fetch_tile_observations = _observations
        first = await provider.fetch_observations()
        second = await provider.fetch_observations()
        return first, second, calls

    first, second, calls = asyncio.run(_run())
    assert first == second
    assert calls == 1
