"""Storm Tracker V3 — tests/test_kmi.py v0.1.0"""
from __future__ import annotations

import io

import pytest


def test_pixel_to_latlon_top_left_corner(kmi_module):
    lat, lon = kmi_module.pixel_to_latlon(0, 0, width=512, height=512)
    assert lat == pytest.approx(kmi_module.KMI_LAT_TOP)
    assert lon == pytest.approx(kmi_module.KMI_LON_LEFT)


def test_pixel_to_latlon_bottom_right_corner(kmi_module):
    lat, lon = kmi_module.pixel_to_latlon(512, 512, width=512, height=512)
    assert lat == pytest.approx(kmi_module.KMI_LAT_BOTTOM)
    assert lon == pytest.approx(kmi_module.KMI_LON_RIGHT)


def test_pixel_to_latlon_center(kmi_module):
    lat, lon = kmi_module.pixel_to_latlon(256, 256, width=512, height=512)
    expected_lat = (kmi_module.KMI_LAT_TOP + kmi_module.KMI_LAT_BOTTOM) / 2
    expected_lon = (kmi_module.KMI_LON_LEFT + kmi_module.KMI_LON_RIGHT) / 2
    assert lat == pytest.approx(expected_lat, abs=0.01)
    assert lon == pytest.approx(expected_lon, abs=0.01)


def test_color_to_intensity_transparent_is_zero(kmi_module):
    assert kmi_module._color_to_intensity(255, 0, 0, a=50) == 0  # a < 128


def test_color_to_intensity_matches_known_colors(kmi_module):
    # Exacte kleuren uit de KMI-schaal moeten hun eigen niveau teruggeven
    assert kmi_module._color_to_intensity(144, 238, 144, 255) == 1
    assert kmi_module._color_to_intensity(255, 0, 0, 255) == 7
    assert kmi_module._color_to_intensity(180, 0, 180, 255) == 8


def test_color_to_intensity_far_from_any_scale_color_is_zero(kmi_module):
    # Puur wit ligt >20000 (kwadratische afstand) van elke schaalkleur -> 0
    assert kmi_module._color_to_intensity(255, 255, 255, 255) == 0


def test_ww_to_text_thresholds(kmi_module):
    assert kmi_module._ww_to_text(96) == "Onweer"
    assert kmi_module._ww_to_text(85) == "Buien"
    assert kmi_module._ww_to_text(65) == "Regen"
    assert kmi_module._ww_to_text(0) == "Onbekend"
    assert kmi_module._ww_to_text(5) == "Licht bewolkt"


def test_kmi_key_is_deterministic_for_same_day(kmi_module):
    key1 = kmi_module._kmi_key("getForecasts")
    key2 = kmi_module._kmi_key("getForecasts")
    assert key1 == key2  # zelfde dag -> zelfde md5-hash
    assert len(key1) == 32  # md5 hexdigest


def test_kmi_factory_supports_inside_coverage(kmi_module):
    assert kmi_module.KmiProviderFactory.supports(51.026, 4.478, 200.0) is True


def test_kmi_factory_rejects_far_outside(kmi_module):
    assert kmi_module.KmiProviderFactory.supports(-33.9, 18.4, 200.0) is False  # Kaapstad


def test_kmi_factory_create_returns_provider(kmi_module):
    factory = kmi_module.KmiProviderFactory()
    provider = factory.create(hass=None, center_lat=51.026, center_lon=4.478, radius_km=200.0)
    assert isinstance(provider, kmi_module.KmiProvider)


def test_extract_observations_from_synthetic_image(kmi_module):
    """Bouwt een klein synthetisch radarplaatje met één rode (intensiteit 7) pixel-blok."""
    from PIL import Image

    img = Image.new("RGBA", (16, 16), (0, 0, 0, 0))  # volledig transparant/droog
    # Zet een rood blok (KMI-schaalkleur voor intensiteit 7) met volle alpha
    for x in range(4, 8):
        for y in range(4, 8):
            img.putpixel((x, y), (255, 0, 0, 255))

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    image_data = buf.getvalue()

    provider = kmi_module.KmiProvider(51.026, 4.478)
    # stride=4 in de echte code -> met een 16x16 testplaatje raken we op zijn minst 1 sample
    obs = provider._extract_observations(image_data, timestamp=1234567890.0)

    assert obs, "verwachtte minstens één RADAR-observatie uit het rode blok"
    assert all(o.source == "kmi" for o in obs)
    assert all(o.obs_type == kmi_module.ObservationType.RADAR for o in obs)


def test_extract_observations_all_dry_gives_empty_list(kmi_module):
    from PIL import Image

    img = Image.new("RGBA", (16, 16), (0, 0, 0, 0))  # volledig transparant = droog
    buf = io.BytesIO()
    img.save(buf, format="PNG")

    provider = kmi_module.KmiProvider(51.026, 4.478)
    obs = provider._extract_observations(buf.getvalue(), timestamp=1234567890.0)
    assert obs == []
