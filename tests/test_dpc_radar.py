from io import BytesIO

import numpy as np
from PIL import Image, TiffImagePlugin


def test_dpc_coverage_is_limited_to_italy(dpc_radar_module, base_module):
    rome = base_module.CoverageArea(41.90, 12.50, 250)
    miami = base_module.CoverageArea(25.76, -80.19, 250)
    provider = dpc_radar_module.DpcRadarProvider(None)
    assert provider.supports(rome).supported
    assert not provider.supports(miami).supported


def test_dpc_sri_geotiff_decodes_fresh_rain(dpc_radar_module):
    values = np.zeros((8, 8), dtype=np.float32)
    values[4, 4] = 12.0
    image = Image.fromarray(values, mode="F")
    tags = TiffImagePlugin.ImageFileDirectory_v2()
    tags[33922] = (0.0, 0.0, 0.0, -600000.0, 650000.0, 0.0)
    tags[33550] = (1000.0, 1000.0, 0.0)
    payload = BytesIO()
    image.save(payload, format="TIFF", tiffinfo=tags)

    observations = dpc_radar_module.parse_sri_geotiff(
        payload.getvalue(), (), timestamp=1_000.0, now=1_100.0
    )
    assert len(observations) == 1
    assert observations[0].source == "dpc_radar"
    assert observations[0].intensity > 0
    assert 35.0 < observations[0].lat < 48.0
    assert 4.0 < observations[0].lon < 21.0


def test_dpc_product_exposes_georeferenced_intensity_runs(dpc_radar_module):
    values = np.zeros((8, 10), dtype=np.float32)
    values[3, 2:5] = 1.0
    values[3, 6:9] = 12.0
    image = Image.fromarray(values, mode="F")
    tags = TiffImagePlugin.ImageFileDirectory_v2()
    tags[33922] = (0.0, 0.0, 0.0, -600000.0, 650000.0, 0.0)
    tags[33550] = (1000.0, 1000.0, 0.0)
    payload = BytesIO()
    image.save(payload, format="TIFF", tiffinfo=tags)

    observations, overlay = dpc_radar_module._parse_sri_product(
        payload.getvalue(), (), timestamp=1_000.0, now=1_100.0
    )

    assert len(observations) == 2
    assert overlay["source"] == "dpc_radar"
    assert len(overlay["runs"]) == 2
    assert overlay["runs"][0]["intensity"] < overlay["runs"][1]["intensity"]
    assert all(len(run["ring"]) == 4 for run in overlay["runs"])


def test_dpc_rejects_stale_frame(dpc_radar_module):
    import pytest
    with pytest.raises(ValueError, match="ouder"):
        dpc_radar_module.parse_sri_geotiff(b"", (), timestamp=0.0, now=2_000.0)
