from io import BytesIO
import tarfile

import numpy as np
from PIL import Image, TiffImagePlugin


def _geotiff():
    values = np.zeros((8, 8), dtype=np.uint8)
    values[2, 4] = 6
    image = Image.fromarray(values, mode="P")
    tags = TiffImagePlugin.ImageFileDirectory_v2()
    tags[33922] = (0.0, 0.0, 0.0, -12.11, 46.30, 0.0)
    tags[33550] = (0.0263317644, 0.0263317644, 0.0)
    payload = BytesIO()
    image.save(payload, format="TIFF", tiffinfo=tags)
    return payload.getvalue()


def test_aemet_archive_selects_latest_safe_frame(aemet_radar_module):
    archive_payload = BytesIO()
    with tarfile.open(fileobj=archive_payload, mode="w:gz") as archive:
        for name in ("down_radw202607201800_4326.tif", "down_radw202607201810_4326.tif"):
            frame = _geotiff()
            info = tarfile.TarInfo(name)
            info.size = len(frame)
            archive.addfile(info, BytesIO(frame))
    frame, timestamp, name = aemet_radar_module.latest_frame_from_archive(
        archive_payload.getvalue()
    )
    assert frame
    assert name == "down_radw202607201810_4326.tif"
    assert timestamp > 0


def test_aemet_geotiff_maps_intensity_and_coordinates(aemet_radar_module):
    observations = aemet_radar_module.parse_aemet_geotiff(
        _geotiff(), (), timestamp=1_000.0, now=1_100.0
    )
    assert len(observations) == 1
    assert observations[0].source == "aemet_radar"
    assert observations[0].intensity == 6
    assert -12.2 < observations[0].lon < 6.2
    assert 33.0 < observations[0].lat < 46.5


def test_aemet_coverage_is_limited_to_published_composite(aemet_radar_module, base_module):
    provider = aemet_radar_module.AemetRadarProvider(None)
    assert provider.supports(base_module.CoverageArea(40.4, -3.7, 250)).supported
    assert not provider.supports(base_module.CoverageArea(28.1, -15.4, 250)).supported
    assert not provider.supports(base_module.CoverageArea(25.8, -80.2, 250)).supported
