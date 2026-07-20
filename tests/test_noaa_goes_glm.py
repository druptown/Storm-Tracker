"""Tests voor de NOAA GOES GLM fallback."""
from __future__ import annotations

from datetime import datetime, timezone
import io

import h5py
import numpy as np
import pytest


def _glm_body() -> bytes:
    stream = io.BytesIO()
    with h5py.File(stream, "w") as handle:
        handle.create_dataset("flash_lat", data=np.array([40.0], dtype=np.float32))
        handle.create_dataset("flash_lon", data=np.array([-90.0], dtype=np.float32))
        offsets = handle.create_dataset(
            "flash_time_offset_of_first_event",
            data=np.array([-1000], dtype=np.int16),
        )
        offsets.attrs["_Unsigned"] = np.bytes_(b"true")
        offsets.attrs["scale_factor"] = np.array([0.00038148], dtype=np.float32)
        offsets.attrs["add_offset"] = np.array([-5.0], dtype=np.float32)
        offsets.attrs["units"] = np.bytes_(b"seconds since 2026-07-17 20:00:00.000")
    return stream.getvalue()


def test_parse_goes_flashes_decodes_unsigned_cf_time(noaa_goes_glm_module):
    observations = noaa_goes_glm_module.parse_goes_flashes(_glm_body(), 19)

    assert len(observations) == 1
    observation = observations[0]
    epoch = datetime(2026, 7, 17, 20, tzinfo=timezone.utc).timestamp()
    expected_offset = 64536 * float(np.float32(0.00038148)) - 5.0
    assert observation.timestamp == pytest.approx(epoch + expected_offset, abs=0.001)
    assert observation.lat == pytest.approx(40.0)
    assert observation.lon == pytest.approx(-90.0)
    assert observation.source == "noaa_goes19_glm"


@pytest.mark.parametrize(("longitude", "source"), [
    (4.4, "eumetsat_li"),
    (-75.0, "noaa_goes19_glm"),
    (-140.0, "noaa_goes18_glm"),
    (174.0, "noaa_goes18_glm"),
    (120.0, None),
])
def test_preferred_source_partitions_overlap(noaa_goes_glm_module, longitude, source):
    assert noaa_goes_glm_module.preferred_source_for_longitude(longitude) == source


def test_satellites_for_regions_keeps_europe_asleep(noaa_goes_glm_module):
    regions = [(51.05, 4.42, 250.0), (52.52, 13.405, 250.0)]

    assert noaa_goes_glm_module.satellites_for_regions(regions) == set()


def test_satellites_for_regions_selects_americas(noaa_goes_glm_module):
    regions = [(25.76, -80.19, 250.0), (37.77, -122.42, 250.0)]

    assert noaa_goes_glm_module.satellites_for_regions(regions) == {18, 19}
