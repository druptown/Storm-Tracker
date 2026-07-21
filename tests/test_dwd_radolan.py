"""Tests voor de DWD RADOLAN RV-provider."""
from __future__ import annotations

from datetime import datetime, timezone
import io
import tarfile

import h5py
import numpy as np
import pytest


def _archive(timestamp: datetime, *, wet=True) -> bytes:
    hdf = io.BytesIO()
    with h5py.File(hdf, "w") as root:
        root.attrs["Conventions"] = np.bytes_(b"ODIM_H5/V2_3")
        dataset = root.create_group("dataset1")
        frame_what = dataset.create_group("what")
        frame_what.attrs["enddate"] = np.bytes_(timestamp.strftime("%Y%m%d").encode())
        frame_what.attrs["endtime"] = np.bytes_(timestamp.strftime("%H%M%S").encode())
        data1 = dataset.create_group("data1")
        values = np.zeros((8, 8), dtype=np.uint32)
        if wet:
            values[4, 4] = 1000  # 0.999 mm/5 min, circa 12 mm/u
        data1.create_dataset("data", data=values)
        what = data1.create_group("what")
        what.attrs["gain"] = 0.001
        what.attrs["offset"] = -0.001
        what.attrs["nodata"] = float(2**32 - 1)
        what.attrs["undetect"] = 0.0
        where = root.create_group("where")
        where.attrs["projdef"] = np.bytes_(
            b"+proj=stere +lat_ts=60 +lat_0=90 +lon_0=10 "
            b"+x_0=543196.835217764 +y_0=3622588.861931002 "
            b"+units=m +a=6378137 +b=6356752.3142451802 +no_defs"
        )
        where.attrs["xscale"] = 1000.0
        where.attrs["yscale"] = 1000.0
        where.attrs["xsize"] = 8
        where.attrs["ysize"] = 8
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w") as output:
        payload = hdf.getvalue()
        info = tarfile.TarInfo("composite_rv_test_000-hd5")
        info.size = len(payload)
        output.addfile(info, io.BytesIO(payload))
    return archive.getvalue()


def test_parse_current_rv_frame(dwd_radolan_module, base_module):
    timestamp = datetime.now(timezone.utc).replace(microsecond=0)
    world = base_module.CoverageArea(50, 10, 5000)
    overlays = []
    observations = dwd_radolan_module.parse_rv_archive(
        _archive(timestamp), (world,), now=timestamp.timestamp(),
        overlay_out=overlays,
    )
    assert len(observations) == 1
    assert observations[0].source == "dwd_radolan"
    assert observations[0].intensity >= 1
    assert observations[0].area_km2 == 1
    assert observations[0].footprint_points[0] == observations[0].footprint_points[-1]
    assert observations[0].parent_system_id
    assert overlays[0]["source"] == "dwd_radolan"
    assert overlays[0]["runs"]


def test_stale_rv_frame_is_rejected(dwd_radolan_module, base_module):
    timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="ouder dan 15 minuten"):
        dwd_radolan_module.parse_rv_archive(
            _archive(timestamp),
            (base_module.CoverageArea(50, 10, 5000),),
            now=timestamp.timestamp() + 16 * 60,
        )


def test_dwd_coverage_wakes_for_german_engine(dwd_radolan_module, base_module):
    provider = dwd_radolan_module.DwdRadolanProvider(session=None)
    assert provider.supports(base_module.CoverageArea(51, 10, 300)).supported
    assert not provider.supports(base_module.CoverageArea(25, -80, 300)).supported
