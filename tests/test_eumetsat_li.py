"""Tests voor de EUMETSAT MTG Lightning Imager fallback."""
from __future__ import annotations

import io

import h5py
import numpy as np
import pytest


def _netcdf_body() -> bytes:
    stream = io.BytesIO()
    with h5py.File(stream, "w") as handle:
        group = handle.create_group("data").create_group("lightning")
        latitude = group.create_dataset(
            "latitude", data=np.array([18545, 32767], dtype=np.int16)
        )
        latitude.attrs["scale_factor"] = 0.00275
        latitude.attrs["add_offset"] = 0.0
        latitude.attrs["_FillValue"] = np.int16(32767)
        longitude = group.create_dataset(
            "longitude", data=np.array([1629, -1600], dtype=np.int16)
        )
        longitude.attrs["scale_factor"] = 0.00275
        flash_time = group.create_dataset(
            "flash_time", data=np.array([837561600.5, 837561601.0])
        )
    return stream.getvalue()


def test_parse_lightning_flashes_decodes_cf_values(eumetsat_li_module):
    observations = eumetsat_li_module.parse_lightning_flashes(_netcdf_body())

    assert len(observations) == 1
    observation = observations[0]
    assert observation.lat == pytest.approx(50.99875)
    assert observation.lon == pytest.approx(4.47975)
    assert observation.timestamp == pytest.approx(1784246400.5)
    assert observation.source == "eumetsat_li"
    assert observation.obs_type.value == "lightning"


def test_parse_lightning_flashes_rejects_missing_key_dataset(eumetsat_li_module):
    stream = io.BytesIO()
    with h5py.File(stream, "w") as handle:
        handle.create_dataset("latitude", data=np.array([1], dtype=np.int16))

    with pytest.raises(ValueError, match="mist"):
        eumetsat_li_module.parse_lightning_flashes(stream.getvalue())
