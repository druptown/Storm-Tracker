from __future__ import annotations

from datetime import datetime, timezone
import io

import h5py
import numpy as np
import pytest

import storm_tracker_worker.enterprise_rainrate as enterprise_rainrate
from storm_tracker_worker.enterprise_rainrate import (
    MAX_FILE_BYTES,
    MAX_LIST_RESPONSE_BYTES,
    _windows,
    parse_enterprise_rainrate,
)


def _product(*, timestamp="2026-07-28T14:00:00Z") -> bytes:
    stream = io.BytesIO()
    with h5py.File(stream, "w") as product:
        product.attrs["time_coverage_start"] = timestamp
        product.attrs["geospatial_lat_min"] = np.asarray([-1.0], np.float32)
        product.attrs["geospatial_lat_max"] = np.asarray([1.0], np.float32)
        product.attrs["geospatial_lon_min"] = np.asarray([-2.0], np.float32)
        product.attrs["geospatial_lon_max"] = np.asarray([1.0], np.float32)
        product.attrs["geospatial_lat_resolution"] = np.asarray([1.0], np.float32)
        product.attrs["geospatial_lon_resolution"] = np.asarray([1.0], np.float32)
        rr = product.create_dataset(
            "RRQPE",
            data=np.asarray([
                [0, 0, 0, 0],
                [0, 12, 15, 0],
                [0, 0, 0, 0],
            ], dtype=np.int16),
        )
        rr.attrs["scale_factor"] = np.asarray([0.1], np.float32)
        rr.attrs["add_offset"] = np.asarray([0.0], np.float32)
        rr.attrs["_FillValue"] = np.asarray([-9990], np.int16)
        product.create_dataset(
            "DQF",
            data=np.zeros((3, 4), dtype=np.int8),
        )
    return stream.getvalue()


def test_parser_reads_only_active_region_and_builds_cell():
    payload = _product()
    result = parse_enterprise_rainrate(
        payload,
        ({"center_lat": 0.0, "center_lon": 0.0, "horizon_km": 180.0},),
        now=datetime(2026, 7, 28, 14, 20, tzinfo=timezone.utc).timestamp(),
    )
    assert result["provider"] == "noaa_enterprise_rainrate"
    assert len(result["observations"]) == 1
    assert result["observations"][0]["source"] == "noaa_enterprise_rainrate"
    assert result["overlay"]["runs"]


def test_parser_rejects_stale_frame():
    try:
        parse_enterprise_rainrate(
            _product(timestamp="2026-07-28T12:00:00Z"),
            ({"center_lat": 0.0, "center_lon": 0.0, "horizon_km": 100.0},),
            now=datetime(2026, 7, 28, 14, 0, tzinfo=timezone.utc).timestamp(),
        )
    except ValueError as err:
        assert "ouder dan 45 minuten" in str(err)
    else:
        raise AssertionError("stale frame werd geaccepteerd")


def test_parser_rejects_oversized_payload():
    try:
        parse_enterprise_rainrate(
            b"x" * (MAX_FILE_BYTES + 1),
            ({"center_lat": 0.0, "center_lon": 0.0, "horizon_km": 100.0},),
        )
    except ValueError as err:
        assert "veiligheidslimiet" in str(err)
    else:
        raise AssertionError("te groot bestand werd geaccepteerd")


def test_dateline_area_is_split_into_two_windows():
    latitudes = np.asarray([1.0, 0.0, -1.0])
    longitudes = np.arange(-180.0, 180.0, 1.0)
    windows = _windows(
        {"center_lat": 0.0, "center_lon": 179.5, "horizon_km": 200.0},
        latitudes,
        longitudes,
    )
    assert len(windows) == 2


class _Response:
    def __init__(self, payload: bytes, *, headers=None):
        self._stream = io.BytesIO(payload)
        if headers is not None:
            self.headers = headers

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self, size=-1):
        return self._stream.read(size)


def test_enterprise_listing_accepts_missing_content_length(monkeypatch):
    payload = (
        b"<ListBucketResult><Contents><Key>frame.nc</Key>"
        b"<LastModified>2026-07-30T00:00:00Z</LastModified>"
        b"<Size>42</Size></Contents></ListBucketResult>"
    )
    monkeypatch.setattr(
        enterprise_rainrate,
        "urlopen",
        lambda *_args, **_kwargs: _Response(payload),
    )

    assert enterprise_rainrate._list_prefix("prefix/") == [{
        "key": "frame.nc",
        "modified": "2026-07-30T00:00:00Z",
        "size": 42,
    }]


@pytest.mark.parametrize(
    "declared",
    ("invalid", str(MAX_LIST_RESPONSE_BYTES + 1), "1"),
)
def test_enterprise_listing_rejects_invalid_or_lying_content_length(
    monkeypatch,
    declared,
):
    payload = b"<ListBucketResult />"
    monkeypatch.setattr(
        enterprise_rainrate,
        "urlopen",
        lambda *_args, **_kwargs: _Response(
            payload,
            headers={"Content-Length": declared},
        ),
    )

    with pytest.raises(ValueError):
        enterprise_rainrate._list_prefix("prefix/")
