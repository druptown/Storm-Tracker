"""Tests voor de DWD RADOLAN RV-provider."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
import tarfile
import threading

import h5py
import numpy as np
import pytest


def _archive(timestamp: datetime, *, wet=True, production_corners=False) -> bytes:
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
        if production_corners:
            where.attrs["UL_lon"] = 1.463301510256666
            where.attrs["UL_lat"] = 55.862087108249824
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


def test_worker_parser_matches_integration_parser(
    dwd_radolan_module, base_module,
):
    from storm_tracker_worker.radar import parse_rv_archive as worker_parse

    timestamp = datetime.now(timezone.utc).replace(microsecond=0)
    payload = _archive(timestamp)
    world = base_module.CoverageArea(50, 10, 5000)
    expected = dwd_radolan_module.parse_rv_archive(
        payload, (world,), now=timestamp.timestamp()
    )
    actual = worker_parse(
        payload,
        ({"center_lat": 50, "center_lon": 10, "horizon_km": 5000},),
        now=timestamp.timestamp(),
    )

    assert actual["frame_timestamp"] == timestamp.timestamp()
    assert len(actual["observations"]) == len(expected)
    assert actual["observations"][0]["radar_cell_id"] == expected[0].radar_cell_id
    assert actual["observations"][0]["lat"] == expected[0].lat
    assert actual["observations"][0]["lon"] == expected[0].lon
    assert (
        tuple(actual["observations"][0]["footprint_points"])
        == expected[0].footprint_points
    )


def test_worker_odim_parser_matches_shared_integration_parser(
    odim_hdf5_module, base_module,
):
    from storm_tracker_worker.radar import parse_odim_rainfall as worker_parse

    timestamp = datetime.now(timezone.utc).replace(microsecond=0)
    archive_payload = _archive(timestamp, production_corners=True)
    with tarfile.open(fileobj=io.BytesIO(archive_payload), mode="r:") as archive:
        member = next(item for item in archive.getmembers() if item.isfile())
        stream = archive.extractfile(member)
        assert stream is not None
        payload = stream.read()
    world = base_module.CoverageArea(50, 10, 5000)
    expected = odim_hdf5_module.parse_odim_rainfall(
        payload,
        (world,),
        source="met_office_radar",
        quality=0.99,
        max_age_seconds=45 * 60,
        now=timestamp.timestamp(),
    )
    actual = worker_parse(
        payload,
        ({"center_lat": 50, "center_lon": 10, "horizon_km": 5000},),
        source="met_office_radar",
        quality=0.99,
        max_age_seconds=45 * 60,
        now=timestamp.timestamp(),
    )

    assert actual["frame_timestamp"] == timestamp.timestamp()
    assert len(actual["observations"]) == len(expected)
    assert actual["observations"][0]["radar_cell_id"] == expected[0].radar_cell_id
    assert actual["observations"][0]["lat"] == expected[0].lat
    assert actual["observations"][0]["lon"] == expected[0].lon
    assert (
        tuple(actual["observations"][0]["footprint_points"])
        == expected[0].footprint_points
    )


def test_production_corner_coordinates_are_used(
    dwd_radolan_module, base_module,
):
    timestamp = datetime.now(timezone.utc).replace(microsecond=0)
    payload = _archive(timestamp, production_corners=True)
    world = base_module.CoverageArea(50, 10, 5000)

    observation = dwd_radolan_module.parse_rv_archive(
        payload, (world,), now=timestamp.timestamp()
    )[0]

    assert 55.0 < observation.lat < 56.5
    assert 1.0 < observation.lon < 3.0


def test_stale_rv_frame_is_rejected(dwd_radolan_module, base_module):
    timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="ouder dan 15 minuten"):
        dwd_radolan_module.parse_rv_archive(
            _archive(timestamp),
            (base_module.CoverageArea(50, 10, 5000),),
            now=timestamp.timestamp() + 16 * 60,
        )


def test_tar_member_size_is_checked_before_extracting(
    dwd_radolan_module,
    monkeypatch,
):
    class Member:
        name = "DE1200_000-hd5"
        size = dwd_radolan_module.MAX_FRAME_BYTES + 1

        @staticmethod
        def isfile():
            return True

    class Archive:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        @staticmethod
        def getmembers():
            return [Member()]

        @staticmethod
        def extractfile(_member):
            raise AssertionError("oversized member must not be extracted")

    monkeypatch.setattr(
        dwd_radolan_module.tarfile,
        "open",
        lambda **_kwargs: Archive(),
    )

    with pytest.raises(ValueError, match="productlimiet"):
        dwd_radolan_module.parse_rv_archive(b"tar", ())


def test_dwd_coverage_wakes_for_german_engine(dwd_radolan_module, base_module):
    provider = dwd_radolan_module.DwdRadolanProvider(session=None)
    assert provider.supports(base_module.CoverageArea(51, 10, 300)).supported
    assert not provider.supports(base_module.CoverageArea(25, -80, 300)).supported


def test_workerless_poll_does_not_start_local_parse(
    dwd_radolan_module, monkeypatch,
):
    parse_calls = 0

    def slow_parse(payload, areas):
        nonlocal parse_calls
        parse_calls += 1
        return ["observation"], {"source": "dwd_radolan"}

    class Response:
        content_length = 4

        def raise_for_status(self):
            return None

        async def read(self):
            return b"data"

    class Request:
        async def __aenter__(self):
            return Response()

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    class Session:
        calls = 0

        def get(self, url):
            self.calls += 1
            return Request()

    async def scenario():
        session = Session()
        provider = dwd_radolan_module.DwdRadolanProvider(session)
        monkeypatch.setattr(dwd_radolan_module, "_parse_rv_job", slow_parse)
        assert await provider.async_fetch() == []
        assert session.calls == 0
        assert parse_calls == 0
        await provider.async_stop()

    asyncio.run(scenario())


def test_empty_shadow_uses_overlay_timestamp(dwd_radolan_module):
    assert dwd_radolan_module._shadow_frame_timestamp(
        [], {"source": "dwd_radolan", "timestamp": 1234.0}
    ) == 1234.0


def test_worker_artifact_is_validated_and_deserialized(
    dwd_radolan_module, tmp_path, monkeypatch,
):
    timestamp = datetime.now(timezone.utc).timestamp()
    payload = {
        "provider": "dwd_radolan",
        "frame_timestamp": timestamp,
        "observations": [{
            "obs_type": "radar",
            "lat": 50.94,
            "lon": 6.96,
            "timestamp": timestamp,
            "intensity": 3,
            "area_km2": 12.0,
            "quality": 0.98,
            "footprint_points": [[50.9, 6.9], [51.0, 7.0]],
            "radar_cell_id": "dwd_radolan:test:c1",
            "parent_system_id": "dwd_radolan:test:c1",
            "parent_area_km2": 12.0,
            "parent_footprint_points": [[50.9, 6.9], [51.0, 7.0]],
            "source": "dwd_radolan",
        }],
        "overlay": {
            "source": "dwd_radolan",
            "timestamp": timestamp,
            "runs": [],
        },
    }
    compressed = gzip.compress(
        json.dumps(payload).encode("utf-8"), mtime=0
    )
    artifact = tmp_path / "job.json.gz"
    artifact.write_bytes(compressed)
    monkeypatch.setattr(
        dwd_radolan_module, "WORKER_ARTIFACT_ROOT", tmp_path
    )

    observations, overlay = dwd_radolan_module.read_radar_worker_artifact({
        "artifact_path": str(artifact),
        "artifact_bytes": len(compressed),
        "artifact_sha256": hashlib.sha256(compressed).hexdigest(),
        "content_type": "application/json+gzip",
        "frame_timestamp": timestamp,
        "observation_count": 1,
    })

    assert observations[0].lat == 50.94
    assert observations[0].radar_cell_id == "dwd_radolan:test:c1"
    assert observations[0].footprint_points == ((50.9, 6.9), (51.0, 7.0))
    assert overlay["source"] == "dwd_radolan"


def test_worker_artifact_rejects_wrong_hash(
    dwd_radolan_module, tmp_path, monkeypatch,
):
    artifact = tmp_path / "job.json.gz"
    artifact.write_bytes(b"not-valid")
    monkeypatch.setattr(
        dwd_radolan_module, "WORKER_ARTIFACT_ROOT", tmp_path
    )

    with pytest.raises(ValueError, match="controlesom"):
        dwd_radolan_module.read_radar_worker_artifact({
            "artifact_path": str(artifact),
            "artifact_bytes": len(b"not-valid"),
            "artifact_sha256": "0" * 64,
            "observation_count": 0,
        })


def test_worker_artifact_rejects_path_outside_shared_root(
    dwd_radolan_module, tmp_path, monkeypatch,
):
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    outside = tmp_path / "outside.json.gz"
    outside.write_bytes(b"payload")
    monkeypatch.setattr(
        dwd_radolan_module, "WORKER_ARTIFACT_ROOT", allowed
    )

    with pytest.raises(ValueError, match="buiten"):
        dwd_radolan_module.read_radar_worker_artifact({
            "artifact_path": str(outside),
            "artifact_bytes": 7,
            "artifact_sha256": hashlib.sha256(b"payload").hexdigest(),
            "observation_count": 0,
        })
