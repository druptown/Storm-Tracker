from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
import tarfile
from types import SimpleNamespace

import h5py
import numpy as np
import pytest

from storm_tracker_worker import radar
from storm_tracker_worker.raw_artifacts import (
    RawArtifactCapacityError,
    RawArtifactConflictError,
    RawArtifactLimits,
    RawArtifactStore,
)


def _hdf_bytes(timestamp: datetime, *, wet: bool = True) -> bytes:
    stream = io.BytesIO()
    with h5py.File(stream, "w") as root:
        root.attrs["Conventions"] = np.bytes_(b"ODIM_H5/V2_3")
        dataset = root.create_group("dataset1")
        frame_what = dataset.create_group("what")
        frame_what.attrs["enddate"] = np.bytes_(
            timestamp.strftime("%Y%m%d").encode()
        )
        frame_what.attrs["endtime"] = np.bytes_(
            timestamp.strftime("%H%M%S").encode()
        )
        data1 = dataset.create_group("data1")
        values = np.zeros((8, 8), dtype=np.uint32)
        if wet:
            values[4, 4] = 1000
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
    return stream.getvalue()


def _member_name(timestamp: datetime, *, lead: int = 0) -> str:
    return (
        f"composite_rv_{timestamp:%Y%m%d}_{timestamp:%H%M}_"
        f"{lead:03d}-hd5"
    )


def _archive(
    timestamp: datetime,
    *,
    member_payload: bytes | None = None,
    filename_timestamp: datetime | None = None,
    include_forecast: bool = True,
) -> bytes:
    hdf = _hdf_bytes(timestamp) if member_payload is None else member_payload
    archive = io.BytesIO()
    filename_time = filename_timestamp or timestamp
    with tarfile.open(fileobj=archive, mode="w") as output:
        info = tarfile.TarInfo(_member_name(filename_time))
        info.size = len(hdf)
        output.addfile(info, io.BytesIO(hdf))
        if include_forecast:
            forecast = tarfile.TarInfo(_member_name(filename_time, lead=5))
            forecast.size = len(hdf)
            output.addfile(forecast, io.BytesIO(hdf))
    return archive.getvalue()


def _timestamp() -> datetime:
    return datetime.now(timezone.utc).replace(second=0, microsecond=0)


def _job(job_id: str = "dwd-rv-slot-001"):
    return SimpleNamespace(job_id=job_id, payload={"areas": []})


def _response(payload: bytes, received_at: float, content_type="application/x-tar"):
    return radar._DwdRvHttpResponse(
        payload=payload,
        content_type=content_type,
        received_at=received_at,
    )


def _install_responses(monkeypatch, responses):
    queue = iter(responses)
    monkeypatch.setattr(radar, "_download_rv_response", lambda: next(queue))


def _capture_artifacts(monkeypatch):
    values: list[dict] = []

    def write(job_id: str, value: dict):
        copy = deepcopy(value)
        values.append(copy)
        encoded = json.dumps(
            copy, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        digest = hashlib.sha256(encoded).hexdigest()
        return {
            "artifact_path": f"/share/storm_tracker_worker/{job_id}-{digest}.json.gz",
            "artifact_sha256": digest,
            "artifact_bytes": len(encoded),
            "content_type": "application/json+gzip",
        }

    monkeypatch.setattr(radar, "write_json_artifact", write)
    return values


def _only_raw_object(root) -> bytes:
    objects = list((root / "objects").glob("*.raw"))
    assert len(objects) == 1
    return objects[0].read_bytes()


def test_dwd_rv_http_archive_limit_fails_before_body_read(monkeypatch):
    class OversizeResponse(io.BytesIO):
        def __init__(self):
            super().__init__(b"must-not-be-read")
            self.headers = {
                "Content-Encoding": "identity",
                "Content-Length": str(radar.MAX_ARCHIVE_BYTES + 1),
                "Content-Type": "application/octet-stream",
            }

        def read(self, *_args, **_kwargs):
            pytest.fail("oversized response body werd gelezen")

    monkeypatch.setattr(
        radar, "urlopen", lambda *_args, **_kwargs: OversizeResponse()
    )

    with pytest.raises(ValueError, match="overschrijdt contractlimiet"):
        radar._download_rv_response()


def test_dwd_rv_exact_tar_is_durable_before_tar_decode(monkeypatch, tmp_path):
    timestamp = _timestamp()
    archive = _archive(timestamp)
    received_at = timestamp.timestamp() + 42.5
    store = RawArtifactStore(tmp_path / "raw")
    _install_responses(monkeypatch, [_response(archive, received_at)])
    artifacts = _capture_artifacts(monkeypatch)
    real_tar_open = radar.tarfile.open

    def checked_tar_open(*args, **kwargs):
        stats = store.stats()
        assert stats["object_count"] == 1
        assert stats["receipt_count"] == 1
        assert stats["capture_count"] == 0
        assert _only_raw_object(tmp_path / "raw") == archive
        return real_tar_open(*args, **kwargs)

    monkeypatch.setattr(radar.tarfile, "open", checked_tar_open)
    metadata = radar.dwd_radolan_fetch(_job(), raw_store=store)
    decoded = artifacts[0]
    raw = decoded["raw_provenance"]
    source = raw["raw_sources"]["rv_tar"]

    assert store.read_bytes(source["raw_receipt_id"]) == archive
    assert source["raw_content_sha256"] == hashlib.sha256(archive).hexdigest()
    assert source["raw_receipt_sha256"]
    assert source["received_at"] == received_at
    assert source["product_timestamp"] == timestamp.timestamp()
    assert source["received_at"] != source["product_timestamp"]
    assert source["archive_member_name"] == _member_name(timestamp)
    assert source["semantic_class"] == "quantitative_precipitation_accumulation"
    assert source["accumulation_minutes"] == 5.0
    assert source["unit"] == "mm"
    assert source["forecast_lead_minutes"] == 0
    assert source["quantitative_semantics_eligible"] is True

    capture = store.load_capture(raw["raw_capture_id"])
    assert capture is not None
    assert capture.product_timestamp == timestamp.timestamp()
    assert capture.capture_receipt_sha256 == raw["raw_capture_receipt_sha256"]
    assert [member.role for member in capture.members] == ["rv_tar"]
    assert capture.members[0].receipt_id == source["raw_receipt_id"]

    assert decoded["quantitative_semantics_eligible"] is True
    assert decoded["operational_admission_eligible"] is False
    assert decoded["decoded_quantitative_semantics_eligible"] is False
    assert decoded["legacy_decoder_quality_constant"] == 0.98
    assert decoded["legacy_decoder_quality_operational"] is False
    assert decoded["observations"]
    observation = decoded["observations"][0]
    assert observation["quality"] == 0.98
    assert observation["quality_semantics"] == (
        "legacy_constant_not_provider_quality"
    )
    assert observation["quantitative_semantics_eligible"] is False
    assert observation["operational_admission_eligible"] is False
    assert decoded["overlay"]["operational_admission_eligible"] is False
    assert metadata["raw_provenance"] == raw
    assert metadata["observation_count"] == len(decoded["observations"])

    persisted_provenance = json.dumps(raw, sort_keys=True).lower()
    assert "http" not in persisted_provenance
    assert "artifact_path" not in persisted_provenance
    assert "payload" not in persisted_provenance
    assert "\\" not in persisted_provenance
    assert set(raw["raw_sources"]) == {"rv_tar"}
    assert store.integrity_check()["healthy"] is True


@pytest.mark.parametrize(
    ("archive_factory", "error_type"),
    (
        (lambda timestamp: b"not-a-tar", tarfile.ReadError),
        (
            lambda timestamp: _archive(timestamp, member_payload=b"not-hdf5"),
            OSError,
        ),
    ),
)
def test_dwd_rv_parse_failure_keeps_receipt_only(
    monkeypatch, tmp_path, archive_factory, error_type,
):
    timestamp = _timestamp()
    archive = archive_factory(timestamp)
    store = RawArtifactStore(tmp_path / "raw")
    _install_responses(monkeypatch, [_response(archive, timestamp.timestamp())])
    artifacts = _capture_artifacts(monkeypatch)

    with pytest.raises(error_type):
        radar.dwd_radolan_fetch(_job(), raw_store=store)

    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0
    assert _only_raw_object(tmp_path / "raw") == archive
    assert artifacts == []


def test_dwd_rv_oversize_member_keeps_tar_receipt_only(
    monkeypatch, tmp_path,
):
    timestamp = _timestamp()
    hdf = _hdf_bytes(timestamp)
    archive = _archive(timestamp, member_payload=hdf)
    store = RawArtifactStore(tmp_path / "raw")
    _install_responses(monkeypatch, [_response(archive, timestamp.timestamp())])
    artifacts = _capture_artifacts(monkeypatch)
    monkeypatch.setattr(radar, "MAX_ODIM_BYTES", len(hdf) - 1)

    with pytest.raises(ValueError, match="productlimiet"):
        radar.dwd_radolan_fetch(_job(), raw_store=store)

    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0
    assert _only_raw_object(tmp_path / "raw") == archive
    assert artifacts == []


def test_dwd_rv_capacity_rejection_stops_before_tar_decode(
    monkeypatch, tmp_path,
):
    timestamp = _timestamp()
    archive = _archive(timestamp)
    store = RawArtifactStore(
        tmp_path / "raw",
        limits=RawArtifactLimits(
            max_object_bytes=len(archive) - 1,
            max_total_bytes=len(archive) * 2,
        ),
    )
    _install_responses(monkeypatch, [_response(archive, timestamp.timestamp())])
    artifacts = _capture_artifacts(monkeypatch)
    monkeypatch.setattr(
        radar.tarfile,
        "open",
        lambda *_args, **_kwargs: pytest.fail("tar-decode werd uitgevoerd"),
    )

    with pytest.raises(RawArtifactCapacityError):
        radar.dwd_radolan_fetch(_job(), raw_store=store)

    assert store.stats()["object_count"] == 0
    assert store.stats()["receipt_count"] == 0
    assert store.stats()["capture_count"] == 0
    assert artifacts == []


def test_dwd_rv_same_job_retry_is_stable_and_next_job_is_new(
    monkeypatch, tmp_path,
):
    timestamp = _timestamp()
    archive = _archive(timestamp)
    store = RawArtifactStore(tmp_path / "raw")
    _install_responses(monkeypatch, [
        _response(archive, timestamp.timestamp() + 10),
        _response(archive, timestamp.timestamp() + 20),
        _response(archive, timestamp.timestamp() + 30),
    ])
    artifacts = _capture_artifacts(monkeypatch)

    first = radar.dwd_radolan_fetch(_job("dwd-slot-001"), raw_store=store)
    retry = radar.dwd_radolan_fetch(_job("dwd-slot-001"), raw_store=store)
    next_slot = radar.dwd_radolan_fetch(_job("dwd-slot-002"), raw_store=store)
    first_raw, retry_raw, next_raw = (
        value["raw_provenance"] for value in artifacts
    )

    assert retry_raw == first_raw
    assert retry["artifact_sha256"] == first["artifact_sha256"]
    assert retry_raw["raw_sources"]["rv_tar"]["received_at"] == (
        timestamp.timestamp() + 10
    )
    assert next_raw["raw_capture_id"] != first_raw["raw_capture_id"]
    assert next_raw["raw_sources"]["rv_tar"]["raw_receipt_id"] != (
        first_raw["raw_sources"]["rv_tar"]["raw_receipt_id"]
    )
    assert next_raw["raw_sources"]["rv_tar"]["raw_content_sha256"] == (
        first_raw["raw_sources"]["rv_tar"]["raw_content_sha256"]
    )
    assert next_slot["artifact_sha256"] != first["artifact_sha256"]
    assert store.stats()["object_count"] == 1
    assert store.stats()["receipt_count"] == 2
    assert store.stats()["capture_count"] == 2


def test_dwd_rv_same_job_content_and_provenance_conflicts_fail_closed(
    monkeypatch, tmp_path,
):
    timestamp = _timestamp()
    first_archive = _archive(timestamp)
    changed_archive = _archive(
        timestamp, member_payload=_hdf_bytes(timestamp, wet=False)
    )
    store = RawArtifactStore(tmp_path / "raw")
    _install_responses(monkeypatch, [
        _response(first_archive, timestamp.timestamp() + 10),
        _response(changed_archive, timestamp.timestamp() + 20),
        _response(first_archive, timestamp.timestamp() + 30),
        _response(first_archive, timestamp.timestamp() + 40),
    ])
    _capture_artifacts(monkeypatch)

    radar.dwd_radolan_fetch(_job("dwd-content"), raw_store=store)
    with pytest.raises(RawArtifactConflictError):
        radar.dwd_radolan_fetch(_job("dwd-content"), raw_store=store)

    radar.dwd_radolan_fetch(_job("dwd-provenance"), raw_store=store)
    original = radar._dwd_rv_native_provenance

    def changed_provenance():
        value = original()
        value["parser_version"] = "different-parser-contract"
        return value

    monkeypatch.setattr(radar, "_dwd_rv_native_provenance", changed_provenance)
    with pytest.raises(RawArtifactConflictError):
        radar.dwd_radolan_fetch(_job("dwd-provenance"), raw_store=store)

    assert store.stats()["object_count"] == 1
    assert store.stats()["receipt_count"] == 2
    assert store.stats()["capture_count"] == 2


@pytest.mark.parametrize(
    ("checkpoint", "receipt_count_before_retry"),
    (("store_after_object_publish", 0), ("capture_before_commit", 1)),
)
def test_dwd_rv_crash_retry_recovers_without_false_capture(
    monkeypatch, tmp_path, checkpoint, receipt_count_before_retry,
):
    timestamp = _timestamp()
    archive = _archive(timestamp)
    fired = False

    def fault(name: str):
        nonlocal fired
        if name == checkpoint and not fired:
            fired = True
            raise RuntimeError(f"simulated crash at {checkpoint}")

    store = RawArtifactStore(tmp_path / "raw", fault_injector=fault)
    _install_responses(monkeypatch, [
        _response(archive, timestamp.timestamp() + 10),
        _response(archive, timestamp.timestamp() + 20),
    ])
    artifacts = _capture_artifacts(monkeypatch)

    with pytest.raises(RuntimeError, match="simulated crash"):
        radar.dwd_radolan_fetch(_job(), raw_store=store)

    before = store.stats()
    assert before["receipt_count"] == receipt_count_before_retry
    assert before["capture_count"] == 0
    assert artifacts == []
    if checkpoint == "store_after_object_publish":
        assert before["unmanaged_file_count"] == 1
        assert _only_raw_object(tmp_path / "raw") == archive

    radar.dwd_radolan_fetch(_job(), raw_store=store)
    after = store.stats()
    assert after["object_count"] == 1
    assert after["receipt_count"] == 1
    assert after["capture_count"] == 1
    source = artifacts[0]["raw_provenance"]["raw_sources"]["rv_tar"]
    assert store.read_bytes(source["raw_receipt_id"]) == archive
    assert store.integrity_check()["healthy"] is True


def test_dwd_rv_filename_and_hdf_product_time_mismatch_is_receipt_only(
    monkeypatch, tmp_path,
):
    timestamp = _timestamp()
    archive = _archive(
        timestamp,
        filename_timestamp=timestamp + timedelta(minutes=5),
    )
    store = RawArtifactStore(tmp_path / "raw")
    _install_responses(monkeypatch, [_response(archive, timestamp.timestamp())])
    artifacts = _capture_artifacts(monkeypatch)

    with pytest.raises(ValueError, match="producttijd verschilt"):
        radar.dwd_radolan_fetch(_job(), raw_store=store)

    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0
    assert artifacts == []
