from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import threading
from types import SimpleNamespace

import h5py
import numpy as np
import pytest
from pyproj import Transformer

from storm_tracker_worker.canonical_evidence_store import CanonicalEvidenceStore
from storm_tracker_worker.desired_state import parse_desired_state
from storm_tracker_worker.dwd_rv_analysis_grid_certificate import (
    SOURCE_NORTH_EDGE_M,
    SOURCE_WEST_EDGE_M,
)
from storm_tracker_worker import dwd_rv_native
from storm_tracker_worker import worker_only_radar_processing
from storm_tracker_worker.dwd_rv_quantitative import (
    DwdRvArtifactCapacityError,
    DwdRvArtifactError,
    DwdRvCaptureMissingError,
    DwdRvDecodeDeadlineError,
    DwdRvDerivedArtifactStore,
    decode_dwd_rv_capture,
)
from storm_tracker_worker.native_capture import (
    NativeCaptureMemberInput,
    capture_native_set,
)
from storm_tracker_worker.provider_capture_registry import capture_policy
from storm_tracker_worker.raw_artifacts import (
    RawArtifactIntegrityError,
    RawArtifactStore,
)
from storm_tracker_worker.worker_only_radar_processing import (
    decode_worker_only_positive_observations,
)


PRODUCT_TIME = datetime(2026, 8, 1, 23, 5, tzinfo=timezone.utc)
RECEIVED_AT = PRODUCT_TIME.timestamp() + 43.25
EVALUATED_AT = RECEIVED_AT + 10.0
PROJDEF = (
    "+proj=stere +lat_ts=60 +lat_0=90 +lon_0=10 "
    "+x_0=543196.83521776402 +y_0=3622588.8619310022 "
    "+units=m +a=6378137 +b=6356752.3142451802 +no_defs"
)
CORNERS = {
    "LL_lat": 45.696425377390064,
    "LL_lon": 3.5669946350078914,
    "LR_lat": 45.68460578137082,
    "LR_lon": 16.580869348598274,
    "UL_lat": 55.862087108249824,
    "UL_lon": 1.463301510256666,
    "UR_lat": 55.845438563255755,
    "UR_lon": 18.73161645466747,
}


def _identity(prefix: str, value: str) -> str:
    return prefix + hashlib.sha256(value.encode("ascii")).hexdigest()


def _hdf_bytes(**overrides) -> bytes:
    timestamp = overrides.pop("timestamp", PRODUCT_TIME)
    start_delta = overrides.pop("start_delta", timedelta(minutes=5))
    quantity = overrides.pop("quantity", "ACRR")
    gain = overrides.pop("gain", 0.0009999999317806213)
    offset = overrides.pop("offset", -0.0009999999317806213)
    nodata = overrides.pop("nodata", float(2**32 - 1))
    undetect = overrides.pop("undetect", 0.0)
    conventions = overrides.pop("conventions", "ODIM_H5/V2_3")
    object_value = overrides.pop("object_value", "COMP")
    version = overrides.pop("version", "H5rad 2.3")
    product = overrides.pop("product", "MAX")
    prodname = overrides.pop("prodname", "RV_top_view")
    projdef = overrides.pop("projdef", PROJDEF)
    xsize = overrides.pop("xsize", 1100)
    ysize = overrides.pop("ysize", 1200)
    corner_shift = overrides.pop("corner_shift", 0.0)
    simulated = overrides.pop("simulated", "False")
    extra_dataset = overrides.pop("extra_dataset", False)
    if overrides:
        raise AssertionError(f"unused fixture overrides: {sorted(overrides)}")

    values = np.zeros((1200, 1100), dtype=np.uint32)
    values[0, 0] = np.uint32(2**32 - 1)
    values[10, 10] = 1
    values[10, 11] = 2
    values[10, 12] = 1001
    output = io.BytesIO()
    with h5py.File(output, "w") as root:
        root.attrs["Conventions"] = np.bytes_(conventions.encode("ascii"))
        top_what = root.create_group("what")
        top_what.attrs["object"] = np.bytes_(object_value.encode("ascii"))
        top_what.attrs["version"] = np.bytes_(version.encode("ascii"))
        top_what.attrs["date"] = np.bytes_(timestamp.strftime("%Y%m%d").encode())
        top_what.attrs["time"] = np.bytes_(timestamp.strftime("%H%M%S").encode())
        top_what.attrs["source"] = np.bytes_(
            b"ORG:78,CTY:616,CMT:Deutscher Wetterdienst test"
        )
        where = root.create_group("where")
        where.attrs["projdef"] = np.bytes_(projdef.encode("ascii"))
        where.attrs["xsize"] = xsize
        where.attrs["ysize"] = ysize
        where.attrs["xscale"] = 1000.0
        where.attrs["yscale"] = 1000.0
        for key, value in CORNERS.items():
            where.attrs[key] = value + (corner_shift if key == "UR_lon" else 0.0)
        how = root.create_group("how")
        how.attrs["simulated"] = np.bytes_(simulated.encode("ascii"))
        dataset = root.create_group("dataset1")
        frame_what = dataset.create_group("what")
        start = timestamp - start_delta
        frame_what.attrs["startdate"] = np.bytes_(start.strftime("%Y%m%d").encode())
        frame_what.attrs["starttime"] = np.bytes_(start.strftime("%H%M%S").encode())
        frame_what.attrs["enddate"] = np.bytes_(timestamp.strftime("%Y%m%d").encode())
        frame_what.attrs["endtime"] = np.bytes_(timestamp.strftime("%H%M%S").encode())
        frame_what.attrs["product"] = np.bytes_(product.encode("ascii"))
        frame_what.attrs["prodname"] = np.bytes_(prodname.encode("ascii"))
        data1 = dataset.create_group("data1")
        data1.create_dataset(
            "data", data=values, chunks=values.shape,
            compression="gzip", compression_opts=1,
        )
        data_what = data1.create_group("what")
        data_what.attrs["quantity"] = np.bytes_(quantity.encode("ascii"))
        data_what.attrs["gain"] = gain
        data_what.attrs["offset"] = offset
        data_what.attrs["nodata"] = nodata
        data_what.attrs["undetect"] = undetect
        if extra_dataset:
            root.create_group("dataset2")
    return output.getvalue()


def _tar_bytes(
    hdf: bytes,
    *,
    filename_time: datetime = PRODUCT_TIME,
    duplicate_plus_zero: bool = False,
    unsafe_member: bool = False,
) -> tuple[bytes, str]:
    member_name = f"composite_rv_{filename_time:%Y%m%d_%H%M}_000-hd5"
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w") as archive:
        members = [member_name]
        if duplicate_plus_zero:
            members.append(member_name)
        members.append(f"composite_rv_{filename_time:%Y%m%d_%H%M}_005-hd5")
        if unsafe_member:
            members.append("../composite_rv_20260801_2305_010-hd5")
        for name in members:
            info = tarfile.TarInfo(name)
            info.size = len(hdf)
            archive.addfile(info, io.BytesIO(hdf))
    return output.getvalue(), member_name


def _capture(
    tmp_path: Path,
    *,
    hdf: bytes | None = None,
    archive: bytes | None = None,
    filename_time: datetime = PRODUCT_TIME,
    extra_role: bool = False,
):
    hdf_payload = hdf if hdf is not None else _hdf_bytes()
    if archive is None:
        archive_payload, member_name = _tar_bytes(
            hdf_payload, filename_time=filename_time
        )
    else:
        archive_payload = archive
        member_name = f"composite_rv_{filename_time:%Y%m%d_%H%M}_000-hd5"
    digest = hashlib.sha256(archive_payload).hexdigest()
    raw_store = RawArtifactStore((tmp_path / "raw").resolve())
    raw_store.initialize()
    members = [
        NativeCaptureMemberInput(
            role="rv_tar",
            product="dwd_open_data_composite_rv_tar",
            content_type="application/octet-stream",
            received_at=RECEIVED_AT,
            upstream_id=(
                "dwd_open_data_composite_rv_tar|lead_minutes=0"
                f"|content_sha256={digest}"
            ),
            payload=archive_payload,
            provenance={
                "declared_accumulation_minutes": 5.0,
                "declared_quantity": "precipitation_accumulation",
                "declared_semantic_class": "quantitative_precipitation_accumulation",
                "declared_unit": "mm",
                "parser_version": "test-fixture-does-not-claim-certification",
                "product": "dwd_open_data_composite_rv_tar",
                "quantitative_semantics_verified_after_decode": False,
                "requested_forecast_lead_minutes": 0,
            },
        )
    ]
    if extra_role:
        members.append(NativeCaptureMemberInput(
            role="unexpected",
            product="dwd_open_data_other",
            content_type="application/octet-stream",
            received_at=RECEIVED_AT,
            upstream_id="dwd-unexpected-test-member",
            payload=b"unexpected",
            provenance={"semantic_class": "unknown"},
        ))
    result = capture_native_set(
        raw_store,
        provider="dwd_radolan",
        capture_product="dwd_open_data_composite_rv_capture",
        fetch_id="dwd-rv-quantitative-fixture",
        upstream_capture_id=(
            "dwd_open_data_composite_rv_capture"
            f"|member={member_name}"
            f"|product_timestamp={PRODUCT_TIME.timestamp():.6f}"
            f"|content_sha256={digest}"
        ),
        product_timestamp=PRODUCT_TIME.timestamp(),
        collector="dwd_open_data_composite_rv",
        collector_version="worker-dwd-rv-exact-raw-v1",
        members=members,
    )
    return raw_store, result.capture


def _stores(tmp_path: Path, raw_store: RawArtifactStore):
    artifacts = DwdRvDerivedArtifactStore((tmp_path / "derived").resolve())
    evidence = CanonicalEvidenceStore(
        (tmp_path / "canonical.sqlite3").resolve(),
        raw_artifact_reader=raw_store,
        clock=lambda: EVALUATED_AT + 1.0,
    )
    return artifacts, evidence


def _decode(tmp_path: Path, raw_store, capture, suffix="a"):
    artifacts, evidence = _stores(tmp_path, raw_store)
    result = decode_dwd_rv_capture(
        capture_id=capture.capture_id,
        decoder_job_id=_identity("decoder-job-", f"job-{suffix}"),
        attempt_id=_identity("decoder-attempt-", f"attempt-{suffix}"),
        raw_store=raw_store,
        artifact_store=artifacts,
        evidence_store=evidence,
        evaluated_at=EVALUATED_AT,
    )
    return result, artifacts, evidence


def test_verified_capture_yields_native_five_minute_raster_and_three_masks(tmp_path):
    raw_store, capture = _capture(tmp_path)

    result, artifacts, evidence = _decode(tmp_path, raw_store, capture)

    assert result.outcome == "decoded_complete"
    assert result.reason_code == "decoded_complete"
    assert result.artifact is not None
    assert result.evidence_receipt.is_structurally_complete_candidate is True
    assert (
        result.evidence_receipt.run.non_operational_candidate_artifact.sha256
        == result.artifact.sha256
    )
    raster = artifacts.read_decoded(result.artifact)
    assert raster.raw_values.shape == (1200, 1100)
    assert raster.raw_values.dtype == np.dtype("<u4")
    assert raster.accumulation_mm.dtype == np.dtype("<f8")
    assert raster.nodata_mask[0, 0] == 1
    assert raster.undetect_mask[0, 1] == 1
    assert raster.valid_mask[10, 10] == 1
    assert raster.valid_mask[10, 11] == 1
    assert raster.valid_mask[10, 12] == 1
    assert raster.accumulation_mm[10, 10] == 0.0
    assert raster.accumulation_mm[10, 12] == pytest.approx(1.0)
    assert raster.accumulation_mm[0, 0] == 0.0
    assert raster.metadata["product"]["accumulation_seconds"] == 300
    assert raster.metadata["product"]["quantity"] == "ACRR"
    assert raster.metadata["product"]["unit"] == "mm"
    assert raster.metadata["grid"]["axis_order"] == "yx"
    assert raster.metadata["grid"]["coverage"] == {
        "kind": "native_bbox",
        "maximum_x": 1099500.0,
        "maximum_y": 500.0,
        "minimum_x": -500.0,
        "minimum_y": -1199500.0,
    }
    assert evidence.audit()["healthy"] is True

    encoded_header = json.dumps(raster.metadata, sort_keys=True).lower()
    assert '"quality"' not in encoded_header
    assert '"cells"' not in encoded_header
    assert "0.98" not in encoded_header
    assert "mm/h" not in encoded_header
    assert capture_policy("dwd_radolan_fetch").vnext_operational_admission is False


def test_worker_only_processor_samples_certified_positive_dwd_target(tmp_path):
    raw_store, capture = _capture(tmp_path)
    transformer = Transformer.from_crs(PROJDEF, "EPSG:4326", always_xy=True)
    longitude, latitude = transformer.transform(
        SOURCE_WEST_EDGE_M + 12.5 * 1000.0,
        SOURCE_NORTH_EDGE_M - 10.5 * 1000.0,
    )
    desired = parse_desired_state({
        "authority_epoch": 1,
        "contract": "stv3-worker-desired-state-v1",
        "generation": 1,
        "issued_at": PRODUCT_TIME.timestamp() - 30.0,
        "previous_state_sha256": None,
        "producer_id": "ha:dwd-worker-only-test",
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": True,
            "region_sharing_distance_km": 100.0,
        },
        "targets": [{
            "available": True,
            "country_code": "DE",
            "display_name": "DWD testpixel",
            "kind": "test",
            "latitude": latitude,
            "location_accuracy_m": 1.0,
            "longitude": longitude,
            "observation_radius_km": 250.0,
            "position_observed_at": PRODUCT_TIME.timestamp() - 10.0,
            "target_id": "dwd-positive-pixel",
        }],
    })

    observations = decode_worker_only_positive_observations(
        workload_id="radar.dwd.radolan_rv",
        desired_state=desired,
        capture_id=capture.capture_id,
        raw_store=raw_store,
        artifact_root=(tmp_path / "worker-only-derived").resolve(),
        evidence_path=(tmp_path / "worker-only-evidence.sqlite3").resolve(),
        evaluated_at=EVALUATED_AT,
        clock=lambda: EVALUATED_AT + 1.0,
    )

    assert len(observations) == 1
    observation = observations[0]
    assert observation.source_id == "radar.dwd.radolan_rv"
    assert observation.target_id == "dwd-positive-pixel"
    assert observation.rain_rate_mm_h == pytest.approx(12.0)
    assert observation.product_valid_at == PRODUCT_TIME.timestamp()
    assert observation.received_at == RECEIVED_AT


def test_worker_only_reuses_decoded_field_but_rebinds_life360_authority(
    tmp_path,
    monkeypatch,
):
    raw_store, capture = _capture(tmp_path)
    transformer = Transformer.from_crs(PROJDEF, "EPSG:4326", always_xy=True)
    longitude, latitude = transformer.transform(
        SOURCE_WEST_EDGE_M + 12.5 * 1000.0,
        SOURCE_NORTH_EDGE_M - 10.5 * 1000.0,
    )
    desired = parse_desired_state({
        "authority_epoch": 1,
        "contract": "stv3-worker-desired-state-v1",
        "generation": 1,
        "issued_at": PRODUCT_TIME.timestamp() - 30.0,
        "previous_state_sha256": None,
        "producer_id": "ha:dwd-worker-only-cache-test",
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": True,
            "region_sharing_distance_km": 100.0,
        },
        "targets": [{
            "available": True,
            "country_code": "DE",
            "display_name": "DWD testpixel",
            "kind": "test",
            "latitude": latitude,
            "location_accuracy_m": 1.0,
            "longitude": longitude,
            "observation_radius_km": 250.0,
            "position_observed_at": PRODUCT_TIME.timestamp() - 10.0,
            "target_id": "dwd-positive-pixel",
        }],
    })
    decode_calls = []
    original_decode = worker_only_radar_processing.decode_dwd_rv_capture

    def counted_decode(**kwargs):
        decode_calls.append(kwargs["capture_id"])
        return original_decode(**kwargs)

    monkeypatch.setattr(
        worker_only_radar_processing,
        "decode_dwd_rv_capture",
        counted_decode,
    )
    arguments = {
        "workload_id": "radar.dwd.radolan_rv",
        "capture_id": capture.capture_id,
        "raw_store": raw_store,
        "artifact_root": (tmp_path / "worker-only-derived").resolve(),
        "evidence_path": (tmp_path / "worker-only-evidence").resolve(),
        "evaluated_at": EVALUATED_AT,
        "clock": lambda: EVALUATED_AT + 1.0,
    }
    first = decode_worker_only_positive_observations(
        desired_state=desired,
        **arguments,
    )
    refreshed_mapping = desired.as_dict()
    refreshed_mapping["generation"] = 2
    refreshed_mapping["issued_at"] = PRODUCT_TIME.timestamp() - 5.0
    refreshed_mapping["previous_state_sha256"] = desired.state_sha256
    refreshed_mapping["targets"][0]["position_observed_at"] = (
        PRODUCT_TIME.timestamp() - 5.0
    )
    refreshed = parse_desired_state(refreshed_mapping)
    second = decode_worker_only_positive_observations(
        desired_state=refreshed,
        **arguments,
    )

    assert decode_calls == [capture.capture_id]
    assert first[0].rain_rate_mm_h == second[0].rain_rate_mm_h
    assert first[0].source_receipt_sha256 == second[0].source_receipt_sha256
    assert first[0].process_receipt_sha256 != second[0].process_receipt_sha256


def test_worker_only_processor_adapts_acquisition_raw_dwd_capture(
    tmp_path,
    monkeypatch,
):
    hdf = _hdf_bytes()
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w") as archive:
        for lead in range(0, 121, 5):
            name = f"composite_rv_{PRODUCT_TIME:%Y%m%d_%H%M}_{lead:03d}-hd5"
            info = tarfile.TarInfo(name)
            info.size = len(hdf)
            archive.addfile(info, io.BytesIO(hdf))
    source_store = RawArtifactStore((tmp_path / "source-raw").resolve())
    source_store.initialize()
    monkeypatch.setattr(
        dwd_rv_native,
        "_download",
        lambda: dwd_rv_native.DwdRvHttpResponse(
            output.getvalue(),
            "application/x-tar",
            RECEIVED_AT,
        ),
    )
    result = dwd_rv_native.capture_latest_dwd_rv_tar(
        SimpleNamespace(job_id="acquisition:" + "d" * 64, payload={}),
        raw_store=source_store,
    )
    transformer = Transformer.from_crs(PROJDEF, "EPSG:4326", always_xy=True)
    longitude, latitude = transformer.transform(
        SOURCE_WEST_EDGE_M + 12.5 * 1000.0,
        SOURCE_NORTH_EDGE_M - 10.5 * 1000.0,
    )
    desired = parse_desired_state({
        "authority_epoch": 1,
        "contract": "stv3-worker-desired-state-v1",
        "generation": 1,
        "issued_at": PRODUCT_TIME.timestamp() - 30.0,
        "previous_state_sha256": None,
        "producer_id": "ha:dwd-worker-only-handoff-test",
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": True,
            "region_sharing_distance_km": 100.0,
        },
        "targets": [{
            "available": True,
            "country_code": "DE",
            "display_name": "DWD testpixel",
            "kind": "test",
            "latitude": latitude,
            "location_accuracy_m": 1.0,
            "longitude": longitude,
            "observation_radius_km": 250.0,
            "position_observed_at": PRODUCT_TIME.timestamp() - 10.0,
            "target_id": "dwd-positive-pixel",
        }],
    })

    observations = decode_worker_only_positive_observations(
        workload_id="radar.dwd.radolan_rv",
        desired_state=desired,
        capture_id=str(result["raw_provenance"]["raw_capture_id"]),
        raw_store=source_store,
        artifact_root=(tmp_path / "worker-only-derived").resolve(),
        evidence_path=(tmp_path / "worker-only-evidence").resolve(),
        handoff_root=(tmp_path / "handoff-raw").resolve(),
        evaluated_at=EVALUATED_AT,
        clock=lambda: EVALUATED_AT + 1.0,
    )

    assert len(observations) == 1
    assert observations[0].rain_rate_mm_h == pytest.approx(12.0)
    handoff_store = RawArtifactStore((tmp_path / "handoff-raw").resolve())
    handoff_store.initialize()
    assert handoff_store.stats()["capture_count"] == 1


def test_same_capture_produces_identical_artifact_across_retries(tmp_path):
    raw_store, capture = _capture(tmp_path)
    artifacts, evidence = _stores(tmp_path, raw_store)

    first = decode_dwd_rv_capture(
        capture_id=capture.capture_id,
        decoder_job_id=_identity("decoder-job-", "stable-job"),
        attempt_id=_identity("decoder-attempt-", "stable-attempt-a"),
        raw_store=raw_store,
        artifact_store=artifacts,
        evidence_store=evidence,
        evaluated_at=EVALUATED_AT,
    )
    second = decode_dwd_rv_capture(
        capture_id=capture.capture_id,
        decoder_job_id=_identity("decoder-job-", "stable-job"),
        attempt_id=_identity("decoder-attempt-", "stable-attempt-b"),
        raw_store=raw_store,
        artifact_store=artifacts,
        evidence_store=evidence,
        evaluated_at=EVALUATED_AT,
    )

    assert first.artifact == second.artifact
    assert artifacts.object_count() == 1
    assert first.evidence_receipt.receipt_sha256 != second.evidence_receipt.receipt_sha256


def test_decode_reads_raw_member_once_plus_independent_evidence_verification(
    tmp_path,
    monkeypatch,
):
    raw_store, capture = _capture(tmp_path)
    original = raw_store._read_verified
    passes: list[bool] = []

    def counted(*args, **kwargs):
        passes.append(bool(kwargs.get("return_bytes")))
        return original(*args, **kwargs)

    monkeypatch.setattr(raw_store, "_read_verified", counted)

    result, _artifacts, _evidence = _decode(
        tmp_path,
        raw_store,
        capture,
        suffix="bounded-raw-reads",
    )

    assert result.outcome == "decoded_complete"
    assert passes == [True, False]


@pytest.mark.parametrize(
    ("hdf_overrides", "reason"),
    [
        ({"quantity": "RATE"}, "unsupported:rv_odim_profile"),
        ({"gain": 0.01, "offset": -0.01}, "unsupported:value_encoding"),
        ({"offset": 0.0}, "unsupported:value_encoding"),
        ({"nodata": 65535.0}, "unsupported:value_encoding"),
        ({"undetect": 1.0}, "unsupported:value_encoding"),
        ({"start_delta": timedelta(minutes=10)}, "unsupported:time_contract"),
        ({"object_value": "IMAGE"}, "unsupported:rv_odim_profile"),
        ({"product": "RR"}, "unsupported:rv_odim_profile"),
        ({"projdef": PROJDEF.replace("+lon_0=10", "+lon_0=11")}, "unsupported:crs_profile"),
        ({"corner_shift": 0.1}, "unsupported:grid_corners"),
        ({"simulated": "True"}, "unsupported:simulated_product"),
        ({"extra_dataset": True}, "unsupported:hdf5_multiplicity"),
    ],
)
def test_semantic_or_grid_drift_is_non_meteorological(
    tmp_path, hdf_overrides, reason,
):
    raw_store, capture = _capture(tmp_path, hdf=_hdf_bytes(**hdf_overrides))

    result, artifacts, _evidence = _decode(tmp_path, raw_store, capture)

    assert result.outcome == "unsupported"
    assert result.reason_code == reason
    assert result.artifact is None
    assert result.evidence_receipt.is_structurally_complete_candidate is False
    assert result.evidence_receipt.run.non_operational_candidate_artifact is None
    assert result.evidence_receipt.run.quantity is None
    assert artifacts.object_count() == 0


@pytest.mark.parametrize("archive_kind", ["corrupt", "duplicate", "unsafe"])
def test_adversarial_archive_never_exposes_a_raster(tmp_path, archive_kind):
    hdf = _hdf_bytes()
    if archive_kind == "corrupt":
        archive = b"not a tar archive"
        expected_outcome = "decode_error"
    else:
        archive, _name = _tar_bytes(
            hdf,
            duplicate_plus_zero=archive_kind == "duplicate",
            unsafe_member=archive_kind == "unsafe",
        )
        expected_outcome = "unsupported"
    raw_store, capture = _capture(tmp_path, archive=archive)

    result, artifacts, _evidence = _decode(tmp_path, raw_store, capture)

    assert result.outcome == expected_outcome
    assert result.artifact is None
    assert result.evidence_receipt.run.non_operational_candidate_artifact is None
    assert artifacts.object_count() == 0


def test_wrong_capture_roles_commit_only_incomplete_receipt(tmp_path):
    raw_store, capture = _capture(tmp_path, extra_role=True)

    result, artifacts, _evidence = _decode(tmp_path, raw_store, capture)

    assert result.outcome == "incomplete"
    assert result.reason_code == "incomplete:capture_roles"
    assert result.artifact is None
    assert result.evidence_receipt.run.native_grid is None
    assert artifacts.object_count() == 0


def test_member_filename_hdf_and_capture_time_must_all_agree(tmp_path):
    filename_time = PRODUCT_TIME + timedelta(minutes=5)
    raw_store, capture = _capture(tmp_path, filename_time=filename_time)

    result, artifacts, _evidence = _decode(tmp_path, raw_store, capture)

    assert result.outcome == "unsupported"
    assert result.reason_code == "unsupported:time_contract"
    assert result.artifact is None
    assert artifacts.object_count() == 0


def test_raw_hash_tamper_stops_before_evidence_or_artifact(tmp_path):
    raw_store, capture = _capture(tmp_path)
    receipt = raw_store.load_receipt(capture.members[0].receipt_id)
    assert receipt is not None
    path = Path(receipt.artifact_path)
    original = path.read_bytes()
    path.write_bytes(bytes([original[0] ^ 0xFF]) + original[1:])
    artifacts, evidence = _stores(tmp_path, raw_store)

    with pytest.raises(RawArtifactIntegrityError):
        decode_dwd_rv_capture(
            capture_id=capture.capture_id,
            decoder_job_id=_identity("decoder-job-", "tamper-job"),
            attempt_id=_identity("decoder-attempt-", "tamper-attempt"),
            raw_store=raw_store,
            artifact_store=artifacts,
            evidence_store=evidence,
            evaluated_at=EVALUATED_AT,
        )

    assert artifacts.object_count() == 0
    assert evidence.stats()["decoder_runs"] == 0


def test_artifact_publish_failure_prevents_decoded_complete_commit(tmp_path):
    raw_store, capture = _capture(tmp_path)

    def fail(checkpoint: str) -> None:
        if checkpoint == "publish.after_stage":
            raise OSError("injected publication failure")

    artifacts = DwdRvDerivedArtifactStore(
        (tmp_path / "derived").resolve(), fault_injector=fail
    )
    evidence = CanonicalEvidenceStore(
        (tmp_path / "canonical.sqlite3").resolve(),
        raw_artifact_reader=raw_store,
        clock=lambda: EVALUATED_AT + 1.0,
    )

    with pytest.raises(OSError, match="injected"):
        decode_dwd_rv_capture(
            capture_id=capture.capture_id,
            decoder_job_id=_identity("decoder-job-", "publish-job"),
            attempt_id=_identity("decoder-attempt-", "publish-attempt"),
            raw_store=raw_store,
            artifact_store=artifacts,
            evidence_store=evidence,
            evaluated_at=EVALUATED_AT,
        )

    assert artifacts.object_count() == 0
    assert evidence.stats()["decoder_runs"] == 0


def test_artifact_content_tamper_is_detected(tmp_path):
    raw_store, capture = _capture(tmp_path)
    result, artifacts, _evidence = _decode(tmp_path, raw_store, capture)
    assert result.artifact is not None
    path = artifacts.objects_path / f"{result.artifact.sha256}.drvq"
    payload = path.read_bytes()
    path.write_bytes(payload[:-1] + bytes([payload[-1] ^ 1]))

    with pytest.raises(DwdRvArtifactError, match="checksum"):
        artifacts.read_decoded(result.artifact)


def test_derived_store_bounds_object_and_staging_entry_counts(tmp_path):
    object_store = DwdRvDerivedArtifactStore(
        (tmp_path / "objects-bounded").resolve(),
        max_objects=1,
    )
    first = object_store.publish(b"first")
    assert object_store.publish(b"first") == first
    with pytest.raises(DwdRvArtifactCapacityError, match="object count"):
        object_store.publish(b"second")

    staging_store = DwdRvDerivedArtifactStore(
        (tmp_path / "staging-bounded").resolve(),
        max_staging_objects=1,
    )
    orphan = staging_store.staging_path / f".{('0' * 64)}.orphan"
    orphan.write_bytes(b"orphan")
    with pytest.raises(DwdRvArtifactCapacityError, match="staging count"):
        staging_store.publish(b"new")


def test_derived_store_unknown_layout_entry_fails_closed(tmp_path):
    store = DwdRvDerivedArtifactStore((tmp_path / "derived").resolve())
    first = store.publish(b"first")
    (store.objects_path / "unbounded-junk").write_bytes(b"junk")
    with pytest.raises(DwdRvArtifactError, match="unknown object entry"):
        store.publish(b"new")
    with pytest.raises(DwdRvArtifactError, match="unknown object entry"):
        store.publish(b"first")
    assert first.size_bytes == len(b"first")


def test_derived_store_profile_binds_limits_to_the_root(tmp_path):
    root = (tmp_path / "profile-bound").resolve()
    store = DwdRvDerivedArtifactStore(
        root,
        max_objects=2,
        max_staging_objects=3,
    )
    profile = json.loads(store.profile_path.read_text(encoding="utf-8"))

    assert profile["schema"] == "stv3-worker-dwd-rv-derived-store-profile-v1"
    assert profile["filename_suffix"] == ".drvq"
    assert profile["limits"]["max_objects"] == 2
    assert profile["limits"]["max_staging_objects"] == 3
    assert DwdRvDerivedArtifactStore(
        root,
        max_objects=2,
        max_staging_objects=3,
    ).object_count() == 0
    with pytest.raises(DwdRvArtifactError, match="immutable limits"):
        DwdRvDerivedArtifactStore(
            root,
            max_objects=3,
            max_staging_objects=3,
        )


def test_derived_store_profile_creation_respects_existing_staging_bound(tmp_path):
    root = (tmp_path / "profile-staging-bound").resolve()
    (root / "objects").mkdir(parents=True)
    (root / "staging").mkdir()
    (root / "staging" / f".{('0' * 64)}.orphan").write_bytes(b"orphan")

    with pytest.raises(DwdRvArtifactCapacityError, match="profile publication"):
        DwdRvDerivedArtifactStore(root, max_staging_objects=1)

    assert not (root / ".store-profile.json").exists()
    assert len(list((root / "staging").iterdir())) == 1


@pytest.mark.parametrize(
    ("checkpoint", "profile_survives"),
    [("profile.after_stage", False), ("profile.after_link", True)],
)
def test_derived_store_profile_publication_is_retryable_at_crash_boundaries(
    tmp_path,
    checkpoint,
    profile_survives,
):
    root = (tmp_path / checkpoint.replace(".", "-")).resolve()

    def fail(stage: str) -> None:
        if stage == checkpoint:
            raise RuntimeError("injected profile publication interruption")

    with pytest.raises(RuntimeError, match="profile publication"):
        DwdRvDerivedArtifactStore(root, fault_injector=fail)

    profile_path = root / ".store-profile.json"
    assert profile_path.exists() is profile_survives
    assert list((root / "staging").iterdir()) == []
    reopened = DwdRvDerivedArtifactStore(root)
    assert reopened.object_count() == 0


def test_parallel_publication_cannot_oversubscribe_object_bound(tmp_path):
    root = (tmp_path / "parallel-derived").resolve()
    first_staged = threading.Event()
    release_first = threading.Event()
    second_staged = threading.Event()

    def first_fault(checkpoint: str) -> None:
        if checkpoint == "publish.after_stage":
            first_staged.set()
            if not release_first.wait(10):
                raise AssertionError("parallel publication release timed out")

    def second_fault(checkpoint: str) -> None:
        if checkpoint == "publish.after_stage":
            second_staged.set()

    first_store = DwdRvDerivedArtifactStore(
        root,
        max_objects=1,
        fault_injector=first_fault,
    )
    second_store = DwdRvDerivedArtifactStore(
        root,
        max_objects=1,
        fault_injector=second_fault,
    )
    outcomes: dict[str, str] = {}

    def publish(label: str, store, payload: bytes) -> None:
        try:
            store.publish(payload)
        except DwdRvArtifactCapacityError:
            outcomes[label] = "capacity"
        except Exception as exc:  # pragma: no cover - retained as evidence
            outcomes[label] = f"unexpected:{type(exc).__name__}:{exc}"
        else:
            outcomes[label] = "published"

    first = threading.Thread(
        target=publish,
        args=("first", first_store, b"first"),
    )
    second = threading.Thread(
        target=publish,
        args=("second", second_store, b"second"),
    )
    first.start()
    assert first_staged.wait(10)
    second.start()
    try:
        assert second_staged.wait(0.25) is False
    finally:
        release_first.set()
    first.join(10)
    second.join(10)

    assert first.is_alive() is False
    assert second.is_alive() is False
    assert outcomes == {"first": "published", "second": "capacity"}
    assert first_store.object_count() == 1
    assert list(first_store.staging_path.iterdir()) == []


def test_missing_capture_cannot_create_a_canonical_outcome(tmp_path):
    raw_store = RawArtifactStore((tmp_path / "raw").resolve())
    raw_store.initialize()
    artifacts, evidence = _stores(tmp_path, raw_store)

    with pytest.raises(DwdRvCaptureMissingError):
        decode_dwd_rv_capture(
            capture_id="capture-" + "0" * 64,
            decoder_job_id=_identity("decoder-job-", "missing-job"),
            attempt_id=_identity("decoder-attempt-", "missing-attempt"),
            raw_store=raw_store,
            artifact_store=artifacts,
            evidence_store=evidence,
            evaluated_at=EVALUATED_AT,
        )

    assert evidence.stats()["decoder_runs"] == 0
    assert artifacts.object_count() == 0


def test_expired_cooperative_deadline_creates_no_artifact_or_evidence(tmp_path):
    raw_store, capture = _capture(tmp_path)
    artifacts, evidence = _stores(tmp_path, raw_store)

    with pytest.raises(DwdRvDecodeDeadlineError, match="decode_start"):
        decode_dwd_rv_capture(
            capture_id=capture.capture_id,
            decoder_job_id=_identity("decoder-job-", "deadline-job"),
            attempt_id=_identity("decoder-attempt-", "deadline-attempt"),
            raw_store=raw_store,
            artifact_store=artifacts,
            evidence_store=evidence,
            evaluated_at=EVALUATED_AT,
            monotonic_deadline=10.0,
            monotonic_clock=lambda: 10.0,
        )

    assert artifacts.object_count() == 0
    assert evidence.stats()["decoder_runs"] == 0


def test_deadline_rechecked_after_single_raw_authority_pass(
    tmp_path,
    monkeypatch,
):
    raw_store, capture = _capture(tmp_path)
    artifacts, evidence = _stores(tmp_path, raw_store)
    original = raw_store._read_verified
    reads: list[bool] = []
    moments = iter((0.0, 0.0, 2.0))

    def counted(*args, **kwargs):
        reads.append(bool(kwargs.get("return_bytes")))
        return original(*args, **kwargs)

    monkeypatch.setattr(raw_store, "_read_verified", counted)
    with pytest.raises(DwdRvDecodeDeadlineError, match="raw_capture_loaded"):
        decode_dwd_rv_capture(
            capture_id=capture.capture_id,
            decoder_job_id=_identity("decoder-job-", "raw-deadline-job"),
            attempt_id=_identity("decoder-attempt-", "raw-deadline-attempt"),
            raw_store=raw_store,
            artifact_store=artifacts,
            evidence_store=evidence,
            evaluated_at=EVALUATED_AT,
            monotonic_deadline=1.0,
            monotonic_clock=lambda: next(moments),
        )

    assert reads == [True]
    assert artifacts.object_count() == 0
    assert evidence.stats()["decoder_runs"] == 0


def test_module_import_is_inert_and_does_not_activate_worker_paths(tmp_path):
    repo_root = Path(__file__).resolve().parents[1]
    script = f"""
import socket
import sqlite3
import sys
import threading
from pathlib import Path

repo_root = {str(repo_root)!r}
workdir = Path.cwd()
before_paths = sorted(str(path.relative_to(workdir)) for path in workdir.rglob('*'))
before_threads = {{(thread.name, thread.ident) for thread in threading.enumerate()}}

def forbidden(operation):
    def reject(*args, **kwargs):
        raise AssertionError(f'forbidden import side effect: {{operation}}')
    return reject

socket.create_connection = forbidden('network connection')
socket.socket.connect = forbidden('socket connection')
sqlite3.connect = forbidden('SQLite connection')
threading.Thread.start = forbidden('thread start')
sys.path.insert(0, repo_root)

import storm_tracker_worker.dwd_rv_quantitative as decoder

after_paths = sorted(str(path.relative_to(workdir)) for path in workdir.rglob('*'))
after_threads = {{(thread.name, thread.ident) for thread in threading.enumerate()}}
assert after_paths == before_paths
assert after_threads == before_threads
assert callable(decoder.decode_dwd_rv_capture)
forbidden_modules = {{
    'storm_tracker_worker.service',
    'storm_tracker_worker.radar',
    'storm_tracker_worker.provider_capture_registry',
}}
assert forbidden_modules.isdisjoint(sys.modules)
store_types = (
    decoder.DwdRvDerivedArtifactStore,
    decoder.CanonicalEvidenceStore,
    decoder.RawArtifactStore,
)
assert not any(
    isinstance(value, store_types)
    for value in vars(decoder).values()
    if not isinstance(value, type)
)
print('dwd-rv-import-inert')
"""
    env = os.environ.copy()
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    python_paths = [str(repo_root), str(repo_root / ".testdeps")]
    if env.get("PYTHONPATH"):
        python_paths.append(env["PYTHONPATH"])
    env["PYTHONPATH"] = os.pathsep.join(python_paths)

    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert completed.stdout.strip() == "dwd-rv-import-inert"
