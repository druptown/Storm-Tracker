from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import threading
import time
from types import SimpleNamespace

import h5py
import numpy as np
import pytest

from storm_tracker_worker.canonical_evidence_store import CanonicalEvidenceStore
from storm_tracker_worker.desired_state import parse_desired_state
from storm_tracker_worker import noaa_err_native as native
from storm_tracker_worker import noaa_err_quantitative as quantitative
from storm_tracker_worker.noaa_err_quantitative import (
    NoaaErrArtifactCapacityError,
    NoaaErrArtifactError,
    NoaaErrCaptureMissingError,
    NoaaErrDerivedArtifactStore,
    NoaaErrRegion,
    decode_noaa_err_region,
)
from storm_tracker_worker.raw_artifacts import (
    RawArtifactIntegrityError,
    RawArtifactStore,
)
from storm_tracker_worker.worker_only_radar_processing import (
    decode_worker_only_positive_observations,
)


PREFIX = "BLEND/RainRate-Blend-INST/2026/07/18/"
KEY = (
    PREFIX
    + "23/RRQPE-INST-GLB-5_v1r1_blend_"
    "s202607182350000_e202607182359599_c202607190008336.nc"
)
ETAG = '"6ff682687fc8b2e28749a45efab19099"'
LAST_MODIFIED = "2026-07-19T00:09:28.000Z"
HTTP_LAST_MODIFIED = "Sun, 19 Jul 2026 00:09:28 GMT"
PRODUCT_START = datetime(2026, 7, 18, 23, 50, tzinfo=timezone.utc)
PRODUCT_END = datetime(2026, 7, 18, 23, 59, tzinfo=timezone.utc)
CREATED = datetime(2026, 7, 19, 0, 8, 33, tzinfo=timezone.utc)
RECEIVED_AT = datetime(2026, 8, 2, 1, 0, tzinfo=timezone.utc).timestamp()
EVALUATED_AT = RECEIVED_AT + 60.0
REGION = NoaaErrRegion(
    minimum_latitude=49.96,
    minimum_longitude=3.96,
    maximum_latitude=50.06,
    maximum_longitude=4.06,
)
INPUT_FILES = (
    "RRQPE-ABI-INST_v1r1_g19_s202607182350190_e202607182359498_c202607190000118.nc "
    "RRQPE-ABI-INST_v1r1_g18_s202607182350232_e202607182359541_c202607190000216.nc "
    "RRQPE-AHI-INST_v1r1_h09_s202607182350206_e202607182359400_c202607190004343.nc "
    "RRQPE-SEVIRI-INST_v1r1_met9_s202607182345100_e202607182357398_c202607190006155.nc "
    "RRQPE-SEVIRI-INST_v1r1_met10_s202607182345108_e202607182357434_c202607190007369.nc"
)


def _identity(prefix: str, value: str) -> str:
    return prefix + hashlib.sha256(value.encode("ascii")).hexdigest()


def _hdf_bytes(
    *,
    dqf_meanings: str = (
        "bad_quality degraded_due_to_LZA_or_latitude_threshold_exceeded"
    ),
    rrqpe_level: int = 6,
    allocate_selected_chunks: bool = True,
    unknown_dqf: bool = False,
    negative_rrqpe: bool = False,
    inconsistent_missing: bool = False,
    nonzero_dimension_scale: bool = False,
    extra_root_object: bool = False,
    skip_rrqpe_deflate_for_selected_chunk: bool = False,
    latitude_resolution: float = 0.02,
    missing_inputs: int = 0,
    input_files: str = INPUT_FILES,
    reverse_coordinates: bool = False,
    omit_axis_bindings: bool = False,
    dimension_scale_dtype: str = ">f4",
    wide_reference_dimension: bool = False,
    dimension_id_dtype: str = "<i4",
    coordinate_dtype: str = "<i4",
) -> bytes:
    output = io.BytesIO()
    with h5py.File(output, "w") as root:
        root.attrs["_NCProperties"] = np.bytes_(
            b"version=2,netcdf=4.7.0,hdf5=1.8.22,"
        )
        root.attrs["Conventions"] = np.bytes_(b"CF-1.6")
        root.attrs["title"] = np.bytes_(b"RRQPE_INST_GLB")
        root.attrs["production_environment"] = np.bytes_(b"prod")
        root.attrs["processing_level"] = np.bytes_(b"NOAA Level 3")
        root.attrs["platform"] = np.bytes_(
            b"GOES-19, GOES-18, Himawari-9, MSG2, MSG3"
        )
        root.attrs["time_coverage_start"] = np.bytes_(
            b"2026-07-18T23:50:00Z"
        )
        root.attrs["time_coverage_end"] = np.bytes_(
            b"2026-07-18T23:59:00Z"
        )
        root.attrs["date_created"] = np.bytes_(b"2026-07-19T00:08:33Z")
        root.attrs["geospatial_lat_min"] = np.asarray([-60.0], dtype="f4")
        root.attrs["geospatial_lat_max"] = np.asarray([70.0], dtype="f4")
        root.attrs["geospatial_lon_min"] = np.asarray([-180.0], dtype="f4")
        root.attrs["geospatial_lon_max"] = np.asarray([179.98], dtype="f4")
        root.attrs["geospatial_lat_resolution"] = np.asarray(
            [latitude_resolution], dtype="f4"
        )
        root.attrs["geospatial_lon_resolution"] = np.asarray([0.02], dtype="f4")
        rows = np.zeros(native.NOAA_ERR_GRID_ROWS, dtype=dimension_scale_dtype)
        columns = np.zeros(native.NOAA_ERR_GRID_COLUMNS, dtype=dimension_scale_dtype)
        if nonzero_dimension_scale:
            rows[0] = 70.0
        columns_dataset = root.create_dataset("Columns", data=columns)
        rows_dataset = root.create_dataset("Rows", data=rows)
        if not omit_axis_bindings:
            columns_dataset.make_scale(
                "This is a netCDF dimension but not a netCDF variable.     18000"
            )
            rows_dataset.make_scale(
                "This is a netCDF dimension but not a netCDF variable.      6501"
            )
            columns_dataset.attrs["_Netcdf4Dimid"] = np.asarray(
                0, dtype=dimension_id_dtype
            )
            rows_dataset.attrs["_Netcdf4Dimid"] = np.asarray(
                1, dtype=dimension_id_dtype
            )
        rrqpe = root.create_dataset(
            "RRQPE",
            shape=(native.NOAA_ERR_GRID_ROWS, native.NOAA_ERR_GRID_COLUMNS),
            dtype="i2",
            chunks=(813, 2250),
            compression="gzip",
            compression_opts=rrqpe_level,
            fillvalue=-9990,
        )
        rrqpe.attrs["_FillValue"] = np.asarray([-9990], dtype="i2")
        rrqpe.attrs["scale_factor"] = np.asarray([0.1], dtype="f4")
        rrqpe.attrs["add_offset"] = np.asarray([0.0], dtype="f4")
        rrqpe.attrs["units"] = np.bytes_(b"mm/h")
        dqf = root.create_dataset(
            "DQF",
            shape=(native.NOAA_ERR_GRID_ROWS, native.NOAA_ERR_GRID_COLUMNS),
            dtype="i1",
            chunks=(1084, 3000),
            compression="gzip",
            compression_opts=9,
            fillvalue=-1,
        )
        dqf.attrs["_FillValue"] = np.asarray([-1], dtype="i1")
        dqf.attrs["valid_range"] = np.asarray([0, 3], dtype="i1")
        dqf.attrs["flag_masks"] = np.asarray([1, 2], dtype="i1")
        dqf.attrs["flag_meanings"] = np.bytes_(dqf_meanings.encode())
        if not omit_axis_bindings:
            coordinates = [0, 1] if reverse_coordinates else [1, 0]
            rrqpe.attrs["_Netcdf4Coordinates"] = np.asarray(
                coordinates, dtype=coordinate_dtype
            )
            dqf.attrs["_Netcdf4Coordinates"] = np.asarray(
                coordinates, dtype=coordinate_dtype
            )
            rrqpe.dims[0].attach_scale(rows_dataset)
            rrqpe.dims[1].attach_scale(columns_dataset)
            dqf.dims[0].attach_scale(rows_dataset)
            dqf.dims[1].attach_scale(columns_dataset)
            reference_dtype = np.dtype({
                "names": ["dataset", "dimension"],
                "formats": [
                    h5py.ref_dtype,
                    "<i8" if wide_reference_dimension else "<i4",
                ],
                "offsets": [0, 8],
                "itemsize": 16,
            })
            for scale in (rows_dataset, columns_dataset):
                original = np.asarray(scale.attrs["REFERENCE_LIST"])
                replacement = np.empty(original.shape, dtype=reference_dtype)
                replacement["dataset"] = original["dataset"]
                replacement["dimension"] = original["dimension"]
                del scale.attrs["REFERENCE_LIST"]
                scale.attrs.create("REFERENCE_LIST", replacement)
        monitoring = root.create_dataset(
            "MonitoringMetaData", shape=(), dtype="i1"
        )
        monitoring.attrs["Number_of_Input_Files"] = np.asarray([5], dtype="i4")
        monitoring.attrs["Number_of_Missing_Inputs"] = np.asarray(
            [missing_inputs], dtype="i4"
        )
        monitoring.attrs["Input_Files"] = np.bytes_(input_files.encode("ascii"))
        root.create_dataset("quality_information", shape=(), dtype="S1")
        if extra_root_object:
            root.create_dataset("unexpected", data=np.asarray([1], dtype="i1"))
        if allocate_selected_chunks:
            rate = np.zeros((16, 16), dtype="i2")
            quality = np.zeros((16, 16), dtype="i1")
            rate[6, 6] = 1
            rate[6, 7] = 28
            rate[6, 8] = 100
            quality[6, 6] = 0
            quality[6, 7] = 1
            quality[6, 8] = 2
            quality[6, 9] = 3
            rate[7, 7] = -9990
            quality[7, 7] = -1
            if unknown_dqf:
                quality[6, 7] = 4
            if negative_rrqpe:
                rate[6, 7] = -2
            if inconsistent_missing:
                quality[7, 7] = 0
            if skip_rrqpe_deflate_for_selected_chunk:
                unfiltered = np.zeros((813, 2250), dtype="i2")
                unfiltered[179:195, 192:208] = rate
                rrqpe.id.write_direct_chunk(
                    (813, 9000), unfiltered.tobytes(order="C"), filter_mask=1
                )
            else:
                rrqpe[992:1008, 9192:9208] = rate
            dqf[992:1008, 9192:9208] = quality
    return output.getvalue()


def _listing_xml(size: int) -> bytes:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f'<ListBucketResult xmlns="{native.NOAA_ERR_XML_NAMESPACE}">'
        f"<Name>{native.NOAA_ERR_BUCKET_NAME}</Name>"
        f"<Prefix>{PREFIX}</Prefix>"
        "<KeyCount>1</KeyCount>"
        f"<MaxKeys>{native.NOAA_ERR_LIST_MAX_KEYS}</MaxKeys>"
        "<EncodingType>url</EncodingType>"
        "<IsTruncated>false</IsTruncated>"
        "<Contents>"
        f"<Key>{KEY}</Key>"
        f"<LastModified>{LAST_MODIFIED}</LastModified>"
        f"<ETag>&quot;{ETAG.strip(chr(34))}&quot;</ETag>"
        "<ChecksumAlgorithm>CRC32</ChecksumAlgorithm>"
        "<ChecksumType>FULL_OBJECT</ChecksumType>"
        f"<Size>{size}</Size>"
        "<StorageClass>STANDARD</StorageClass>"
        "</Contents>"
        "</ListBucketResult>"
    ).encode()


def _listing_response(payload: bytes) -> native.S3HttpResponse:
    return native.S3HttpResponse(
        payload=payload,
        content_type="application/xml",
        content_length=len(payload),
        declared_content_length=None,
        received_at=RECEIVED_AT,
        request_id="LISTREQUEST",
    )


def _object_response(payload: bytes) -> native.S3HttpResponse:
    return native.S3HttpResponse(
        payload=payload,
        content_type="binary/octet-stream",
        content_length=len(payload),
        declared_content_length=len(payload),
        received_at=RECEIVED_AT + 1.0,
        etag=ETAG,
        last_modified=HTTP_LAST_MODIFIED,
        last_modified_timestamp=datetime(
            2026, 7, 19, 0, 9, 28, tzinfo=timezone.utc
        ).timestamp(),
        request_id="OBJECTREQUEST",
        request_id_2="secondary",
        server_side_encryption="AES256",
    )


def _capture(monkeypatch, tmp_path: Path, *, payload: bytes | None = None):
    hdf = _hdf_bytes() if payload is None else payload
    xml = _listing_xml(len(hdf))
    monkeypatch.setattr(native, "_fetch_listing", lambda _prefix: _listing_response(xml))
    monkeypatch.setattr(native, "_fetch_object", lambda _selected: _object_response(hdf))
    raw = RawArtifactStore((tmp_path / "raw").resolve())
    result = native.capture_latest_noaa_err_netcdf4(
        SimpleNamespace(job_id="noaa-quantitative-fixture", payload={}),
        raw_store=raw,
        now=datetime(2026, 7, 19, 0, 20, tzinfo=timezone.utc),
    )
    capture = raw.load_capture_receipt(
        result["raw_provenance"]["raw_capture_id"]
    )
    assert capture is not None
    return raw, capture


def _stores(tmp_path: Path, raw: RawArtifactStore):
    artifacts = NoaaErrDerivedArtifactStore((tmp_path / "derived").resolve())
    evidence = CanonicalEvidenceStore(
        (tmp_path / "canonical.sqlite3").resolve(),
        raw_artifact_reader=raw,
        clock=lambda: EVALUATED_AT + 1.0,
    )
    return artifacts, evidence


def _decode(tmp_path: Path, raw, capture, *, suffix: str = "a", region=REGION):
    artifacts, evidence = _stores(tmp_path, raw)
    result = decode_noaa_err_region(
        capture_id=capture.capture_id,
        decoder_job_id=_identity("decoder-job-", f"job-{suffix}"),
        attempt_id=_identity("decoder-attempt-", f"attempt-{suffix}"),
        region=region,
        raw_store=raw,
        artifact_store=artifacts,
        evidence_store=evidence,
        evaluated_at=EVALUATED_AT,
    )
    return result, artifacts, evidence


def test_valid_bounded_decode_preserves_rate_and_dqf_semantics(monkeypatch, tmp_path):
    raw, capture = _capture(monkeypatch, tmp_path)
    result, artifacts, evidence = _decode(tmp_path, raw, capture)

    assert result.outcome == "decoded_complete"
    assert result.artifact is not None
    decoded = artifacts.read_decoded(result.artifact)
    assert decoded.rrqpe_raw.shape == (6, 6)
    assert decoded.metadata["product"]["instantaneous_not_accumulation"] is True
    assert decoded.metadata["product"]["semantic_class"] == (
        "satellite_precipitation_rate_product"
    )
    assert decoded.metadata["grid"]["row_direction"] == "north_to_south"
    assert set(np.unique(decoded.dqf_raw)).issubset({-1, 0, 1, 2, 3})
    valid = decoded.rate_valid_mask == 1
    assert np.array_equal(
        decoded.precipitation_rate_mm_h[valid],
        decoded.rrqpe_raw[valid].astype(np.float64) * 0.1,
    )
    assert np.array_equal(
        decoded.dqf_bad_quality_mask,
        np.asarray(
            (decoded.dqf_raw != -1) & ((decoded.dqf_raw & 1) != 0),
            dtype=np.uint8,
        ),
    )
    assert np.array_equal(
        decoded.dqf_degraded_mask,
        np.asarray(
            (decoded.dqf_raw != -1) & ((decoded.dqf_raw & 2) != 0),
            dtype=np.uint8,
        ),
    )
    receipt = evidence.load(result.evidence_receipt.run_id)
    assert receipt is not None
    assert receipt.run.outcome == "decoded_complete"
    assert receipt.run.quantity == "instantaneous_precipitation_rate"
    assert receipt.run.unit == "mm/h"
    assert receipt.run.non_operational_candidate_artifact is not None


def test_worker_only_processor_samples_positive_noaa_target(monkeypatch, tmp_path):
    raw, capture = _capture(monkeypatch, tmp_path)
    desired = parse_desired_state({
        "authority_epoch": 1,
        "contract": "stv3-worker-desired-state-v1",
        "generation": 1,
        "issued_at": PRODUCT_START.timestamp() - 30.0,
        "previous_state_sha256": None,
        "producer_id": "ha:noaa-worker-only-test",
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": True,
            "region_sharing_distance_km": 100.0,
        },
        "targets": [{
            "available": True,
            "country_code": "BE",
            "display_name": "NOAA testpixel",
            "kind": "test",
            "latitude": 50.04,
            "location_accuracy_m": 1.0,
            "longitude": 3.96,
            "observation_radius_km": 250.0,
            "position_observed_at": PRODUCT_START.timestamp() - 10.0,
            "target_id": "noaa-positive-pixel",
        }],
    })

    observations = decode_worker_only_positive_observations(
        workload_id="satellite.noaa.err",
        desired_state=desired,
        capture_id=capture.capture_id,
        raw_store=raw,
        artifact_root=(tmp_path / "worker-only-derived").resolve(),
        evidence_path=(tmp_path / "worker-only-evidence").resolve(),
        evaluated_at=EVALUATED_AT,
        clock=lambda: EVALUATED_AT + 1.0,
    )

    assert len(observations) == 1
    observation = observations[0]
    assert observation.source_id == "satellite.noaa.err"
    assert observation.target_id == "noaa-positive-pixel"
    assert observation.rain_rate_mm_h == pytest.approx(0.1)
    assert observation.product_valid_at == PRODUCT_START.timestamp()


def test_exact_retry_reuses_same_immutable_artifact(monkeypatch, tmp_path):
    raw, capture = _capture(monkeypatch, tmp_path)
    first, artifacts, evidence = _decode(tmp_path, raw, capture)
    second = decode_noaa_err_region(
        capture_id=capture.capture_id,
        decoder_job_id=first.evidence_receipt.run.decoder_job_id,
        attempt_id=first.evidence_receipt.run.attempt_id,
        region=REGION,
        raw_store=raw,
        artifact_store=artifacts,
        evidence_store=evidence,
        evaluated_at=EVALUATED_AT,
    )
    assert second.artifact == first.artifact
    assert second.evidence_receipt.commit_id == first.evidence_receipt.commit_id


@pytest.mark.parametrize(
    "region",
    [
        NoaaErrRegion(-61.0, 0.0, 0.0, 1.0),
        NoaaErrRegion(0.0, 170.0, 1.0, -170.0),
        NoaaErrRegion(0.0, 0.0, 40.0, 40.0),
        NoaaErrRegion(1.0, 1.0, 1.0, 2.0),
    ],
)
def test_invalid_or_unbounded_region_fails_before_decode(monkeypatch, tmp_path, region):
    raw, capture = _capture(monkeypatch, tmp_path)
    artifacts, evidence = _stores(tmp_path, raw)
    with pytest.raises(ValueError, match="region"):
        decode_noaa_err_region(
            capture_id=capture.capture_id,
            decoder_job_id=_identity("decoder-job-", "invalid-region"),
            attempt_id=_identity("decoder-attempt-", "invalid-region"),
            region=region,
            raw_store=raw,
            artifact_store=artifacts,
            evidence_store=evidence,
            evaluated_at=EVALUATED_AT,
        )
    assert not artifacts.root.exists()


@pytest.mark.parametrize(
    ("kwargs", "reason"),
    [
        ({"unknown_dqf": True}, "unsupported:unknown_dqf_bits"),
        ({"negative_rrqpe": True}, "unsupported:negative_rrqpe_code"),
        ({"inconsistent_missing": True}, "unsupported:inconsistent_missingness"),
        ({"nonzero_dimension_scale": True}, "unsupported:dimension_scale_values"),
        ({"extra_root_object": True}, "unsupported:hdf_object_tree"),
        ({"rrqpe_level": 5}, "unsupported:hdf_storage_profile"),
        ({"dqf_meanings": "invented_quality"}, "unsupported:dqf_semantics"),
    ],
)
def test_semantic_or_profile_drift_is_payload_free_rejection(
    monkeypatch, tmp_path, kwargs, reason,
):
    raw, capture = _capture(monkeypatch, tmp_path, payload=_hdf_bytes(**kwargs))
    result, artifacts, _evidence = _decode(tmp_path, raw, capture)
    assert result.outcome == "unsupported"
    assert result.reason_code == reason
    assert result.artifact is None
    assert result.evidence_receipt.run.quantity is None
    assert result.evidence_receipt.run.unit is None
    assert result.evidence_receipt.run.coverage is None
    assert result.evidence_receipt.run.native_grid is None
    assert not artifacts.objects_path.exists()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"latitude_resolution": 0.02009},
        {"missing_inputs": 5},
        {"input_files": INPUT_FILES.replace(
            "RRQPE-ABI-INST_v1r1_g18_s202607182350232_e202607182359541_c202607190000216.nc",
            "RRQPE-ABI-INST_v1r1_g19_s202607182350190_e202607182359498_c202607190000118.nc",
        )},
    ],
)
def test_native_capture_rejects_grid_or_input_completeness_drift(
    monkeypatch, tmp_path, kwargs,
):
    with pytest.raises(native.NoaaErrNativeUnsupportedError):
        _capture(monkeypatch, tmp_path, payload=_hdf_bytes(**kwargs))


@pytest.mark.parametrize(
    ("kwargs", "reason"),
    [
        ({"omit_axis_bindings": True}, "unsupported:dimension_scale"),
        ({"reverse_coordinates": True}, "unsupported:dataset_axes"),
        ({"dimension_scale_dtype": "<f4"}, "unsupported:dimension_scale"),
        (
            {"dimension_id_dtype": "<i8"},
            "unsupported:dimension_scale_binding",
        ),
        ({"coordinate_dtype": "<i8"}, "unsupported:dataset_axes"),
        (
            {"wide_reference_dimension": True},
            "unsupported:dimension_scale_binding",
        ),
    ],
)
def test_dimension_scale_and_axis_binding_are_exact(
    monkeypatch, tmp_path, kwargs, reason,
):
    raw, capture = _capture(monkeypatch, tmp_path, payload=_hdf_bytes(**kwargs))
    result, artifacts, _evidence = _decode(tmp_path, raw, capture)
    assert result.outcome == "unsupported"
    assert result.reason_code == reason
    assert result.artifact is None
    assert not artifacts.objects_path.exists()


def test_native_profile_errors_are_typed_semantic_vs_corruption():
    selected = native._parse_listing_xml(
        _listing_xml(len(_hdf_bytes())), expected_prefix=PREFIX
    )
    with pytest.raises(native.NoaaErrNativeUnsupportedError):
        native._inspect_netcdf4_identity(
            _hdf_bytes(latitude_resolution=0.02009), selected=selected
        )
    with pytest.raises(native.NoaaErrNativeCorruptionError):
        native._inspect_netcdf4_identity(b"geen-hdf5", selected=selected)


def test_unallocated_selected_chunks_never_become_zero_rain(monkeypatch, tmp_path):
    raw, capture = _capture(
        monkeypatch,
        tmp_path,
        payload=_hdf_bytes(allocate_selected_chunks=False),
    )
    result, artifacts, _evidence = _decode(tmp_path, raw, capture)
    assert result.outcome == "incomplete"
    assert result.reason_code == "incomplete:unallocated_or_unfiltered_chunk"
    assert result.artifact is None
    assert not artifacts.objects_path.exists()


def test_selected_chunk_cannot_bypass_declared_deflate_filter(monkeypatch, tmp_path):
    raw, capture = _capture(
        monkeypatch,
        tmp_path,
        payload=_hdf_bytes(skip_rrqpe_deflate_for_selected_chunk=True),
    )
    result, artifacts, _evidence = _decode(tmp_path, raw, capture)
    assert result.outcome == "incomplete"
    assert result.reason_code == "incomplete:unallocated_or_unfiltered_chunk"
    assert result.artifact is None
    assert not artifacts.objects_path.exists()


def test_missing_capture_has_no_canonical_result(tmp_path):
    raw = RawArtifactStore((tmp_path / "raw").resolve())
    raw.initialize()
    artifacts, evidence = _stores(tmp_path, raw)
    with pytest.raises(NoaaErrCaptureMissingError):
        decode_noaa_err_region(
            capture_id="capture-" + "0" * 64,
            decoder_job_id=_identity("decoder-job-", "missing"),
            attempt_id=_identity("decoder-attempt-", "missing"),
            region=REGION,
            raw_store=raw,
            artifact_store=artifacts,
            evidence_store=evidence,
            evaluated_at=EVALUATED_AT,
        )
    assert evidence.stats()["decoder_runs"] == 0


def test_raw_object_tamper_is_integrity_failure_without_artifact(
    monkeypatch, tmp_path,
):
    raw, capture = _capture(monkeypatch, tmp_path)
    netcdf_member = next(member for member in capture.members if member.role == "netcdf4")
    receipt = raw.load_receipt(netcdf_member.receipt_id)
    assert receipt is not None
    path = Path(receipt.artifact_path)
    payload = bytearray(path.read_bytes())
    payload[-1] ^= 1
    path.write_bytes(payload)
    artifacts, evidence = _stores(tmp_path, raw)
    with pytest.raises(RawArtifactIntegrityError):
        decode_noaa_err_region(
            capture_id=capture.capture_id,
            decoder_job_id=_identity("decoder-job-", "tamper"),
            attempt_id=_identity("decoder-attempt-", "tamper"),
            region=REGION,
            raw_store=raw,
            artifact_store=artifacts,
            evidence_store=evidence,
            evaluated_at=EVALUATED_AT,
        )
    assert evidence.stats()["decoder_runs"] == 0


def test_published_artifact_tamper_is_detected(monkeypatch, tmp_path):
    raw, capture = _capture(monkeypatch, tmp_path)
    result, artifacts, _evidence = _decode(tmp_path, raw, capture)
    assert result.artifact is not None
    path = artifacts.objects_path / f"{result.artifact.sha256}.neqr"
    changed = bytearray(path.read_bytes())
    changed[-1] ^= 1
    path.write_bytes(changed)
    with pytest.raises(NoaaErrArtifactError, match="SHA-256"):
        artifacts.read_decoded(result.artifact)


def test_rehashed_semantic_header_tamper_is_still_rejected(monkeypatch, tmp_path):
    raw, capture = _capture(monkeypatch, tmp_path)
    result, artifacts, _evidence = _decode(tmp_path, raw, capture)
    assert result.artifact is not None
    payload = artifacts.read_bytes(result.artifact)
    prefix_size = len(quantitative.ARTIFACT_MAGIC) + 4
    header_size = struct.unpack(
        ">I", payload[len(quantitative.ARTIFACT_MAGIC):prefix_size]
    )[0]
    header_end = prefix_size + header_size
    metadata = json.loads(payload[prefix_size:header_end].decode("utf-8"))
    metadata["grid"]["maximum_latitude"] += 0.02
    changed_header = quantitative._canonical_json(metadata)
    changed = (
        quantitative.ARTIFACT_MAGIC
        + struct.pack(">I", len(changed_header))
        + changed_header
        + payload[header_end:]
    )
    changed_ref = artifacts.publish(changed)
    assert changed_ref.sha256 == hashlib.sha256(changed).hexdigest()
    with pytest.raises(NoaaErrArtifactError, match="gridbinding"):
        artifacts.read_decoded(changed_ref)


@pytest.mark.parametrize(
    "mutation",
    [
        "capture_id",
        "source_digest",
        "product_time",
        "empty_receipt_id",
    ],
)
def test_rehashed_provenance_forgery_is_rejected(
    monkeypatch, tmp_path, mutation,
):
    raw, capture = _capture(monkeypatch, tmp_path)
    result, artifacts, _evidence = _decode(tmp_path, raw, capture)
    assert result.artifact is not None
    payload = artifacts.read_bytes(result.artifact)
    prefix_size = len(quantitative.ARTIFACT_MAGIC) + 4
    header_size = struct.unpack(
        ">I", payload[len(quantitative.ARTIFACT_MAGIC):prefix_size]
    )[0]
    header_end = prefix_size + header_size
    metadata = json.loads(payload[prefix_size:header_end].decode("utf-8"))
    if mutation == "capture_id":
        metadata["capture"]["capture_id"] = "capture-" + "0" * 64
    elif mutation == "source_digest":
        metadata["source"]["netcdf_content_sha256"] = "1" * 64
    elif mutation == "product_time":
        metadata["product"]["product_start"] += 3600.0
        metadata["product"]["product_end"] += 3600.0
    else:
        metadata["capture"]["members"][0]["receipt_id"] = ""
    changed_header = quantitative._canonical_json(metadata)
    changed = (
        quantitative.ARTIFACT_MAGIC
        + struct.pack(">I", len(changed_header))
        + changed_header
        + payload[header_end:]
    )
    changed_ref = artifacts.publish(changed)
    with pytest.raises(NoaaErrArtifactError):
        artifacts.read_decoded(changed_ref)


@pytest.mark.parametrize("target", ["arrays", "compression"])
def test_rehashed_unknown_nested_artifact_fields_are_rejected(
    monkeypatch, tmp_path, target,
):
    raw, capture = _capture(monkeypatch, tmp_path)
    result, artifacts, _evidence = _decode(tmp_path, raw, capture)
    assert result.artifact is not None
    payload = artifacts.read_bytes(result.artifact)
    prefix_size = len(quantitative.ARTIFACT_MAGIC) + 4
    header_size = struct.unpack(
        ">I", payload[len(quantitative.ARTIFACT_MAGIC):prefix_size]
    )[0]
    header_end = prefix_size + header_size
    metadata = json.loads(payload[prefix_size:header_end].decode("utf-8"))
    if target == "arrays":
        metadata["arrays"][0]["admission"] = True
    else:
        metadata["compression"]["admission"] = True
    changed_header = quantitative._canonical_json(metadata)
    changed = (
        quantitative.ARTIFACT_MAGIC
        + struct.pack(">I", len(changed_header))
        + changed_header
        + payload[header_end:]
    )
    with pytest.raises(NoaaErrArtifactError):
        quantitative._parse_artifact(changed)


@pytest.mark.parametrize(
    "header",
    [
        b'{"schema":"stv3-worker-noaa-err-quantitative-region-v2","x":NaN}',
        b'{"schema":"stv3-worker-noaa-err-quantitative-region-v2","x":"\\ud800"}',
        (
            b'{"schema":"stv3-worker-noaa-err-quantitative-region-v2","x":'
            + (b"9" * 5000)
            + b"}"
        ),
    ],
)
def test_nonstandard_or_pathological_json_is_domain_error(header):
    payload = (
        quantitative.ARTIFACT_MAGIC
        + struct.pack(">I", len(header))
        + header
        + b"x"
    )
    with pytest.raises(NoaaErrArtifactError, match="geen JSON"):
        quantitative._parse_artifact(payload)


def test_missing_artifact_object_is_domain_error(tmp_path):
    store = NoaaErrDerivedArtifactStore((tmp_path / "missing-derived").resolve())
    reference = quantitative.NoaaErrArtifactRef(
        handle=quantitative.ARTIFACT_HANDLE_PREFIX + ("0" * 64),
        sha256="0" * 64,
        size_bytes=1,
    )
    with pytest.raises(NoaaErrArtifactError, match="ontbreekt"):
        store.read_bytes(reference)


def test_store_rejects_unknown_object_and_bounds_staging(monkeypatch, tmp_path):
    store = NoaaErrDerivedArtifactStore((tmp_path / "bounded-derived").resolve())
    store.initialize()
    (store.objects_path / "junk").write_bytes(b"x")
    with pytest.raises(NoaaErrArtifactError, match="vreemd object"):
        store.publish(b"first")
    (store.objects_path / "junk").unlink()

    for name in ("a", "b", "c"):
        with (store.staging_path / f".noaa-err-{name}.tmp").open("wb") as stream:
            stream.truncate(quantitative.MAX_ARTIFACT_BYTES)
    with pytest.raises(NoaaErrArtifactCapacityError, match="staging"):
        store.publish(b"second")

    with pytest.raises(ValueError, match="capaciteit"):
        NoaaErrDerivedArtifactStore(
            (tmp_path / "unbounded").resolve(),
            max_total_bytes=quantitative.MAX_ARTIFACT_TOTAL_BYTES + 1,
        )


def test_object_count_cap_and_objects_directory_fsync(monkeypatch, tmp_path):
    calls: list[Path] = []
    monkeypatch.setattr(
        quantitative, "_fsync_directory", lambda path: calls.append(Path(path))
    )
    monkeypatch.setattr(quantitative, "MAX_ARTIFACT_OBJECTS", 1)
    store = NoaaErrDerivedArtifactStore((tmp_path / "count-derived").resolve())
    store.initialize()
    (store.objects_path / (("0" * 64) + ".neqr")).write_bytes(b"existing")
    with pytest.raises(NoaaErrArtifactCapacityError, match="capaciteit"):
        store.publish(b"new")

    monkeypatch.setattr(quantitative, "MAX_ARTIFACT_OBJECTS", 2)
    reference = store.publish(b"new")
    assert reference.size_bytes == 3
    assert store.objects_path in calls


def test_expired_cooperative_deadline_has_no_evidence_or_artifact(
    monkeypatch, tmp_path,
):
    raw, capture = _capture(monkeypatch, tmp_path)
    artifacts, evidence = _stores(tmp_path, raw)
    with pytest.raises(quantitative.NoaaErrDecodeDeadlineError, match="decode_start"):
        decode_noaa_err_region(
            capture_id=capture.capture_id,
            decoder_job_id=_identity("decoder-job-", "expired"),
            attempt_id=_identity("decoder-attempt-", "expired"),
            region=REGION,
            raw_store=raw,
            artifact_store=artifacts,
            evidence_store=evidence,
            evaluated_at=EVALUATED_AT,
            monotonic_deadline=10.0,
            monotonic_clock=lambda: 10.0,
        )
    assert evidence.stats()["decoder_runs"] == 0
    assert not artifacts.root.exists()


def test_absolute_deadline_is_rechecked_after_raw_authority_validation(
    monkeypatch, tmp_path,
):
    raw, capture = _capture(monkeypatch, tmp_path)
    artifacts, evidence = _stores(tmp_path, raw)
    readings = iter((0.0, 0.0, 0.0, 2.0))

    with pytest.raises(
        quantitative.NoaaErrDecodeDeadlineError,
        match="raw_capture_validated",
    ):
        decode_noaa_err_region(
            capture_id=capture.capture_id,
            decoder_job_id=_identity("decoder-job-", "mid-decode-expired"),
            attempt_id=_identity("decoder-attempt-", "mid-decode-expired"),
            region=REGION,
            raw_store=raw,
            artifact_store=artifacts,
            evidence_store=evidence,
            evaluated_at=EVALUATED_AT,
            monotonic_deadline=1.0,
            monotonic_clock=lambda: next(readings),
        )

    assert evidence.stats()["decoder_runs"] == 0
    assert not artifacts.root.exists()


def test_concurrent_distinct_publications_cannot_overbook_capacity(tmp_path):
    root = (tmp_path / "concurrent-derived").resolve()
    capacity = quantitative.MAX_ARTIFACT_BYTES
    stores = [
        NoaaErrDerivedArtifactStore(root, max_total_bytes=capacity),
        NoaaErrDerivedArtifactStore(root, max_total_bytes=capacity),
    ]
    stores[0].initialize()
    payloads = [b"a" * 1024, b"b" * 1024]
    with (stores[0].objects_path / (("0" * 64) + ".neqr")).open("wb") as stream:
        stream.truncate(capacity - len(payloads[0]) - 1)
    barrier = threading.Barrier(2)

    def publish(index: int):
        barrier.wait(timeout=5.0)
        try:
            return stores[index].publish(payloads[index])
        except NoaaErrArtifactCapacityError as error:
            return error

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(publish, range(2)))
    assert sum(not isinstance(value, Exception) for value in outcomes) == 1
    assert sum(
        isinstance(value, NoaaErrArtifactCapacityError) for value in outcomes
    ) == 1
    assert sum(path.stat().st_size for path in stores[0].objects_path.iterdir()) <= capacity


def test_distinct_processes_cannot_overbook_capacity(tmp_path):
    root = (tmp_path / "multiprocess-derived").resolve()
    capacity = quantitative.MAX_ARTIFACT_BYTES
    store = NoaaErrDerivedArtifactStore(root, max_total_bytes=capacity)
    store.initialize()
    with (store.objects_path / (("0" * 64) + ".neqr")).open("wb") as stream:
        stream.truncate(capacity - 1025)
    gate = (tmp_path / "publish.go").resolve()
    child_code = r'''
import pathlib, sys, time
from storm_tracker_worker.noaa_err_quantitative import (
    NoaaErrArtifactCapacityError, NoaaErrDerivedArtifactStore,
)
root, ready, gate, marker = map(pathlib.Path, sys.argv[1:5])
ready.write_text("ready", encoding="ascii")
deadline = time.monotonic() + 15.0
while not gate.exists():
    if time.monotonic() >= deadline:
        raise TimeoutError("gate")
    time.sleep(0.01)
store = NoaaErrDerivedArtifactStore(root, max_total_bytes=32 * 1024 * 1024)
try:
    store.publish(marker.name.encode("ascii") * 1024)
except NoaaErrArtifactCapacityError:
    print("capacity")
else:
    print("published")
'''
    repository = Path(__file__).parents[1]
    environment = {**os.environ, "PYTHONPATH": str(repository)}
    processes = []
    ready_paths = []
    for marker in ("a", "b"):
        ready = (tmp_path / f"ready-{marker}").resolve()
        ready_paths.append(ready)
        processes.append(subprocess.Popen(
            [
                sys.executable,
                "-c",
                child_code,
                str(root),
                str(ready),
                str(gate),
                marker,
            ],
            cwd=repository,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ))
    deadline = time.monotonic() + 15.0
    while not all(path.exists() for path in ready_paths):
        if time.monotonic() >= deadline:
            for process in processes:
                process.kill()
            pytest.fail("multiprocess-publicatietest bereikte gate niet")
        time.sleep(0.01)
    gate.write_text("go", encoding="ascii")
    outputs = []
    for process in processes:
        stdout, stderr = process.communicate(timeout=20.0)
        assert process.returncode == 0, stderr
        outputs.append(stdout.strip())
    assert sorted(outputs) == ["capacity", "published"]
    assert sum(path.stat().st_size for path in store.objects_path.iterdir()) <= capacity


def test_import_is_inert_and_does_not_open_network_or_sqlite(tmp_path):
    script = r'''
import pathlib, socket, sqlite3, threading
before = sorted(str(path.relative_to(pathlib.Path.cwd())) for path in pathlib.Path.cwd().rglob("*"))
socket.create_connection = lambda *a, **k: (_ for _ in ()).throw(AssertionError("network"))
sqlite3.connect = lambda *a, **k: (_ for _ in ()).throw(AssertionError("sqlite"))
threading.Thread.start = lambda *a, **k: (_ for _ in ()).throw(AssertionError("thread"))
import storm_tracker_worker.noaa_err_quantitative
after = sorted(str(path.relative_to(pathlib.Path.cwd())) for path in pathlib.Path.cwd().rglob("*"))
assert before == after
'''
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        env={"PYTHONPATH": str(Path(__file__).parents[1])},
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr


def test_module_is_not_wired_to_registry_or_service():
    root = Path(__file__).parents[1]
    for relative in (
        "storm_tracker_worker/service.py",
        "storm_tracker_worker/provider_capture_registry.py",
    ):
        assert "noaa_err_quantitative" not in (root / relative).read_text(
            encoding="utf-8"
        )
