from __future__ import annotations

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

import h5py
import numpy as np
import pytest

from storm_tracker_worker import knmi_rtcor_quantitative as decoder_module
from storm_tracker_worker.canonical_evidence_store import CanonicalEvidenceStore
from storm_tracker_worker.desired_state import parse_desired_state
from storm_tracker_worker.knmi_rtcor_quantitative import (
    ARTIFACT_SCHEMA,
    EXPECTED_COLUMNS,
    EXPECTED_ROWS,
    KnmiRtcorArtifactCapacityError,
    KnmiRtcorArtifactError,
    KnmiRtcorCaptureMissingError,
    KnmiRtcorDecodeDeadlineError,
    KnmiRtcorDerivedArtifactStore,
    decode_knmi_rtcor_capture,
)
from storm_tracker_worker.native_capture import (
    NativeCaptureMemberInput,
    capture_native_set,
)
from storm_tracker_worker.raw_artifacts import (
    RawArtifactIntegrityError,
    RawArtifactStore,
)
from storm_tracker_worker.worker_only_radar_processing import (
    decode_worker_only_positive_observations,
)


PRODUCT_END = datetime(2026, 8, 2, 12, 0, tzinfo=timezone.utc)
PRODUCT_START = datetime(2026, 8, 2, 11, 55, tzinfo=timezone.utc)
RECEIVED_AT = PRODUCT_END.timestamp() + 123.25
EVALUATED_AT = RECEIVED_AT + 10.0
FILENAME = "RAD_NL25_RAC_RT_202608021200.h5"
PROJECTION = (
    "+proj=stere +x_0=0 +y_0=0 +lat_0=90 +lon_0=0 "
    "+lat_ts=60 +a=6378.388 +b=6356.906"
)
CORNERS = np.asarray(
    [0.0, 49.362064, 0.0, 55.973602, 10.856453, 55.388973, 9.009301, 48.895306],
    dtype=np.float64,
)


def _identity(prefix: str, value: str) -> str:
    return prefix + hashlib.sha256(value.encode()).hexdigest()


def _hdf_bytes(**overrides) -> bytes:
    product_name = overrides.pop("product_name", "RAD_NL25_COR_NA")
    tag = overrides.pop("tag", "3.6")
    start = overrides.pop("start", "02-AUG-2026;11:55:00.000")
    end = overrides.pop("end", "02-AUG-2026;12:00:00.000")
    rows = overrides.pop("rows", EXPECTED_ROWS)
    columns = overrides.pop("columns", EXPECTED_COLUMNS)
    pixel_x = overrides.pop("pixel_x", 1.0000035)
    pixel_y = overrides.pop("pixel_y", -1.0000049)
    pixel_def = overrides.pop("pixel_def", "LU")
    projection = overrides.pop("projection", PROJECTION)
    corners = overrides.pop("corners", CORNERS)
    precip_formula = overrides.pop("precip_formula", "GEO=0.010000*PV+0")
    precip_missing = overrides.pop("precip_missing", 65534)
    precip_out = overrides.pop("precip_out", 65535)
    quality_formula = overrides.pop("quality_formula", "GEO=0.004*PV+0")
    quality_missing = overrides.pop("quality_missing", 254)
    quality_out = overrides.pop("quality_out", 255)
    adjustment_formula = overrides.pop("adjustment_formula", "GEO=0.16*PV-20.24")
    adjustment_missing = overrides.pop("adjustment_missing", 254)
    adjustment_out = overrides.pop("adjustment_out", 255)
    precip_dtype = np.dtype(overrides.pop("precip_dtype", "<u2"))
    aux_dtype = np.dtype(overrides.pop("aux_dtype", "u1"))
    shape = tuple(overrides.pop("shape", (EXPECTED_ROWS, EXPECTED_COLUMNS)))
    compression = overrides.pop("compression", "gzip")
    shuffle = overrides.pop("shuffle", False)
    fletcher32 = overrides.pop("fletcher32", False)
    chunk_shape = tuple(overrides.pop("chunk_shape", shape))
    write_data = overrides.pop("write_data", True)
    include_quality = overrides.pop("include_quality", True)
    include_adjustment = overrides.pop("include_adjustment", True)
    image_product = overrides.pop("image_product", product_name)
    image_parameter = overrides.pop("image_parameter", "PRECIP_[MM]")
    image_count = overrides.pop("image_count", 3)
    if overrides:
        raise AssertionError(f"unused fixture overrides: {sorted(overrides)}")

    stream = io.BytesIO()
    with h5py.File(stream, "w") as root:
        overview = root.create_group("overview")
        overview.attrs["product_group_name"] = np.bytes_(product_name.encode())
        overview.attrs["hdftag_version_number"] = np.bytes_(tag.encode())
        overview.attrs["product_datetime_start"] = np.bytes_(start.encode())
        overview.attrs["product_datetime_end"] = np.bytes_(end.encode())
        overview.attrs["number_image_groups"] = image_count

        geographic = root.create_group("geographic")
        geographic.attrs["geo_number_columns"] = columns
        geographic.attrs["geo_number_rows"] = rows
        geographic.attrs["geo_pixel_size_x"] = pixel_x
        geographic.attrs["geo_pixel_size_y"] = pixel_y
        geographic.attrs["geo_par_pixel"] = np.bytes_(b"X,Y")
        geographic.attrs["geo_dim_pixel"] = np.bytes_(b"KM,KM")
        geographic.attrs["geo_column_offset"] = 0.0
        geographic.attrs["geo_row_offset"] = 3649.9814
        geographic.attrs["geo_pixel_def"] = np.bytes_(pixel_def.encode())
        geographic.attrs["geo_product_corners"] = corners
        map_projection = geographic.create_group("map_projection")
        map_projection.attrs["projection_indication"] = np.bytes_(b"Y")
        map_projection.attrs["projection_name"] = np.bytes_(b"STEREOGRAPHIC")
        map_projection.attrs["projection_proj4_params"] = np.bytes_(projection.encode())

        layer_specs = [
            (
                "image1", image_parameter, precip_dtype, precip_formula,
                precip_missing, precip_out,
            )
        ]
        if include_quality:
            layer_specs.append(
                ("image2", "QUALITY_[-]", aux_dtype, quality_formula, quality_missing, quality_out)
            )
        if include_adjustment:
            layer_specs.append(
                (
                    "image3", "ADJUSTMENT_FACTOR_[DB]", aux_dtype,
                    adjustment_formula, adjustment_missing, adjustment_out,
                )
            )
        for group_name, parameter, dtype, formula, missing, out in layer_specs:
            group = root.create_group(group_name)
            group.attrs["image_product_name"] = np.bytes_(image_product.encode())
            group.attrs["image_geo_parameter"] = np.bytes_(parameter.encode())
            # This is the observed live RTCOR declaration: auxiliary uint8
            # layers still declare two bytes per pixel and the matching size.
            group.attrs["image_bytes_per_pixel"] = 2
            group.attrs["image_size"] = EXPECTED_ROWS * EXPECTED_COLUMNS * 2
            calibration = group.create_group("calibration")
            calibration.attrs["calibration_flag"] = np.bytes_(b"Y")
            calibration.attrs["calibration_formulas"] = np.bytes_(formula.encode())
            calibration.attrs["calibration_missing_data"] = missing
            calibration.attrs["calibration_out_of_image"] = out
            values = np.zeros(shape, dtype=dtype)
            if values.ndim == 2 and values.shape[0] > 10 and values.shape[1] > 12:
                if group_name == "image1":
                    values[0, 0], values[0, 1] = 65534, 65535
                    values[10, 10], values[10, 11], values[10, 12] = 1, 100, 253
                elif group_name == "image2":
                    values[0, 0], values[0, 1] = 254, 255
                    values[10, 10], values[10, 11], values[10, 12] = 0, 125, 253
                else:
                    values[0, 0], values[0, 1] = 254, 255
                    values[10, 10], values[10, 11], values[10, 12] = 0, 126, 253
            options = {}
            if compression is not None:
                options = {
                    "chunks": chunk_shape,
                    "compression": compression,
                    "fletcher32": fletcher32,
                    "shuffle": shuffle,
                }
            if write_data:
                dataset = group.create_dataset("image_data", data=values, **options)
            else:
                dataset = group.create_dataset(
                    "image_data", shape=shape, dtype=dtype, **options
                )
            dataset.attrs["CLASS"] = np.bytes_(b"IMAGE")
            dataset.attrs["IMAGE_VERSION"] = np.bytes_(b"1.2")
    return stream.getvalue()


def _mutated_hdf(mutator) -> bytes:
    stream = io.BytesIO(_hdf_bytes())
    with h5py.File(stream, "r+") as root:
        mutator(root)
    return stream.getvalue()


def _skip_quality_gzip_filter(root: h5py.File) -> None:
    dataset = root["image2/image_data"]
    raw_chunk = np.zeros(dataset.shape, dtype=dataset.dtype).tobytes(order="C")
    dataset.id.write_direct_chunk((0, 0), raw_chunk, filter_mask=1)


def _alias_adjustment_to_quality_dataset(root: h5py.File) -> None:
    quality = root["image2/image_data"]
    del root["image3/image_data"]
    root["image3"]["image_data"] = quality


def _corrupt_precipitation_chunk(root: h5py.File) -> None:
    dataset = root["image1/image_data"]
    filter_mask, encoded = dataset.id.read_direct_chunk((0, 0))
    assert filter_mask == 0
    damaged = bytearray(encoded)
    damaged[len(damaged) // 2] ^= 0x5A
    dataset.id.write_direct_chunk((0, 0), bytes(damaged), filter_mask=0)


def _truncated_hdf_corpus() -> list[tuple[str, bytes]]:
    valid = _hdf_bytes()
    cuts = (1, 8, 64, 512, 4096, len(valid) // 4, len(valid) // 2, len(valid) - 1)
    return [(f"truncated-at-{cut}", valid[:cut]) for cut in cuts]


def _capture(tmp_path: Path, *, hdf: bytes | None = None, extra_role: bool = False):
    payload = _hdf_bytes() if hdf is None else hdf
    digest = hashlib.sha256(payload).hexdigest()
    store = RawArtifactStore((tmp_path / "raw").resolve())
    store.initialize()
    members = [NativeCaptureMemberInput(
        role="hdf5",
        product="knmi_nl_rdr_data_rtcor_5m_hdf5",
        content_type="application/x-hdf5",
        received_at=RECEIVED_AT,
        upstream_id=f"nl_rdr_data_rtcor_5m/1.0|filename={FILENAME}",
        payload=payload,
        provenance={
            "dataset_license": "CC-BY-4.0",
            "dataset_name": "nl_rdr_data_rtcor_5m",
            "dataset_version": "1.0",
            "declared_accumulation_minutes": 5.0,
            "declared_quantity": "precipitation_accumulation",
            "declared_semantic_class": "quantitative_precipitation_accumulation",
            "declared_unit": "mm",
            "download_size_bytes": len(payload),
            "listing_size_bytes": len(payload),
            "quantitative_semantics_verified_after_decode": False,
            "selection_limit": 1,
            "selection_order": "created_descending",
            "structure_inspector_version": "knmi-rtcor-live-golden-identity-v2",
            "upstream_filename": FILENAME,
        },
    )]
    if extra_role:
        members.append(NativeCaptureMemberInput(
            role="unexpected",
            product="knmi_other",
            content_type="application/octet-stream",
            received_at=RECEIVED_AT,
            upstream_id="unexpected",
            payload=b"unexpected",
            provenance={"semantic_class": "unknown"},
        ))
    result = capture_native_set(
        store,
        provider="knmi",
        capture_product="knmi_nl_rdr_data_rtcor_5m_capture",
        fetch_id="knmi-rtcor-quantitative-fixture",
        upstream_capture_id=(
            "knmi_nl_rdr_data_rtcor_5m_capture"
            f"|filename={FILENAME}"
            f"|product_timestamp={PRODUCT_END.timestamp():.6f}"
            f"|content_sha256={digest}"
        ),
        product_timestamp=PRODUCT_END.timestamp(),
        collector="knmi_open_data_native_file",
        collector_version="worker-knmi-rtcor-5m-exact-raw-v1",
        members=members,
    )
    return store, result.capture


def _stores(tmp_path: Path, raw_store: RawArtifactStore):
    artifacts = KnmiRtcorDerivedArtifactStore((tmp_path / "derived").resolve())
    evidence = CanonicalEvidenceStore(
        (tmp_path / "canonical.sqlite3").resolve(),
        raw_artifact_reader=raw_store,
        clock=lambda: EVALUATED_AT + 1.0,
    )
    return artifacts, evidence


def _decode(tmp_path: Path, raw_store, capture, suffix="a"):
    artifacts, evidence = _stores(tmp_path, raw_store)
    result = decode_knmi_rtcor_capture(
        capture_id=capture.capture_id,
        decoder_job_id=_identity("decoder-job-", f"job-{suffix}"),
        attempt_id=_identity("decoder-attempt-", f"attempt-{suffix}"),
        raw_store=raw_store,
        artifact_store=artifacts,
        evidence_store=evidence,
        evaluated_at=EVALUATED_AT,
    )
    return result, artifacts, evidence


def test_exact_three_layer_decode_preserves_native_values_masks_and_adjustment(tmp_path):
    raw_store, capture = _capture(tmp_path)

    result, artifacts, evidence = _decode(tmp_path, raw_store, capture)

    assert result.outcome == "decoded_complete"
    assert result.reason_code == "decoded_complete"
    assert result.artifact is not None
    assert result.evidence_receipt.is_structurally_complete_candidate is True
    assert result.evidence_receipt.run.non_operational_candidate_artifact.sha256 == result.artifact.sha256
    raster = artifacts.read_decoded(result.artifact)
    assert raster.metadata["schema"] == ARTIFACT_SCHEMA
    assert raster.precipitation_raw.shape == (765, 700)
    assert raster.precipitation_raw.dtype == np.dtype("<u2")
    assert raster.precipitation_missing_mask[0, 0] == 1
    assert raster.precipitation_out_of_image_mask[0, 1] == 1
    assert raster.precipitation_valid_mask[10, 11] == 1
    assert raster.precipitation_mm[10, 10] == 0.01
    assert raster.precipitation_mm[10, 11] == 1.0
    assert raster.precipitation_mm[10, 12] == pytest.approx(2.53)
    assert raster.quality_missing_mask[0, 0] == 1
    assert raster.quality_out_of_image_mask[0, 1] == 1
    assert raster.quality_index[10, 11] == 0.5
    assert raster.quality_index[10, 12] == pytest.approx(1.012)
    assert raster.adjustment_missing_mask[0, 0] == 1
    assert raster.adjustment_out_of_image_mask[0, 1] == 1
    assert raster.adjustment_db[10, 10] == -20.24
    assert raster.adjustment_db[10, 11] == pytest.approx(-0.08)
    assert raster.adjustment_db[10, 12] == pytest.approx(20.24)
    # The delivered adjustment layer is evidence, not an instruction to alter
    # the already gauge-adjusted image1 precipitation a second time.
    assert raster.precipitation_mm[10, 11] == 1.0
    assert raster.metadata["layers"]["adjustment"]["application"] == (
        "diagnostic_delivered_field_not_applied_by_decoder"
    )
    assert raster.metadata["layers"]["precipitation"]["upstream_gauge_adjusted"] is True
    assert raster.metadata["layers"]["precipitation"]["upstream_adjustment_state"] == (
        "gauge_adjusted_by_knmi_before_delivery"
    )
    assert raster.metadata["layers"]["adjustment"]["may_reapply_to_image1"] is False
    assert raster.metadata["layers"]["adjustment"]["reapplication_policy"] == (
        "never_reapply_image1_already_gauge_adjusted"
    )
    assert raster.precipitation_mm.flags.writeable is False
    assert evidence.audit()["healthy"] is True

    serialized = json.dumps(raster.metadata, sort_keys=True).lower()
    for forbidden in ("cells", "threshold", "rain_decision", "dry", "admission_score", "0.98"):
        assert forbidden not in serialized


def test_worker_only_processor_decodes_knmi_but_withholds_uncertified_grid(
    tmp_path,
):
    raw_store, capture = _capture(tmp_path)
    desired = parse_desired_state({
        "authority_epoch": 1,
        "contract": "stv3-worker-desired-state-v1",
        "generation": 1,
        "issued_at": PRODUCT_END.timestamp() - 30.0,
        "previous_state_sha256": None,
        "producer_id": "ha:knmi-worker-only-test",
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": True,
            "region_sharing_distance_km": 100.0,
        },
        "targets": [{
            "available": True,
            "country_code": "NL",
            "display_name": "KNMI testdoel",
            "kind": "test",
            "latitude": 52.1,
            "location_accuracy_m": 1.0,
            "longitude": 5.1,
            "observation_radius_km": 250.0,
            "position_observed_at": PRODUCT_END.timestamp() - 10.0,
            "target_id": "knmi-test-target",
        }],
    })

    observations = decode_worker_only_positive_observations(
        workload_id="radar.knmi.rtcor",
        desired_state=desired,
        capture_id=capture.capture_id,
        raw_store=raw_store,
        artifact_root=(tmp_path / "worker-only-derived").resolve(),
        evidence_path=(tmp_path / "worker-only-evidence").resolve(),
        evaluated_at=EVALUATED_AT,
        clock=lambda: EVALUATED_AT + 1.0,
    )

    assert observations == ()


def test_same_capture_is_content_identical_across_retry_attempts(tmp_path):
    raw_store, capture = _capture(tmp_path)
    artifacts, evidence = _stores(tmp_path, raw_store)
    first = decode_knmi_rtcor_capture(
        capture_id=capture.capture_id,
        decoder_job_id=_identity("decoder-job-", "stable-job"),
        attempt_id=_identity("decoder-attempt-", "attempt-a"),
        raw_store=raw_store,
        artifact_store=artifacts,
        evidence_store=evidence,
        evaluated_at=EVALUATED_AT,
    )
    second = decode_knmi_rtcor_capture(
        capture_id=capture.capture_id,
        decoder_job_id=_identity("decoder-job-", "stable-job"),
        attempt_id=_identity("decoder-attempt-", "attempt-b"),
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
    ("overrides", "reason"),
    [
        ({"product_name": "RAD_NL25_RAC_RT"}, "unsupported:rtcor_profile"),
        ({"tag": "3.8"}, "unsupported:rtcor_profile"),
        ({"start": "02-AUG-2026;12:00:00.000"}, "unsupported:time_contract"),
        ({"rows": 764}, "unsupported:grid_contract"),
        ({"pixel_x": 1.1}, "unsupported:grid_contract"),
        ({"pixel_y": 1.0000049}, "unsupported:grid_contract"),
        ({"pixel_def": "CENTRE"}, "unsupported:grid_contract"),
        ({"projection": PROJECTION.replace("+lon_0=0", "+lon_0=1")}, "unsupported:crs_profile"),
        ({"corners": CORNERS + np.asarray([0, 0, 0, 0, 0.1, 0, 0, 0])}, "unsupported:grid_corners"),
        ({"precip_formula": "GEO=0.1*PV+0"}, "unsupported:calibration_contract"),
        ({"quality_formula": "GEO=0.01*PV+0"}, "unsupported:calibration_contract"),
        ({"adjustment_formula": "GEO=0.16*PV+0"}, "unsupported:calibration_contract"),
        ({"precip_missing": 0}, "unsupported:calibration_contract"),
        ({"quality_out": 254}, "unsupported:calibration_contract"),
        ({"image_parameter": "RATE_[MM/H]"}, "unsupported:layer_contract"),
        ({"image_count": 2}, "unsupported:rtcor_profile"),
        ({"compression": None}, "unsupported:layer_storage"),
        ({"compression": "lzf"}, "unsupported:hdf5_filter_pipeline"),
        ({"shuffle": True}, "unsupported:hdf5_filter_pipeline"),
        ({"fletcher32": True}, "unsupported:hdf5_filter_pipeline"),
        ({"chunk_shape": (EXPECTED_ROWS, EXPECTED_COLUMNS // 2)}, "unsupported:layer_storage"),
        ({"write_data": False}, "unsupported:hdf5_chunk_allocation"),
    ],
)
def test_semantic_calibration_grid_and_storage_drift_is_non_meteorological(
    tmp_path, overrides, reason,
):
    raw_store, capture = _capture(tmp_path, hdf=_hdf_bytes(**overrides))
    result, artifacts, _evidence = _decode(tmp_path, raw_store, capture)
    assert result.outcome == "unsupported"
    assert result.reason_code == reason
    assert result.artifact is None
    assert result.evidence_receipt.run.non_operational_candidate_artifact is None
    assert result.evidence_receipt.run.quantity is None
    assert result.evidence_receipt.run.unit is None
    assert result.evidence_receipt.run.native_crs is None
    assert result.evidence_receipt.run.native_grid is None
    assert artifacts.object_count() == 0


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"include_quality": False}, "incomplete:hdf5_structure"),
        ({"include_adjustment": False}, "incomplete:hdf5_structure"),
        ({"shape": (700, 765)}, "unsupported:layer_storage"),
        ({"precip_dtype": ">u2"}, "unsupported:layer_storage"),
        ({"aux_dtype": "<u2"}, "unsupported:layer_storage"),
    ],
)
def test_required_layers_shape_and_exact_dtypes_fail_closed(tmp_path, overrides, reason):
    raw_store, capture = _capture(tmp_path, hdf=_hdf_bytes(**overrides))
    result, artifacts, _evidence = _decode(tmp_path, raw_store, capture)
    assert result.reason_code == reason
    assert result.artifact is None
    assert artifacts.object_count() == 0


@pytest.mark.parametrize(
    ("hdf", "reason"),
    [
        (
            _mutated_hdf(
                lambda root: root.__setitem__("soft", h5py.SoftLink("/image1"))
            ),
            "unsupported:hdf5_links",
        ),
        (
            _mutated_hdf(
                lambda root: root.create_dataset(
                    "external_storage",
                    shape=(1,),
                    dtype="u1",
                    external=[("outside.bin", 0, 1)],
                )
            ),
            "unsupported:hdf5_storage",
        ),
    ],
    ids=("soft-link", "external-dataset-storage"),
)
def test_hdf_resource_indirection_is_rejected_again_at_decode_boundary(
    tmp_path, hdf, reason,
):
    raw_store, capture = _capture(tmp_path, hdf=hdf)
    result, artifacts, _evidence = _decode(tmp_path, raw_store, capture)
    assert result.reason_code == reason
    assert result.artifact is None
    assert artifacts.object_count() == 0


@pytest.mark.parametrize(
    ("hdf", "reason"),
    [
        (
            _mutated_hdf(_skip_quality_gzip_filter),
            "unsupported:hdf5_filter_pipeline",
        ),
        (
            _mutated_hdf(_alias_adjustment_to_quality_dataset),
            "unsupported:hdf5_dataset_alias",
        ),
    ],
    ids=("skipped-gzip-filter-mask", "aliased-image2-image3-dataset"),
)
def test_chunk_filter_mask_and_dataset_object_aliases_are_rejected(
    tmp_path, hdf, reason,
):
    raw_store, capture = _capture(tmp_path, hdf=hdf)
    result, artifacts, _evidence = _decode(tmp_path, raw_store, capture)
    assert result.outcome == "unsupported"
    assert result.reason_code == reason
    assert result.artifact is None
    assert result.evidence_receipt.run.quantity is None
    assert artifacts.object_count() == 0


def test_corrupt_hdf_is_decode_error_without_artifact_or_meteorology(tmp_path):
    raw_store, capture = _capture(tmp_path, hdf=b"not-hdf5")
    result, artifacts, _evidence = _decode(tmp_path, raw_store, capture)
    assert result.outcome == "decode_error"
    assert result.reason_code == "decode_error:hdf5_container"
    assert result.artifact is None
    assert result.evidence_receipt.run.quantity is None
    assert artifacts.object_count() == 0


@pytest.mark.parametrize(
    ("case_name", "hdf"),
    [
        pytest.param(case_name, payload, id=case_name)
        for case_name, payload in _truncated_hdf_corpus()
    ],
)
def test_bounded_deterministic_corruption_corpus_is_payload_free_decode_error(
    tmp_path, case_name, hdf,
):
    assert case_name.startswith("truncated-at-")
    assert 0 < len(hdf) <= 64 * 1024
    raw_store, capture = _capture(tmp_path, hdf=hdf)
    result, artifacts, _evidence = _decode(tmp_path, raw_store, capture)
    assert result.outcome == "decode_error"
    assert result.reason_code == "decode_error:hdf5_container"
    assert result.artifact is None
    run = result.evidence_receipt.run
    assert run.non_operational_candidate_artifact is None
    assert run.quantity is None
    assert run.unit is None
    assert run.native_crs is None
    assert run.native_grid is None
    assert run.coverage is None
    assert artifacts.object_count() == 0


def test_corrupt_allocated_compressed_chunk_is_normalized_before_evidence(tmp_path):
    raw_store, capture = _capture(
        tmp_path,
        hdf=_mutated_hdf(_corrupt_precipitation_chunk),
    )
    result, artifacts, _evidence = _decode(tmp_path, raw_store, capture)
    assert result.outcome == "decode_error"
    assert result.reason_code == "decode_error:raster_read"
    assert result.artifact is None
    assert result.evidence_receipt.run.quantity is None
    assert result.evidence_receipt.run.coverage is None
    assert artifacts.object_count() == 0


@pytest.mark.parametrize(
    ("helper_name", "error_type"),
    [
        ("_validate_self_contained", RuntimeError),
        ("_hard_group", KeyError),
        ("_validate_geography", ValueError),
        ("_dataset_object_identity", TypeError),
        ("_validate_filter_pipeline_and_allocation", IndexError),
        ("_decode_layer", OverflowError),
        ("_validate_self_contained", AttributeError),
        ("_validate_geography", BufferError),
        ("_decode_layer", struct.error),
    ],
)
def test_h5py_link_group_attribute_and_dataspace_failures_normalize_to_decode_error(
    tmp_path, monkeypatch, helper_name, error_type,
):
    def fail_from_corrupt_container(*_args, **_kwargs):
        raise error_type("synthetic deterministic h5py corruption")

    monkeypatch.setattr(decoder_module, helper_name, fail_from_corrupt_container)
    raw_store, capture = _capture(tmp_path)
    result, artifacts, _evidence = _decode(tmp_path, raw_store, capture)
    assert result.outcome == "decode_error"
    assert result.reason_code == "decode_error:hdf5_container"
    assert result.artifact is None
    assert result.evidence_receipt.run.quantity is None
    assert result.evidence_receipt.run.unit is None
    assert result.evidence_receipt.run.coverage is None
    assert artifacts.object_count() == 0


def test_extra_capture_role_commits_payload_free_incomplete_receipt(tmp_path):
    raw_store, capture = _capture(tmp_path, extra_role=True)
    result, artifacts, _evidence = _decode(tmp_path, raw_store, capture)
    assert result.outcome == "incomplete"
    assert result.reason_code == "incomplete:capture_roles"
    assert result.artifact is None
    assert result.evidence_receipt.run.coverage is None
    assert artifacts.object_count() == 0


def test_raw_hash_tamper_stops_before_artifact_and_canonical_evidence(tmp_path):
    raw_store, capture = _capture(tmp_path)
    receipt = raw_store.load_receipt(capture.members[0].receipt_id)
    assert receipt is not None
    path = Path(receipt.artifact_path)
    payload = path.read_bytes()
    path.write_bytes(bytes([payload[0] ^ 1]) + payload[1:])
    artifacts, evidence = _stores(tmp_path, raw_store)
    with pytest.raises(RawArtifactIntegrityError):
        decode_knmi_rtcor_capture(
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


def test_publish_fault_cleans_stage_and_prevents_decoded_complete(tmp_path):
    raw_store, capture = _capture(tmp_path)

    def fail(checkpoint: str) -> None:
        if checkpoint == "publish.after_stage":
            raise OSError("injected publish failure")

    artifacts = KnmiRtcorDerivedArtifactStore(
        (tmp_path / "derived").resolve(), fault_injector=fail
    )
    evidence = CanonicalEvidenceStore(
        (tmp_path / "canonical.sqlite3").resolve(),
        raw_artifact_reader=raw_store,
        clock=lambda: EVALUATED_AT + 1,
    )
    with pytest.raises(OSError, match="injected"):
        decode_knmi_rtcor_capture(
            capture_id=capture.capture_id,
            decoder_job_id=_identity("decoder-job-", "publish-job"),
            attempt_id=_identity("decoder-attempt-", "publish-attempt"),
            raw_store=raw_store,
            artifact_store=artifacts,
            evidence_store=evidence,
            evaluated_at=EVALUATED_AT,
        )
    assert artifacts.object_count() == 0
    assert list(artifacts.staging_path.iterdir()) == []
    assert evidence.stats()["decoder_runs"] == 0


def test_artifact_capacity_and_corruption_fail_closed(tmp_path):
    store = KnmiRtcorDerivedArtifactStore(
        (tmp_path / "derived").resolve(), max_total_bytes=32 * 1024 * 1024
    )
    filler = store.objects_path / f"{('0' * 64)}.krtq"
    with filler.open("wb") as stream:
        stream.truncate(32 * 1024 * 1024)
    with pytest.raises(KnmiRtcorArtifactCapacityError):
        store.publish(b"x")

    raw_store, capture = _capture(tmp_path / "other")
    result, artifacts, _evidence = _decode(tmp_path / "other", raw_store, capture)
    assert result.artifact is not None
    path = artifacts.objects_path / f"{result.artifact.sha256}.krtq"
    payload = path.read_bytes()
    path.write_bytes(payload[:-1] + bytes([payload[-1] ^ 1]))
    with pytest.raises(KnmiRtcorArtifactError, match="checksum"):
        artifacts.read_decoded(result.artifact)


def test_derived_store_bounds_object_and_staging_entry_counts(tmp_path):
    object_store = KnmiRtcorDerivedArtifactStore(
        (tmp_path / "objects-bounded").resolve(),
        max_objects=1,
    )
    first = object_store.publish(b"first")
    assert object_store.publish(b"first") == first
    with pytest.raises(KnmiRtcorArtifactCapacityError, match="object count"):
        object_store.publish(b"second")

    staging_store = KnmiRtcorDerivedArtifactStore(
        (tmp_path / "staging-bounded").resolve(),
        max_staging_objects=1,
    )
    orphan = staging_store.staging_path / f".{('0' * 64)}.orphan"
    orphan.write_bytes(b"orphan")
    with pytest.raises(KnmiRtcorArtifactCapacityError, match="staging count"):
        staging_store.publish(b"new")


def test_derived_store_unknown_layout_entry_fails_closed(tmp_path):
    store = KnmiRtcorDerivedArtifactStore((tmp_path / "derived-layout").resolve())
    first = store.publish(b"first")
    (store.objects_path / "unbounded-junk").write_bytes(b"junk")
    with pytest.raises(KnmiRtcorArtifactError, match="unknown object entry"):
        store.publish(b"new")
    with pytest.raises(KnmiRtcorArtifactError, match="unknown object entry"):
        store.publish(b"first")
    assert first.size_bytes == len(b"first")


def test_derived_store_profile_binds_limits_to_the_root(tmp_path):
    root = (tmp_path / "profile-bound").resolve()
    store = KnmiRtcorDerivedArtifactStore(
        root,
        max_objects=2,
        max_staging_objects=3,
    )
    profile = json.loads(store.profile_path.read_text(encoding="utf-8"))

    assert profile["schema"] == "stv3-worker-knmi-rtcor-derived-store-profile-v1"
    assert profile["filename_suffix"] == ".krtq"
    assert profile["limits"]["max_objects"] == 2
    assert profile["limits"]["max_staging_objects"] == 3
    assert KnmiRtcorDerivedArtifactStore(
        root,
        max_objects=2,
        max_staging_objects=3,
    ).object_count() == 0
    with pytest.raises(KnmiRtcorArtifactError, match="immutable limits"):
        KnmiRtcorDerivedArtifactStore(
            root,
            max_objects=3,
            max_staging_objects=3,
        )


def test_derived_store_profile_creation_respects_existing_staging_bound(tmp_path):
    root = (tmp_path / "profile-staging-bound").resolve()
    (root / "objects").mkdir(parents=True)
    (root / "staging").mkdir()
    (root / "staging" / f".{('0' * 64)}.orphan").write_bytes(b"orphan")

    with pytest.raises(KnmiRtcorArtifactCapacityError, match="profile publication"):
        KnmiRtcorDerivedArtifactStore(root, max_staging_objects=1)

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
        KnmiRtcorDerivedArtifactStore(root, fault_injector=fail)

    profile_path = root / ".store-profile.json"
    assert profile_path.exists() is profile_survives
    assert list((root / "staging").iterdir()) == []
    reopened = KnmiRtcorDerivedArtifactStore(root)
    assert reopened.object_count() == 0


def test_parallel_capacity_accounting_and_publish_are_atomic_under_store_lock(tmp_path):
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

    total_bound = 32 * 1024 * 1024
    first_store = KnmiRtcorDerivedArtifactStore(
        root,
        max_total_bytes=total_bound,
        fault_injector=first_fault,
    )
    second_store = KnmiRtcorDerivedArtifactStore(
        root,
        max_total_bytes=total_bound,
        fault_injector=second_fault,
    )
    payload_size = 17 * 1024 * 1024
    outcomes: dict[str, str] = {}

    def publish(label: str, store, payload: bytes) -> None:
        try:
            store.publish(payload)
        except KnmiRtcorArtifactCapacityError:
            outcomes[label] = "capacity"
        except Exception as exc:  # pragma: no cover - retained as assertion evidence
            outcomes[label] = f"unexpected:{type(exc).__name__}:{exc}"
        else:
            outcomes[label] = "published"

    first = threading.Thread(
        target=publish,
        args=("first", first_store, b"a" * payload_size),
    )
    second = threading.Thread(
        target=publish,
        args=("second", second_store, b"b" * payload_size),
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
    assert first_store._stored_bytes() == payload_size
    assert first_store._stored_bytes() <= total_bound
    assert list(first_store.staging_path.iterdir()) == []


@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (
            lambda metadata: metadata["grid"].__setitem__(
                "row_direction", "south_to_north"
            ),
            "grid contract",
        ),
        (
            lambda metadata: metadata["source"].__setitem__(
                "hdf5_upstream_id", "knmi/forged"
            ),
            "source identity",
        ),
        (
            lambda metadata: metadata["source"].__setitem__(
                "hdf5_received_at", PRODUCT_END.timestamp() - 1.0
            ),
            "source binding",
        ),
        (
            lambda metadata: metadata["payload"].__setitem__(
                "operational_admission", True
            ),
            "compressed metadata",
        ),
        (
            lambda metadata: metadata["layers"]["adjustment"].__setitem__(
                "may_reapply_to_image1", True
            ),
            "decoder contract",
        ),
    ],
)
def test_rehashed_forged_artifact_metadata_is_not_trusted(tmp_path, mutator, message):
    raw_store, capture = _capture(tmp_path)
    result, artifacts, _evidence = _decode(tmp_path, raw_store, capture)
    assert result.artifact is not None
    payload = artifacts.read(result.artifact)
    prefix_size = len(decoder_module.ARTIFACT_MAGIC) + 4
    header_size = struct.unpack(
        ">I", payload[len(decoder_module.ARTIFACT_MAGIC):prefix_size]
    )[0]
    metadata = json.loads(
        payload[prefix_size:prefix_size + header_size].decode("utf-8")
    )
    mutator(metadata)
    forged_header = decoder_module._canonical_json(metadata)
    forged = (
        decoder_module.ARTIFACT_MAGIC
        + struct.pack(">I", len(forged_header))
        + forged_header
        + payload[prefix_size + header_size:]
    )
    with pytest.raises(KnmiRtcorArtifactError, match=message):
        decoder_module._parse_artifact(forged)


def test_missing_capture_and_invalid_id_create_nothing(tmp_path):
    raw_store = RawArtifactStore((tmp_path / "raw").resolve())
    raw_store.initialize()
    artifacts, evidence = _stores(tmp_path, raw_store)
    with pytest.raises(KnmiRtcorCaptureMissingError):
        decode_knmi_rtcor_capture(
            capture_id="capture-" + "0" * 64,
            decoder_job_id=_identity("decoder-job-", "missing"),
            attempt_id=_identity("decoder-attempt-", "missing"),
            raw_store=raw_store,
            artifact_store=artifacts,
            evidence_store=evidence,
            evaluated_at=EVALUATED_AT,
        )
    with pytest.raises(ValueError, match="stable"):
        decode_knmi_rtcor_capture(
            capture_id="capture-" + "0" * 64,
            decoder_job_id="unstable",
            attempt_id=_identity("decoder-attempt-", "missing"),
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

    with pytest.raises(KnmiRtcorDecodeDeadlineError, match="decode_start"):
        decode_knmi_rtcor_capture(
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
    with pytest.raises(KnmiRtcorDecodeDeadlineError, match="raw_capture_loaded"):
        decode_knmi_rtcor_capture(
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
socket.create_connection = forbidden('network')
socket.socket.connect = forbidden('socket')
sqlite3.connect = forbidden('sqlite')
threading.Thread.start = forbidden('thread')
sys.path.insert(0, repo_root)
import storm_tracker_worker.knmi_rtcor_quantitative as decoder
after_paths = sorted(str(path.relative_to(workdir)) for path in workdir.rglob('*'))
after_threads = {{(thread.name, thread.ident) for thread in threading.enumerate()}}
assert before_paths == after_paths
assert before_threads == after_threads
assert callable(decoder.decode_knmi_rtcor_capture)
forbidden_modules = {{
    'storm_tracker_worker.service',
    'storm_tracker_worker.radar',
    'storm_tracker_worker.provider_capture_registry',
}}
assert forbidden_modules.isdisjoint(sys.modules)
print('knmi-rtcor-import-inert')
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
    assert completed.stdout.strip() == "knmi-rtcor-import-inert"
