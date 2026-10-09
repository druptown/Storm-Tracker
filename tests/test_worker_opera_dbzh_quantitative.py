from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
from types import SimpleNamespace
import time

import h5py
import numpy as np
from pyproj import CRS, Transformer
import pytest

from storm_tracker_worker import opera_native as native
from storm_tracker_worker import opera_dbzh_quantitative as quantitative
from storm_tracker_worker.opera_dbzh_quantitative import (
    OperaDbzhArtifactCapacityError,
    OperaDbzhArtifactError,
    OperaDbzhDecodeDeadlineError,
    OperaDbzhDerivedArtifactStore,
    OperaDbzhRawAuthority,
    OperaWgs84Region,
    decode_opera_dbzh_region,
    open_opera_evidence_store,
)
from storm_tracker_worker.raw_artifacts import (
    RawArtifactIntegrityError,
    RawArtifactStore,
)


PRODUCT_TIMESTAMP = datetime(
    2026, 8, 2, 12, 0, tzinfo=timezone.utc
).timestamp()
RECEIVED_AT = PRODUCT_TIMESTAMP + 5.0
EVALUATED_AT = RECEIVED_AT + 60.0
ROWS = 8
COLUMNS = 10
CHUNK_ROWS = 4
CHUNK_COLUMNS = 5
JOB_ID = "opera-dbzh-20260802T1200"
OBJECT_KEY = "2026/08/02/OPERA/COMP/OPERA@20260802T1200@0@DBZH.h5"


def _identity(prefix: str, text: str) -> str:
    return prefix + hashlib.sha256(text.encode("ascii")).hexdigest()


def _bytes(text: str) -> np.bytes_:
    return np.bytes_(text.encode("ascii"))


def _small_grid(monkeypatch) -> None:
    monkeypatch.setattr(native, "OPERA_GRID_ROWS", ROWS)
    monkeypatch.setattr(native, "OPERA_GRID_COLUMNS", COLUMNS)
    monkeypatch.setattr(native, "OPERA_CHUNK_ROWS", CHUNK_ROWS)
    monkeypatch.setattr(native, "OPERA_CHUNK_COLUMNS", CHUNK_COLUMNS)


def _source_arrays(
    *,
    qi_override: tuple[int, int, float] | None = None,
    dbzh_override: tuple[int, int, float] | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    dbzh = np.full(
        (ROWS, COLUMNS), native.OPERA_DBZH_NODATA, dtype=np.dtype("<f8")
    )
    qi = np.full(
        (ROWS, COLUMNS), native.OPERA_QUALITY_NODATA, dtype=np.dtype("<f8")
    )
    dbzh[3, 4] = native.OPERA_DBZH_UNDETECT
    qi[3, 4] = 0.0
    dbzh[4, 5] = 0.0
    qi[4, 5] = 0.0
    dbzh[4, 6] = 35.0
    qi[4, 6] = 1.0
    dbzh[5, 5] = -10.0
    qi[5, 5] = 0.5
    if qi_override is not None:
        qi[qi_override[0], qi_override[1]] = qi_override[2]
    if dbzh_override is not None:
        dbzh[dbzh_override[0], dbzh_override[1]] = dbzh_override[2]
    return dbzh, qi


def _hdf5(
    *,
    product_timestamp: float = PRODUCT_TIMESTAMP,
    qi_override: tuple[int, int, float] | None = None,
    dbzh_override: tuple[int, int, float] | None = None,
) -> bytes:
    dbzh_values, qi_values = _source_arrays(
        qi_override=qi_override, dbzh_override=dbzh_override
    )
    moment = datetime.fromtimestamp(product_timestamp, tz=timezone.utc)
    start = datetime.fromtimestamp(product_timestamp - 599.0, tz=timezone.utc)
    output = io.BytesIO()
    with h5py.File(output, "w") as root:
        root.attrs["Conventions"] = _bytes(native.OPERA_ODIM_CONVENTIONS)
        what = root.create_group("what")
        what.attrs["object"] = _bytes("COMP")
        what.attrs["version"] = _bytes(native.OPERA_H5RAD_VERSION)
        what.attrs["source"] = _bytes(native.OPERA_SOURCE)
        what.attrs["date"] = _bytes(moment.strftime("%Y%m%d"))
        what.attrs["time"] = _bytes(moment.strftime("%H%M%S"))

        how = root.create_group("how")
        for name, value in native.OPERA_HOW_FIXED_TEXT.items():
            how.attrs[name] = _bytes(value)
        how.attrs["nodes"] = _bytes("'behel', 'bejab'")

        where = root.create_group("where")
        where.attrs["xsize"] = COLUMNS
        where.attrs["ysize"] = ROWS
        where.attrs["xscale"] = native.OPERA_GRID_SCALE_METRES
        where.attrs["yscale"] = native.OPERA_GRID_SCALE_METRES
        where.attrs["projdef"] = _bytes(native.OPERA_PROJDEF)
        for name, value in native.OPERA_CORNERS.items():
            where.attrs[name] = value

        dataset_what = root.create_group("dataset1/what")
        dataset_what.attrs["product"] = _bytes("MAX")
        dataset_what.attrs["prodname"] = _bytes(native.OPERA_PRODUCT_NAME)
        dataset_what.attrs["startdate"] = _bytes(start.strftime("%Y%m%d"))
        dataset_what.attrs["starttime"] = _bytes(start.strftime("%H%M%S"))
        dataset_what.attrs["enddate"] = _bytes(moment.strftime("%Y%m%d"))
        dataset_what.attrs["endtime"] = _bytes(moment.strftime("%H%M%S"))

        data1 = root.create_group("dataset1/data1")
        data_what = data1.create_group("what")
        data_what.attrs["quantity"] = _bytes(native.OPERA_PRODUCT_CODE)
        data_what.attrs["gain"] = native.OPERA_DBZH_GAIN
        data_what.attrs["offset"] = native.OPERA_DBZH_OFFSET
        data_what.attrs["nodata"] = native.OPERA_DBZH_NODATA
        data_what.attrs["undetect"] = native.OPERA_DBZH_UNDETECT
        dbzh = data1.create_dataset(
            "data",
            data=dbzh_values,
            chunks=(CHUNK_ROWS, CHUNK_COLUMNS),
            compression="gzip",
            compression_opts=9,
        )
        dbzh.attrs["CLASS"] = _bytes(native.OPERA_IMAGE_CLASS)
        dbzh.attrs["IMAGE_VERSION"] = _bytes(native.OPERA_IMAGE_VERSION)

        quality1 = data1.create_group("quality1")
        quality_what = quality1.create_group("what")
        quality_what.attrs["gain"] = native.OPERA_QUALITY_GAIN
        quality_what.attrs["offset"] = native.OPERA_QUALITY_OFFSET
        quality_what.attrs["nodata"] = native.OPERA_QUALITY_NODATA
        quality_how = quality1.create_group("how")
        quality_how.attrs["task"] = _bytes(native.OPERA_QUALITY_TASK)
        qi = quality1.create_dataset(
            "data",
            data=qi_values,
            chunks=(CHUNK_ROWS, CHUNK_COLUMNS),
            compression="gzip",
            compression_opts=9,
        )
        qi.attrs["CLASS"] = _bytes(native.OPERA_IMAGE_CLASS)
        qi.attrs["IMAGE_VERSION"] = _bytes(native.OPERA_IMAGE_VERSION)
    return output.getvalue()


def _capture(
    monkeypatch,
    tmp_path: Path,
    *,
    payload: bytes | None = None,
    product_timestamp: float = PRODUCT_TIMESTAMP,
    raw_only: bool = False,
):
    exact = _hdf5(product_timestamp=product_timestamp) if payload is None else payload
    revision = native.OperaRevisionIdentity(
        etag='"ABCDEFABCDEFABCDEFABCDEFABCDEFAB"',
        version_id="fixture-v1",
        content_length=len(exact),
        content_type="application/octet-stream",
    )
    counter = 0

    def head(object_key: str, *, deadline: float):
        assert object_key == (
            f"{datetime.fromtimestamp(product_timestamp, tz=timezone.utc):%Y/%m/%d}"
            "/OPERA/COMP/"
            f"OPERA@{datetime.fromtimestamp(product_timestamp, tz=timezone.utc):%Y%m%dT%H%M}"
            "@0@DBZH.h5"
        )
        assert deadline > time.monotonic()
        return revision

    def download(
        object_key: str,
        observed,
        *,
        deadline: float,
        destination_path: str | Path | None = None,
    ):
        nonlocal counter
        assert observed == revision
        counter += 1
        path = (
            Path(destination_path)
            if destination_path is not None
            else tmp_path / f"opera-transport-{counter}.part"
        )
        path.write_bytes(exact)
        return native.OperaDownloadedFile(
            path=str(path),
            size_bytes=len(exact),
            content_sha256=hashlib.sha256(exact).hexdigest(),
            content_type=revision.content_type,
            received_at=product_timestamp + 5.0,
            etag=revision.etag,
            version_id=revision.version_id,
        )

    monkeypatch.setattr(native, "_head_hdf5", head)
    monkeypatch.setattr(native, "_download_hdf5", download)
    moment = datetime.fromtimestamp(product_timestamp, tz=timezone.utc)
    job_id = f"opera-dbzh-{moment:%Y%m%dT%H%M}"
    raw = RawArtifactStore((tmp_path / "raw").resolve())
    capture = (
        native.capture_opera_dbzh_hdf5_raw_only
        if raw_only
        else native.capture_opera_dbzh_hdf5
    )
    capture_kwargs = {"raw_store": raw}
    if raw_only:
        capture_kwargs["raw_identity"] = (
            "acquisition:" + hashlib.sha256(job_id.encode("utf-8")).hexdigest()
        )
    result = capture(
        SimpleNamespace(
            job_id=job_id,
            payload={"product_timestamp": product_timestamp},
        ),
        **capture_kwargs,
    )
    return raw, result["raw_provenance"]["raw_capture_id"], result


def _region_for_projected_box(
    minimum_x: float = 4_000.0,
    minimum_y: float = -5_000.0,
    maximum_x: float = 6_000.0,
    maximum_y: float = -3_000.0,
) -> OperaWgs84Region:
    inverse = Transformer.from_crs(
        CRS.from_proj4(native.OPERA_PROJDEF), CRS.from_epsg(4326), always_xy=True
    )
    points = [
        inverse.transform(x, y)
        for x, y in (
            (minimum_x, minimum_y),
            (minimum_x, maximum_y),
            (maximum_x, minimum_y),
            (maximum_x, maximum_y),
        )
    ]
    longitudes = [point[0] for point in points]
    latitudes = [point[1] for point in points]
    padding = 1.0e-6
    return OperaWgs84Region(
        minimum_latitude=min(latitudes) - padding,
        minimum_longitude=min(longitudes) - padding,
        maximum_latitude=max(latitudes) + padding,
        maximum_longitude=max(longitudes) + padding,
    )


def _stores(tmp_path: Path, raw: RawArtifactStore, *, suffix: str = ""):
    authority = OperaDbzhRawAuthority(raw)
    artifacts = OperaDbzhDerivedArtifactStore(
        (tmp_path / f"derived{suffix}").resolve()
    )
    evidence = open_opera_evidence_store(
        (tmp_path / f"canonical{suffix}.sqlite3").resolve(),
        raw_authority=authority,
        clock=lambda: EVALUATED_AT + 1.0,
    )
    return authority, artifacts, evidence


def _decode(
    capture_id: str,
    *,
    authority: OperaDbzhRawAuthority,
    artifacts: OperaDbzhDerivedArtifactStore,
    evidence,
    suffix: str = "a",
    region: OperaWgs84Region | None = None,
):
    return decode_opera_dbzh_region(
        capture_id=capture_id,
        decoder_job_id=_identity("decoder-job-", f"job-{suffix}"),
        attempt_id=_identity("decoder-attempt-", f"attempt-{suffix}"),
        region=region or _region_for_projected_box(),
        raw_authority=authority,
        artifact_store=artifacts,
        evidence_store=evidence,
        evaluated_at=EVALUATED_AT,
    )


def test_valid_decode_preserves_float64_dbzh_qi_and_explicit_masks(
    monkeypatch, tmp_path
):
    _small_grid(monkeypatch)
    raw, capture_id, _capture_result = _capture(monkeypatch, tmp_path)
    authority, artifacts, evidence = _stores(tmp_path, raw)
    result = _decode(
        capture_id, authority=authority, artifacts=artifacts, evidence=evidence
    )
    assert result.outcome == "decoded_complete", result.reason_code
    assert result.artifact is not None
    decoded = artifacts.read_decoded(result.artifact, checkpoint=lambda _stage: None)
    window = decoded.metadata["grid"]
    row_start = window["row_start"]
    column_start = window["column_start"]
    assert decoded.dbzh_raw.dtype.str == "<f8"
    assert decoded.qi_raw.dtype.str == "<f8"
    local_undetect = (3 - row_start, 4 - column_start)
    local_zero = (4 - row_start, 5 - column_start)
    local_detected = (4 - row_start, 6 - column_start)
    assert decoded.dbzh_undetect_mask[local_undetect] == 1
    assert decoded.dbzh_detected_mask[local_zero] == 1
    assert decoded.detected_dbz[local_zero] == 0.0
    assert decoded.detected_dbz[local_detected] == 35.0
    assert decoded.qi_raw[local_zero] == 0.0
    assert decoded.qi_valid_mask[local_zero] == 1
    assert decoded.qi_missing_mask[local_zero] == 0
    assert decoded.metadata["product"]["upstream_advected_contributions"] is True
    assert decoded.metadata["product"]["motion_truth_eligible"] is False
    assert decoded.metadata["product"]["rain_rate_conversion_applied"] is False
    assert decoded.metadata["product"]["operational_admission_eligible"] is False
    assert result.evidence_receipt.run.quantity == (
        "maximum_horizontal_reflectivity_factor"
    )
    assert result.evidence_receipt.run.unit == "dBZ"
    external = json.dumps(decoded.metadata, sort_keys=True).lower()
    assert "mm/h" not in external
    assert "weather_state" not in external
    assert authority.raw_verification_count == 1


def test_raw_only_capture_is_certified_and_decoded_without_rewriting_raw_metadata(
    monkeypatch, tmp_path
):
    _small_grid(monkeypatch)
    raw, capture_id, _ = _capture(monkeypatch, tmp_path, raw_only=True)
    authority, artifacts, evidence = _stores(tmp_path, raw, suffix="-raw-only")

    result = _decode(
        capture_id,
        authority=authority,
        artifacts=artifacts,
        evidence=evidence,
        suffix="raw-only",
    )

    assert result.outcome == "decoded_complete", result.reason_code
    assert result.artifact is not None
    capture = raw.load_capture(capture_id)
    assert capture is not None and capture.product_timestamp is None
    decoded = artifacts.read_decoded(result.artifact, checkpoint=lambda _stage: None)
    assert decoded.metadata["capture"]["product_timestamp"] is None
    assert decoded.metadata["product"]["nominal_timestamp"] == PRODUCT_TIMESTAMP
    assert result.evidence_receipt.run.product_time == PRODUCT_TIMESTAMP
    assert result.evidence_receipt.run.capture_id != capture_id


def test_qi_missing_and_dbzh_detected_are_independent(monkeypatch, tmp_path):
    _small_grid(monkeypatch)
    payload = _hdf5(qi_override=(4, 6, native.OPERA_QUALITY_NODATA))
    raw, capture_id, _ = _capture(monkeypatch, tmp_path, payload=payload)
    authority, artifacts, evidence = _stores(tmp_path, raw)
    result = _decode(
        capture_id, authority=authority, artifacts=artifacts, evidence=evidence
    )
    decoded = artifacts.read_decoded(result.artifact, checkpoint=lambda _stage: None)
    row = 4 - decoded.metadata["grid"]["row_start"]
    column = 6 - decoded.metadata["grid"]["column_start"]
    assert decoded.dbzh_detected_mask[row, column] == 1
    assert decoded.qi_missing_mask[row, column] == 1
    assert decoded.qi_valid_mask[row, column] == 0


def test_bbox_is_densified_guarded_and_outward_rounded(monkeypatch):
    _small_grid(monkeypatch)
    window = quantitative._window_for_region(
        _region_for_projected_box(), checkpoint=lambda _stage: None
    )
    assert window.densified_point_count == 4 * (
        quantitative.BBOX_DENSIFY_SEGMENTS_PER_EDGE + 1
    )
    assert window.row_start <= 1
    assert window.row_stop >= 7
    assert window.column_start <= 2
    assert window.column_stop >= 8
    assert window.rows <= quantitative.MAX_REGION_ROWS
    assert window.columns <= quantitative.MAX_REGION_COLUMNS


@pytest.mark.parametrize(
    "region",
    [
        OperaWgs84Region(0.0, 170.0, 1.0, -170.0),
        OperaWgs84Region(-91.0, 0.0, -90.0, 1.0),
        OperaWgs84Region(1.0, 1.0, 1.0, 2.0),
        OperaWgs84Region(45.0, -5.0, 60.0, 30.0),
    ],
)
def test_invalid_or_oversized_regions_fail_before_raw_access(tmp_path, region):
    raw = RawArtifactStore((tmp_path / "raw").resolve())
    authority, artifacts, evidence = _stores(tmp_path, raw)
    with pytest.raises(ValueError):
        _decode(
            "capture-" + "0" * 64,
            authority=authority,
            artifacts=artifacts,
            evidence=evidence,
            region=region,
        )
    assert authority.raw_verification_count == 0


def test_exact_retry_uses_one_raw_verification_and_no_second_hdf_inspection(
    monkeypatch, tmp_path
):
    _small_grid(monkeypatch)
    raw, capture_id, _ = _capture(monkeypatch, tmp_path)
    authority, artifacts, evidence = _stores(tmp_path, raw)
    real_decode = quantitative._decode_hdf_region
    inspections = 0

    def counted(*args, **kwargs):
        nonlocal inspections
        inspections += 1
        return real_decode(*args, **kwargs)

    monkeypatch.setattr(quantitative, "_decode_hdf_region", counted)
    first = _decode(
        capture_id, authority=authority, artifacts=artifacts, evidence=evidence
    )
    second = _decode(
        capture_id, authority=authority, artifacts=artifacts, evidence=evidence
    )
    assert first.artifact == second.artifact
    assert first.evidence_receipt.receipt_sha256 == second.evidence_receipt.receipt_sha256
    assert inspections == 1
    assert authority.raw_verification_count == 1


def test_decoder_and_canonical_evidence_share_exactly_one_raw_sha_pass(
    monkeypatch, tmp_path
):
    _small_grid(monkeypatch)
    raw, capture_id, _ = _capture(monkeypatch, tmp_path)
    authority, artifacts, evidence = _stores(tmp_path, raw)
    real_read_verified = RawArtifactStore._read_verified
    raw_reads = 0

    def counted(
        self,
        path,
        *,
        expected_size,
        expected_sha256,
        return_bytes,
        allow_staging=False,
    ):
        nonlocal raw_reads
        raw_reads += 1
        return real_read_verified(
            self,
            path,
            expected_size=expected_size,
            expected_sha256=expected_sha256,
            return_bytes=return_bytes,
            allow_staging=allow_staging,
        )

    monkeypatch.setattr(RawArtifactStore, "_read_verified", counted)
    _decode(
        capture_id, authority=authority, artifacts=artifacts, evidence=evidence
    )
    _decode(
        capture_id, authority=authority, artifacts=artifacts, evidence=evidence
    )
    assert raw_reads == 1
    assert authority.raw_verification_count == 1


def test_deterministic_invalid_qi_is_cached_across_reopened_job_session(
    monkeypatch, tmp_path
):
    _small_grid(monkeypatch)
    raw, capture_id, _ = _capture(
        monkeypatch, tmp_path, payload=_hdf5(qi_override=(4, 5, 1.5))
    )
    authority, artifacts, evidence = _stores(tmp_path, raw)
    real_decode = quantitative._decode_hdf_region
    inspections = 0

    def counted(*args, **kwargs):
        nonlocal inspections
        inspections += 1
        return real_decode(*args, **kwargs)

    monkeypatch.setattr(quantitative, "_decode_hdf_region", counted)
    first = _decode(
        capture_id, authority=authority, artifacts=artifacts, evidence=evidence
    )
    reopened_authority = OperaDbzhRawAuthority(raw)
    reopened_evidence = open_opera_evidence_store(
        evidence.path,
        raw_authority=reopened_authority,
        clock=lambda: EVALUATED_AT + 1.0,
    )
    second = _decode(
        capture_id,
        authority=reopened_authority,
        artifacts=artifacts,
        evidence=reopened_evidence,
    )
    assert first.outcome == second.outcome == "decode_error"
    assert first.reason_code == second.reason_code == "decode_error:qi_domain"
    assert first.artifact is second.artifact is None
    assert inspections == 1
    assert authority.raw_verification_count == 1
    assert reopened_authority.raw_verification_count == 1
    assert len(list(artifacts.verdicts_path.glob("*.json"))) == 1


@pytest.mark.parametrize(
    "error",
    [OperaDbzhDecodeDeadlineError("synthetic deadline"), OSError(5, "synthetic I/O")],
)
def test_deadline_and_io_failures_are_never_cached_or_committed(
    monkeypatch, tmp_path, error
):
    _small_grid(monkeypatch)
    raw, capture_id, _ = _capture(monkeypatch, tmp_path)
    authority, artifacts, evidence = _stores(tmp_path, raw)
    calls = 0

    def fail(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        raise type(error)(*error.args)

    monkeypatch.setattr(quantitative, "_decode_hdf_region", fail)
    for _ in range(2):
        with pytest.raises(type(error)):
            _decode(
                capture_id,
                authority=authority,
                artifacts=artifacts,
                evidence=evidence,
            )
    assert calls == 2
    assert evidence.stats()["decoder_runs"] == 0
    assert not artifacts.root.exists() or not list(artifacts.verdicts_path.glob("*.json"))


def test_expired_deadline_fails_before_raw_access(monkeypatch, tmp_path):
    _small_grid(monkeypatch)
    raw = RawArtifactStore((tmp_path / "raw").resolve())
    authority, artifacts, evidence = _stores(tmp_path, raw)
    with pytest.raises(OperaDbzhDecodeDeadlineError, match="decode_start"):
        decode_opera_dbzh_region(
            capture_id="capture-" + "0" * 64,
            decoder_job_id=_identity("decoder-job-", "expired"),
            attempt_id=_identity("decoder-attempt-", "expired"),
            region=_region_for_projected_box(),
            raw_authority=authority,
            artifact_store=artifacts,
            evidence_store=evidence,
            evaluated_at=EVALUATED_AT,
            monotonic_deadline=10.0,
            monotonic_clock=lambda: 10.0,
        )
    assert authority.raw_verification_count == 0
    assert evidence.stats()["decoder_runs"] == 0


def test_raw_corruption_produces_no_artifact_or_canonical_evidence(
    monkeypatch, tmp_path
):
    _small_grid(monkeypatch)
    raw, capture_id, capture_result = _capture(monkeypatch, tmp_path)
    receipt_id = capture_result["raw_provenance"]["raw_sources"]["hdf5"][
        "raw_receipt_id"
    ]
    receipt = raw.load_receipt(receipt_id)
    assert receipt is not None
    path = Path(receipt.artifact_path)
    changed = bytearray(path.read_bytes())
    changed[-1] ^= 1
    path.write_bytes(bytes(changed))
    authority, artifacts, evidence = _stores(tmp_path, raw)
    with pytest.raises(RawArtifactIntegrityError):
        _decode(
            capture_id, authority=authority, artifacts=artifacts, evidence=evidence
        )
    assert evidence.stats()["decoder_runs"] == 0
    assert not artifacts.root.exists()


def test_non_current_collector_is_payload_free_unsupported(monkeypatch, tmp_path):
    _small_grid(monkeypatch)
    raw, capture_id, _ = _capture(monkeypatch, tmp_path)
    authority, artifacts, evidence = _stores(tmp_path, raw)
    bundle = authority.load_capture_bundle_bytes(capture_id)
    assert bundle is not None
    provenance = bundle[1]["hdf5"].provenance
    provenance["collector_version"] = "obsolete-v2"
    assert authority._bundle is not None
    authority._bundle[1]["hdf5"] = replace(
        authority._bundle[1]["hdf5"],
        _provenance_json=json.dumps(provenance, separators=(",", ":"), sort_keys=True),
    )
    result = _decode(
        capture_id, authority=authority, artifacts=artifacts, evidence=evidence
    )
    assert result.outcome == "unsupported"
    assert result.reason_code == "unsupported:raw_provenance"
    assert result.artifact is None
    assert result.evidence_receipt.run.quantity is None
    assert not artifacts.root.exists()


def test_artifact_capacity_failure_leaves_no_evidence_or_verdict(monkeypatch, tmp_path):
    _small_grid(monkeypatch)
    raw, capture_id, _ = _capture(monkeypatch, tmp_path)
    authority = OperaDbzhRawAuthority(raw)
    artifacts = OperaDbzhDerivedArtifactStore(
        (tmp_path / "capacity-derived").resolve(),
        max_total_bytes=quantitative.MAX_ARTIFACT_BYTES,
    )
    artifacts.initialize()
    existing = artifacts.objects_path / (("0" * 64) + ".odqr")
    with existing.open("wb") as stream:
        stream.truncate(quantitative.MAX_ARTIFACT_BYTES)
    evidence = open_opera_evidence_store(
        (tmp_path / "capacity.sqlite3").resolve(),
        raw_authority=authority,
        clock=lambda: EVALUATED_AT + 1.0,
    )
    with pytest.raises(OperaDbzhArtifactCapacityError):
        _decode(
            capture_id, authority=authority, artifacts=artifacts, evidence=evidence
        )
    assert evidence.stats()["decoder_runs"] == 0
    assert not list(artifacts.verdicts_path.glob("*.json"))


def test_success_verdict_survives_canonical_commit_fault_without_redecode(
    monkeypatch, tmp_path
):
    _small_grid(monkeypatch)
    raw, capture_id, _ = _capture(monkeypatch, tmp_path)
    authority = OperaDbzhRawAuthority(raw)
    artifacts = OperaDbzhDerivedArtifactStore((tmp_path / "derived").resolve())

    def fault(stage: str):
        if stage == "commit.after_begin":
            raise RuntimeError("synthetic evidence fault")

    failing_evidence = open_opera_evidence_store(
        (tmp_path / "canonical.sqlite3").resolve(),
        raw_authority=authority,
        clock=lambda: EVALUATED_AT + 1.0,
        fault_injector=fault,
    )
    with pytest.raises(RuntimeError, match="synthetic evidence fault"):
        _decode(
            capture_id,
            authority=authority,
            artifacts=artifacts,
            evidence=failing_evidence,
        )
    assert len(list(artifacts.objects_path.glob("*.odqr"))) == 1
    assert len(list(artifacts.verdicts_path.glob("*.json"))) == 1

    reopened_authority = OperaDbzhRawAuthority(raw)
    evidence = open_opera_evidence_store(
        failing_evidence.path,
        raw_authority=reopened_authority,
        clock=lambda: EVALUATED_AT + 1.0,
    )
    monkeypatch.setattr(
        quantitative,
        "_decode_hdf_region",
        lambda *_args, **_kwargs: pytest.fail("retry repeated HDF inspection"),
    )
    result = _decode(
        capture_id,
        authority=reopened_authority,
        artifacts=artifacts,
        evidence=evidence,
    )
    assert result.outcome == "decoded_complete"
    assert evidence.stats()["decoder_runs"] == 1
    assert reopened_authority.raw_verification_count == 1


def test_evidence_store_must_share_the_single_read_authority(monkeypatch, tmp_path):
    _small_grid(monkeypatch)
    raw, capture_id, _ = _capture(monkeypatch, tmp_path)
    authority, artifacts, _evidence = _stores(tmp_path, raw)
    different_authority = OperaDbzhRawAuthority(raw)
    wrong_evidence = open_opera_evidence_store(
        (tmp_path / "wrong.sqlite3").resolve(),
        raw_authority=different_authority,
    )
    with pytest.raises(TypeError, match="single-read"):
        _decode(
            capture_id,
            authority=authority,
            artifacts=artifacts,
            evidence=wrong_evidence,
        )
    assert authority.raw_verification_count == 0


def test_derived_artifact_tamper_and_trailing_bytes_fail_closed(monkeypatch, tmp_path):
    _small_grid(monkeypatch)
    raw, capture_id, _ = _capture(monkeypatch, tmp_path)
    authority, artifacts, evidence = _stores(tmp_path, raw)
    result = _decode(
        capture_id, authority=authority, artifacts=artifacts, evidence=evidence
    )
    payload = artifacts.read_bytes(result.artifact, checkpoint=lambda _stage: None)
    with pytest.raises(OperaDbzhArtifactError, match="trailing"):
        quantitative._parse_artifact(
            payload + b"x", checkpoint=lambda _stage: None
        )
    object_path = artifacts.objects_path / f"{result.artifact.sha256}.odqr"
    changed = bytearray(object_path.read_bytes())
    changed[-1] ^= 1
    object_path.write_bytes(bytes(changed))
    with pytest.raises(OperaDbzhArtifactError, match="integrity"):
        artifacts.read_decoded(result.artifact, checkpoint=lambda _stage: None)


@pytest.mark.parametrize(
    "payload",
    [
        lambda: _hdf5(qi_override=(4, 5, float("nan"))),
        lambda: _hdf5(dbzh_override=(4, 5, float("inf"))),
    ],
)
def test_nonfinite_quantitative_values_are_deterministically_rejected(
    monkeypatch, tmp_path, payload
):
    _small_grid(monkeypatch)
    raw, capture_id, _ = _capture(monkeypatch, tmp_path, payload=payload())
    authority, artifacts, evidence = _stores(tmp_path, raw)
    result = _decode(
        capture_id, authority=authority, artifacts=artifacts, evidence=evidence
    )
    assert result.outcome == "decode_error"
    assert result.artifact is None
    assert len(list(artifacts.verdicts_path.glob("*.json"))) == 1


def test_missing_native_v3_verdict_is_incomplete_not_decoded(monkeypatch, tmp_path):
    _small_grid(monkeypatch)
    raw, capture_id, _ = _capture(monkeypatch, tmp_path)
    verdicts = list((raw.root / "opera_validation_verdicts").glob("*.json"))
    assert len(verdicts) == 1
    verdicts[0].unlink()
    authority, artifacts, evidence = _stores(tmp_path, raw)
    result = _decode(
        capture_id, authority=authority, artifacts=artifacts, evidence=evidence
    )
    assert result.outcome == "incomplete"
    assert result.reason_code == "incomplete:v3_validation_verdict"
    assert result.artifact is None
    assert evidence.stats()["decoder_runs"] == 1


def test_live_golden_replay_decodes_bounded_belgian_region(monkeypatch, tmp_path):
    golden = Path(
        ".analysis/opera-native-live-golden-20260802T0045/objects/"
        "80cb81b9c953a8fac9bcb8127a342aed0f93d0f203764caf8866fd0d244b471a.raw"
    )
    if not golden.exists():
        pytest.skip("read-only OPERA live golden is unavailable")
    payload = golden.read_bytes()
    golden_timestamp = datetime(
        2026, 8, 2, 0, 45, tzinfo=timezone.utc
    ).timestamp()
    raw, capture_id, _ = _capture(
        monkeypatch,
        tmp_path,
        payload=payload,
        product_timestamp=golden_timestamp,
    )
    authority = OperaDbzhRawAuthority(raw)
    artifacts = OperaDbzhDerivedArtifactStore((tmp_path / "golden-derived").resolve())
    evaluated = golden_timestamp + 3600.0
    evidence = open_opera_evidence_store(
        (tmp_path / "golden.sqlite3").resolve(),
        raw_authority=authority,
        clock=lambda: evaluated + 1.0,
    )
    result = decode_opera_dbzh_region(
        capture_id=capture_id,
        decoder_job_id=_identity("decoder-job-", "live-golden"),
        attempt_id=_identity("decoder-attempt-", "live-golden"),
        region=OperaWgs84Region(50.90, 4.25, 51.00, 4.40),
        raw_authority=authority,
        artifact_store=artifacts,
        evidence_store=evidence,
        evaluated_at=evaluated,
    )
    assert result.outcome == "decoded_complete"
    decoded = artifacts.read_decoded(result.artifact, checkpoint=lambda _stage: None)
    assert decoded.dbzh_raw.size <= quantitative.MAX_REGION_PIXELS
    assert decoded.metadata["source"]["content_sha256"] == (
        "80cb81b9c953a8fac9bcb8127a342aed0f93d0f203764caf8866fd0d244b471a"
    )
    assert decoded.metadata["product"]["upstream_advected_contributions"] is True
    assert np.all(decoded.qi_valid_mask[decoded.qi_raw == 0.0] == 1)
    assert authority.raw_verification_count == 1
