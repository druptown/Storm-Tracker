from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from functools import lru_cache
import hashlib
import inspect
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
from xml.etree import ElementTree as ET

import h5py
import numpy as np
import pytest
from pyproj import Transformer

from storm_tracker_worker import met_office_native as native
from storm_tracker_worker import met_office_rate_quantitative as quantitative
from storm_tracker_worker.canonical_evidence_store import (
    CanonicalEvidenceCapacityError,
    CanonicalEvidenceConflict,
)
from storm_tracker_worker.desired_state import parse_desired_state
from storm_tracker_worker.met_office_rate_quantitative import (
    MetOfficeRateArtifactCapacityError,
    MetOfficeRateArtifactError,
    MetOfficeRateDecodeDeadlineError,
    MetOfficeRateDerivedArtifactStore,
    MetOfficeRateRawAuthority,
    decode_met_office_rate_capture,
    open_met_office_rate_evidence_store,
)
from storm_tracker_worker.raw_artifacts import (
    RawArtifactIntegrityError,
    RawArtifactStore,
)
from storm_tracker_worker.worker_only_radar_processing import (
    decode_worker_only_positive_observations,
    decode_worker_only_radar_result,
)


PRODUCT_TIME = datetime(2026, 8, 2, 5, 15, tzinfo=timezone.utc).timestamp()
RECEIVED_AT = PRODUCT_TIME + 400.0
EVALUATED_AT = RECEIVED_AT + 20.0
PREVIOUS_PREFIX = "radar/2026/08/01/"
CURRENT_PREFIX = "radar/2026/08/02/"
KEY = (
    "radar/2026/08/02/"
    "202608020515_ODIM_ng_radar_rainrate_composite_1km_UK.h5"
)
ETAG = '"cab59e871637a772dc15f01f55009924"'
LAST_MODIFIED = PRODUCT_TIME + 364.0
JOB_ID = "met-office:slot:20260802T0515"


def _identity(prefix: str, value: str) -> str:
    return prefix + hashlib.sha256(value.encode("ascii")).hexdigest()


DECODER_JOB_ID = _identity("decoder-job-", "met-office-rate-job")
ATTEMPT_ID = _identity("decoder-attempt-", "met-office-rate-attempt")


def _bytes(value: str) -> np.bytes_:
    return np.bytes_(value.encode("ascii"))


def _add(parent, name: str, value: object) -> None:
    child = ET.SubElement(parent, f"{{{native.S3_NAMESPACE}}}{name}")
    child.text = str(value)


def _listing(prefix: str, objects: tuple[native.MetOfficeObject, ...]) -> bytes:
    root = ET.Element(f"{{{native.S3_NAMESPACE}}}ListBucketResult")
    _add(root, "Name", native.BUCKET_NAME)
    _add(root, "Prefix", prefix)
    _add(root, "KeyCount", len(objects))
    _add(root, "MaxKeys", native.LIST_MAX_KEYS)
    _add(root, "IsTruncated", "false")
    _add(root, "EncodingType", "url")
    for item in objects:
        child = ET.SubElement(root, f"{{{native.S3_NAMESPACE}}}Contents")
        _add(child, "Key", item.key)
        _add(
            child,
            "LastModified",
            datetime.fromtimestamp(item.last_modified, timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
        )
        _add(child, "ETag", item.etag)
        if item.checksum_algorithm is not None:
            _add(child, "ChecksumAlgorithm", item.checksum_algorithm)
            _add(child, "ChecksumType", item.checksum_type)
        _add(child, "Size", item.size_bytes)
        _add(child, "StorageClass", item.storage_class)
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def _response(
    *,
    payload: bytes,
    content_type: str,
    selected: native.MetOfficeObject | None,
) -> native.S3Response:
    return native.S3Response(
        payload=payload,
        content_type=content_type,
        received_at=RECEIVED_AT,
        etag=None if selected is None else selected.etag,
        last_modified=None if selected is None else selected.last_modified,
        version_id=None,
        request_id="fixture-request-1",
        request_id_2="fixture-request-2",
        server_side_encryption=None if selected is None else "AES256",
    )


@lru_cache(maxsize=32)
def _odim(
    value_case: str = "valid",
    *,
    quantity: str = native.ODIM_QUANTITY,
    nominal_offset: int = 0,
    columns: int = native.GRID_COLUMNS,
    dtype: str = "float32",
    gain: float = 1.0,
    link_kind: str = "hard",
    quality_group: bool = False,
) -> bytes:
    nominal = PRODUCT_TIME + nominal_offset
    start = nominal - 52
    end = nominal + 9
    moment = datetime.fromtimestamp(nominal, timezone.utc)
    start_moment = datetime.fromtimestamp(start, timezone.utc)
    end_moment = datetime.fromtimestamp(end, timezone.utc)
    target = io.BytesIO()
    with h5py.File(target, "w") as root:
        root.attrs["Conventions"] = _bytes(native.ODIM_CONVENTIONS)
        what = root.create_group("what")
        what.attrs.update(
            {
                "date": _bytes(moment.strftime("%Y%m%d")),
                "object": _bytes(native.ODIM_OBJECT),
                "source": _bytes("WMO:03523,WMO:03675"),
                "time": _bytes(moment.strftime("%H%M%S")),
                "version": _bytes(native.ODIM_VERSION),
            }
        )
        where = root.create_group("where")
        where.attrs.update(
            {
                **native.GRID_CORNERS,
                "projdef": _bytes(native.GRID_PROJECTION),
                "xscale": native.GRID_SCALE_METRES,
                "xsize": columns,
                "yscale": native.GRID_SCALE_METRES,
                "ysize": native.GRID_ROWS,
            }
        )
        how = root.create_group("how")
        how.attrs.update(
            {
                "VPRCorr": _bytes("True"),
                "camethod": _bytes("DOMAIN"),
                "endepochs": int(end),
                "nodes": _bytes("ukcle,ukche"),
                "simulated": _bytes("False"),
                "software": _bytes("RADARNET"),
                "startepochs": int(start),
                "sw_version": _bytes("2021.12.1"),
                "task": _bytes("MO.UK.RADARNET.PCompMulti"),
                "zr_a": 200.0,
                "zr_b": 1.6,
            }
        )
        dataset = root.create_group("dataset1")
        dataset_how = dataset.create_group("how")
        dataset_how.attrs["origin"] = _bytes("UPPER LEFT")
        dataset_what = dataset.create_group("what")
        dataset_what.attrs.update(
            {
                "enddate": _bytes(end_moment.strftime("%Y%m%d")),
                "endtime": _bytes(end_moment.strftime("%H%M%S")),
                "product": _bytes(native.ODIM_PRODUCT),
                "startdate": _bytes(start_moment.strftime("%Y%m%d")),
                "starttime": _bytes(start_moment.strftime("%H%M%S")),
            }
        )
        data_group = dataset.create_group("data1")
        data_what = data_group.create_group("what")
        data_what.attrs.update(
            {
                "gain": gain,
                "nodata": -1.0,
                "offset": 0.0,
                "quantity": _bytes(quantity),
                "undetect": 0.0,
            }
        )
        if quality_group:
            data_group.create_group("quality1")
        if link_kind == "external":
            data_group["data"] = h5py.ExternalLink("outside.h5", "/data")
        elif link_kind == "soft":
            root.create_dataset("outside", data=np.zeros((1,), dtype="float32"))
            data_group["data"] = h5py.SoftLink("/outside")
        else:
            raster = data_group.create_dataset(
                "data",
                shape=(native.GRID_ROWS, columns),
                dtype=dtype,
                chunks=(75, columns),
                compression="gzip",
                compression_opts=6,
            )
            chunk = np.zeros((75, columns), dtype=dtype)
            chunk[0, 0] = -1.0
            chunk[0, 1] = 0.0
            chunk[0, 2] = 1.25
            chunk[0, 3] = 2.5
            if value_case == "negative":
                chunk[0, 4] = -0.5
            elif value_case == "nan":
                chunk[0, 4] = np.nan
            elif value_case == "inf":
                chunk[0, 4] = np.inf
            for row in range(0, native.GRID_ROWS, 75):
                count = min(75, native.GRID_ROWS - row)
                raster[row : row + count, :] = chunk[:count, :]
                chunk.fill(0.0)
    return target.getvalue()


def _capture(
    tmp_path: Path,
    *,
    payload: bytes | None = None,
    key: str = KEY,
    product_timestamp: float = PRODUCT_TIME,
    mutate_hdf_provenance=None,
) -> tuple[RawArtifactStore, str]:
    exact = _odim() if payload is None else payload
    selected = native.MetOfficeObject(
        key=key,
        product_timestamp=product_timestamp,
        last_modified=LAST_MODIFIED,
        etag=ETAG,
        size_bytes=len(exact),
        storage_class="STANDARD",
        checksum_algorithm="SHA256",
        checksum_type="FULL_OBJECT",
    )
    previous = native.MetOfficeObject(
        key=(
            "radar/2026/08/01/"
            "202608012345_ODIM_ng_radar_rainrate_composite_1km_UK.h5"
        ),
        product_timestamp=PRODUCT_TIME - 5.5 * 60 * 60,
        last_modified=PRODUCT_TIME - 5.5 * 60 * 60 + 300,
        etag='"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"',
        size_bytes=123,
        storage_class="STANDARD",
        checksum_algorithm="SHA256",
        checksum_type="FULL_OBJECT",
    )
    store = RawArtifactStore((tmp_path / "raw").resolve())
    store.initialize()
    receipts = {}
    for role, prefix, objects in (
        ("listing_previous", PREVIOUS_PREFIX, (previous,)),
        ("listing_current", CURRENT_PREFIX, (selected,)),
    ):
        listing = _listing(prefix, objects)
        response = _response(
            payload=listing, content_type="application/xml", selected=None
        )
        receipts[role] = store.store(
            provider=native.PROVIDER,
            product=native.LISTING_PRODUCT,
            fetch_id=f"{JOB_ID}:{role}",
            content_type=response.content_type,
            received_at=response.received_at,
            upstream_id=(
                f"s3-list-v2:{native.BUCKET_NAME}:{prefix}:max-{native.LIST_MAX_KEYS}"
            ),
            provenance=native._listing_provenance(role, prefix, response),
            payload=listing,
        )
    object_response = _response(
        payload=exact, content_type="binary/octet-stream", selected=selected
    )
    provenance = native._object_provenance(selected, object_response)
    if mutate_hdf_provenance is not None:
        mutate_hdf_provenance(provenance)
    receipts["odim_hdf5"] = store.store(
        provider=native.PROVIDER,
        product=native.HDF5_PRODUCT,
        fetch_id=f"{JOB_ID}:odim_hdf5",
        content_type=object_response.content_type,
        received_at=object_response.received_at,
        upstream_id=f"s3:{native.BUCKET_NAME}:{selected.key}:etag:{selected.etag}",
        provenance=provenance,
        payload=exact,
    )
    capture = store.commit_capture(
        provider=native.PROVIDER,
        product=native.CAPTURE_PRODUCT,
        fetch_id=f"{JOB_ID}:capture",
        upstream_capture_id=(
            f"{native.CAPTURE_PRODUCT}|key={selected.key}|etag={selected.etag}"
            f"|previous={receipts['listing_previous'].content_sha256}"
            f"|current={receipts['listing_current'].content_sha256}"
            f"|hdf5={receipts['odim_hdf5'].content_sha256}"
        ),
        product_timestamp=product_timestamp,
        members={role: receipt.receipt_id for role, receipt in receipts.items()},
    )
    return store, capture.capture_id


def _stores(tmp_path: Path, raw: RawArtifactStore, suffix: str = ""):
    authority = MetOfficeRateRawAuthority(raw)
    artifacts = MetOfficeRateDerivedArtifactStore(
        (tmp_path / f"derived{suffix}").resolve()
    )
    evidence = open_met_office_rate_evidence_store(
        (tmp_path / f"evidence{suffix}.sqlite3").resolve(),
        raw_authority=authority,
    )
    return authority, artifacts, evidence


def _decode(
    capture_id: str,
    authority: MetOfficeRateRawAuthority,
    artifacts: MetOfficeRateDerivedArtifactStore,
    evidence,
    *,
    attempt_id: str = ATTEMPT_ID,
    evaluated_at: float = EVALUATED_AT,
    **kwargs,
):
    return decode_met_office_rate_capture(
        capture_id=capture_id,
        decoder_job_id=DECODER_JOB_ID,
        attempt_id=attempt_id,
        raw_authority=authority,
        artifact_store=artifacts,
        evidence_store=evidence,
        evaluated_at=evaluated_at,
        **kwargs,
    )


def test_exact_three_member_capture_decodes_native_rate_masks_and_times(tmp_path):
    raw, capture_id = _capture(tmp_path)
    authority, artifacts, evidence = _stores(tmp_path, raw)

    result = _decode(capture_id, authority, artifacts, evidence)

    assert result.outcome == "decoded_complete"
    assert result.reason_code == "decoded_complete"
    assert result.artifact is not None
    assert authority.raw_verification_count == 1
    assert result.evidence_receipt.run.product_time == PRODUCT_TIME
    assert len(result.evidence_receipt.run.members) == 3
    decoded = artifacts.read_decoded(result.artifact, checkpoint=lambda _stage: None)
    assert decoded.rate_encoded[0, :4].tolist() == [-1.0, 0.0, 1.25, 2.5]
    assert decoded.rate_nodata_mask[0, :4].tolist() == [1, 0, 0, 0]
    assert decoded.rate_undetect_mask[0, :4].tolist() == [0, 1, 0, 0]
    assert decoded.rate_valid_mask[0, :4].tolist() == [0, 0, 1, 1]
    assert decoded.rate_mm_h[0, :4].tolist() == [0.0, 0.0, 1.25, 2.5]
    product = decoded.metadata["product"]
    assert product["nominal_timestamp"] == PRODUCT_TIME
    assert product["input_window_start_timestamp"] == PRODUCT_TIME - 52
    assert product["input_window_end_timestamp"] == PRODUCT_TIME + 9
    assert product["quality_control"] == {
        "quality_dataset_present": False,
        "reason_code": "odim_quality_dataset_absent",
        "state": "unavailable",
    }
    assert product["interpolation_applied"] is False
    assert product["reprojection_applied"] is False
    assert product["thresholding_applied"] is False
    assert product["wet_dry_classification_applied"] is False
    assert decoded.rate_mm_h.flags.writeable is False


def test_worker_only_processor_samples_positive_met_office_target(tmp_path):
    raw, capture_id = _capture(tmp_path)
    transformer = Transformer.from_crs(
        native.GRID_PROJECTION,
        "EPSG:4326",
        always_xy=True,
    )
    longitude, latitude = transformer.transform(
        quantitative.NATIVE_MINIMUM_X_METRES + 2.5 * native.GRID_SCALE_METRES,
        quantitative.NATIVE_MAXIMUM_Y_METRES - 0.5 * native.GRID_SCALE_METRES,
    )
    desired = parse_desired_state({
        "authority_epoch": 1,
        "contract": "stv3-worker-desired-state-v1",
        "generation": 1,
        "issued_at": PRODUCT_TIME - 30.0,
        "previous_state_sha256": None,
        "producer_id": "ha:met-office-worker-only-test",
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": True,
            "region_sharing_distance_km": 100.0,
        },
        "targets": [{
            "available": True,
            "country_code": "GB",
            "display_name": "Met Office testpixel",
            "kind": "test",
            "latitude": latitude,
            "location_accuracy_m": 1.0,
            "longitude": longitude,
            "observation_radius_km": 250.0,
            "position_observed_at": PRODUCT_TIME - 10.0,
            "target_id": "met-office-positive-pixel",
        }],
    })

    processed = decode_worker_only_radar_result(
        workload_id="radar.metoffice.uk_rate",
        desired_state=desired,
        capture_id=capture_id,
        raw_store=raw,
        artifact_root=(tmp_path / "worker-only-derived").resolve(),
        evidence_path=(tmp_path / "worker-only-evidence").resolve(),
        evaluated_at=EVALUATED_AT,
        clock=lambda: EVALUATED_AT + 1.0,
    )
    observations = processed.observations

    assert len(observations) == 1
    observation = observations[0]
    assert observation.source_id == "radar.metoffice.uk_rate"
    assert observation.target_id == "met-office-positive-pixel"
    assert observation.rain_rate_mm_h == pytest.approx(1.25)
    assert observation.product_valid_at == PRODUCT_TIME
    assert processed.verified_source is not None
    assert processed.verified_source.field.capture_id == capture_id
    assert (
        processed.verified_source.field.semantics_certificate_id
        == "met-office.odim-rate.v1"
    )


def test_same_session_retry_is_byte_idempotent_and_reads_raw_members_once(tmp_path):
    raw, capture_id = _capture(tmp_path)
    authority, artifacts, evidence = _stores(tmp_path, raw)
    first = _decode(capture_id, authority, artifacts, evidence)
    retry = _decode(capture_id, authority, artifacts, evidence)

    assert retry.artifact == first.artifact
    assert retry.evidence_receipt.run_id == first.evidence_receipt.run_id
    assert retry.evidence_receipt.commit_id == first.evidence_receipt.commit_id
    assert authority.raw_verification_count == 1
    assert len(list(artifacts.objects_path.iterdir())) == 1


def test_reused_attempt_identity_with_changed_facts_conflicts(tmp_path):
    raw, capture_id = _capture(tmp_path)
    authority, artifacts, evidence = _stores(tmp_path, raw)
    _decode(capture_id, authority, artifacts, evidence)
    with pytest.raises(CanonicalEvidenceConflict):
        _decode(
            capture_id,
            authority,
            artifacts,
            evidence,
            evaluated_at=EVALUATED_AT + 1,
        )


@pytest.mark.parametrize(
    ("payload", "reason"),
    [
        (_odim("negative"), "decode_error:rate_negative"),
        (_odim("nan"), "decode_error:rate_nonfinite"),
        (_odim("inf"), "decode_error:rate_nonfinite"),
        (_odim(quantity="DBZH"), "decode_error:odim_contract"),
        (_odim(nominal_offset=900), "decode_error:odim_contract"),
        (_odim(columns=native.GRID_COLUMNS - 1), "decode_error:odim_contract"),
        (_odim(dtype="float64"), "decode_error:odim_contract"),
        (_odim(gain=0.5), "decode_error:odim_contract"),
        (_odim(link_kind="external"), "decode_error:odim_contract"),
        (_odim(link_kind="soft"), "decode_error:odim_contract"),
        (_odim(quality_group=True), "decode_error:odim_contract"),
        (_odim()[:-20], "decode_error:odim_contract"),
    ],
    ids=(
        "negative",
        "nan",
        "infinity",
        "quantity-drift",
        "nominal-time-drift",
        "grid-drift",
        "dtype-drift",
        "gain-drift",
        "external-link",
        "soft-link",
        "unexpected-quality",
        "truncated",
    ),
)
def test_semantic_schema_link_and_truncation_fail_closed(tmp_path, payload, reason):
    raw, capture_id = _capture(tmp_path, payload=payload)
    authority, artifacts, evidence = _stores(tmp_path, raw)
    result = _decode(capture_id, authority, artifacts, evidence)
    assert result.outcome == "decode_error"
    assert result.reason_code == reason
    assert result.artifact is None
    assert list(artifacts.objects_path.iterdir()) == []


def test_exact_provenance_drift_is_not_accepted(tmp_path):
    def mutate(provenance):
        provenance["collector_version"] = "drift"

    raw, capture_id = _capture(tmp_path, mutate_hdf_provenance=mutate)
    authority, artifacts, evidence = _stores(tmp_path, raw)
    result = _decode(capture_id, authority, artifacts, evidence)
    assert result.outcome == "unsupported"
    assert result.reason_code == "unsupported:hdf_provenance"
    assert result.artifact is None


def test_raw_corruption_is_detected_before_decode_and_not_rehashed_by_decoder(tmp_path):
    raw, capture_id = _capture(tmp_path)
    bundle = raw.load_capture_bundle_bytes(capture_id)
    assert bundle is not None
    receipt = bundle[1]["odim_hdf5"]
    Path(receipt.artifact_path).write_bytes(b"corrupt")
    authority, artifacts, evidence = _stores(tmp_path, raw)
    with pytest.raises(RawArtifactIntegrityError):
        _decode(capture_id, authority, artifacts, evidence)
    assert authority.raw_verification_count == 0


def test_absolute_deadline_stops_before_raw_read(tmp_path):
    raw, capture_id = _capture(tmp_path)
    authority, artifacts, evidence = _stores(tmp_path, raw)
    with pytest.raises(MetOfficeRateDecodeDeadlineError):
        _decode(
            capture_id,
            authority,
            artifacts,
            evidence,
            monotonic_deadline=1.0,
            monotonic_clock=lambda: 1.0,
        )
    assert authority.raw_verification_count == 0


def test_artifact_publication_is_concurrent_content_addressed_and_tamper_evident(tmp_path):
    store = MetOfficeRateDerivedArtifactStore((tmp_path / "derived").resolve())
    payload = b"same deterministic payload"
    with ThreadPoolExecutor(max_workers=8) as pool:
        refs = list(
            pool.map(
                lambda _index: store.publish(
                    payload, checkpoint=lambda _stage: None
                ),
                range(32),
            )
        )
    assert len({reference.handle for reference in refs}) == 1
    assert len(list(store.objects_path.iterdir())) == 1
    target = next(store.objects_path.iterdir())
    target.write_bytes(b"tampered")
    with pytest.raises(MetOfficeRateArtifactError):
        store.read_bytes(refs[0], checkpoint=lambda _stage: None)


def test_artifact_object_and_staging_capacity_are_hard_bounds(tmp_path):
    store = MetOfficeRateDerivedArtifactStore(
        (tmp_path / "derived").resolve(), max_objects=1, max_staging_objects=1
    )
    store.publish(b"first", checkpoint=lambda _stage: None)
    with pytest.raises(MetOfficeRateArtifactCapacityError):
        store.publish(b"second", checkpoint=lambda _stage: None)

    staged_root = (tmp_path / "staged").resolve()
    staged = MetOfficeRateDerivedArtifactStore(staged_root, max_staging_objects=1)
    digest = hashlib.sha256(b"orphan").hexdigest()
    (staged.staging_path / f".{digest}.one").write_bytes(b"orphan")
    reopened = MetOfficeRateDerivedArtifactStore(staged_root, max_staging_objects=1)
    with pytest.raises(MetOfficeRateArtifactCapacityError):
        reopened.publish(b"different", checkpoint=lambda _stage: None)


def test_disk_full_is_mapped_and_partial_stage_is_removed(tmp_path):
    def fault(point: str) -> None:
        if point == "artifact.after_stage_fsync":
            raise OSError(28, "No space left on device")

    store = MetOfficeRateDerivedArtifactStore(
        (tmp_path / "derived").resolve(), fault_injector=fault
    )
    with pytest.raises(MetOfficeRateArtifactCapacityError):
        store.publish(b"payload", checkpoint=lambda _stage: None)
    assert list(store.objects_path.iterdir()) == []
    assert list(store.staging_path.iterdir()) == []


def test_evidence_sqlite_full_is_mapped_without_false_success(tmp_path):
    raw, capture_id = _capture(tmp_path)
    authority = MetOfficeRateRawAuthority(raw)
    artifacts = MetOfficeRateDerivedArtifactStore((tmp_path / "derived").resolve())

    def fault(point: str) -> None:
        if point == "commit.before_commit":
            raise sqlite3.OperationalError("database or disk is full")

    evidence = open_met_office_rate_evidence_store(
        (tmp_path / "evidence.sqlite3").resolve(),
        raw_authority=authority,
        fault_injector=fault,
    )
    with pytest.raises(CanonicalEvidenceCapacityError):
        _decode(capture_id, authority, artifacts, evidence)
    assert evidence.stats()["decoder_runs"] == 0


def test_abrupt_exit_after_link_is_recoverable_and_idempotent(tmp_path):
    root = (tmp_path / "derived").resolve()
    payload = b"crash-recovery-payload"
    script = "\n".join(
        (
            "import os, pathlib, sys",
            "from storm_tracker_worker.met_office_rate_quantitative import MetOfficeRateDerivedArtifactStore",
            "root = pathlib.Path(sys.argv[1]).resolve()",
            "def fault(point):",
            "    if point == 'artifact.after_link': os._exit(73)",
            "store = MetOfficeRateDerivedArtifactStore(root, fault_injector=fault)",
            "store.publish(b'crash-recovery-payload', checkpoint=lambda _stage: None)",
        )
    )
    child = subprocess.run(
        [sys.executable, "-c", script, str(root)],
        cwd=Path(__file__).resolve().parents[1],
        check=False,
    )
    assert child.returncode == 73
    recovered = MetOfficeRateDerivedArtifactStore(root)
    reference = recovered.publish(payload, checkpoint=lambda _stage: None)
    assert recovered.read_bytes(reference, checkpoint=lambda _stage: None) == payload
    assert list(recovered.staging_path.iterdir()) == []
    assert len(list(recovered.objects_path.iterdir())) == 1


def test_os_process_lock_serializes_publish_and_obeys_checkpoint_deadline(tmp_path):
    root = (tmp_path / "process-locked").resolve()
    store = MetOfficeRateDerivedArtifactStore(root)
    ready = tmp_path / "lock-ready"
    release = tmp_path / "lock-release"
    script = "\n".join(
        (
            "import pathlib, sys, time",
            "from storm_tracker_worker import met_office_rate_quantitative as module",
            "root = pathlib.Path(sys.argv[1]).resolve()",
            "ready = pathlib.Path(sys.argv[2])",
            "release = pathlib.Path(sys.argv[3])",
            "module.MetOfficeRateDerivedArtifactStore(root)",
            "with module._exclusive_store_lock(root, checkpoint=lambda _stage: None):",
            "    ready.write_text('ready', encoding='ascii')",
            "    deadline = time.monotonic() + 10",
            "    while not release.exists():",
            "        if time.monotonic() >= deadline: raise RuntimeError('release timeout')",
            "        time.sleep(0.01)",
        )
    )
    child = subprocess.Popen(
        [sys.executable, "-c", script, str(root), str(ready), str(release)],
        cwd=Path(__file__).resolve().parents[1],
    )
    try:
        wait_until = time.monotonic() + 5.0
        while not ready.exists():
            if child.poll() is not None:
                raise AssertionError(f"lock child stopped with {child.returncode}")
            if time.monotonic() >= wait_until:
                raise AssertionError("lock child did not acquire process lock")
            time.sleep(0.01)
        deadline = time.monotonic() + 0.2
        stages: list[str] = []

        def checkpoint(stage: str) -> None:
            stages.append(stage)
            if time.monotonic() >= deadline:
                raise MetOfficeRateDecodeDeadlineError("test process-lock deadline")

        with pytest.raises(MetOfficeRateDecodeDeadlineError, match="process-lock"):
            store.publish(b"must-wait", checkpoint=checkpoint)
        assert "artifact_process_lock_wait" in stages
        assert list(store.objects_path.iterdir()) == []
    finally:
        release.write_text("release", encoding="ascii")
        child.wait(timeout=10)
        assert child.returncode == 0


def test_decoder_is_unwired_and_has_no_classification_or_reprojection_calls():
    module_source = inspect.getsource(quantitative)
    assert "register_handler" not in module_source
    assert "provider_capture_registry" not in module_source
    assert "wet_dry_classification_applied\": False" in module_source
    assert "reprojection_applied\": False" in module_source
    assert "interpolation_applied\": False" in module_source
    for path in (
        Path("storm_tracker_worker/service.py"),
        Path("storm_tracker_worker/autonomous_scheduler.py"),
        Path("storm_tracker_worker/provider_capture_registry.py"),
        Path("storm_tracker_worker/__init__.py"),
    ):
        assert path.exists()
        assert "met_office_rate_quantitative" not in path.read_text(encoding="utf-8")


def test_golden_identity_profile_binds_the_observed_native_product():
    fixture = json.loads(
        Path("tests/fixtures/met_office_uk_odim_golden.json").read_text(encoding="utf-8")
    )
    assert fixture["conventions"] == native.ODIM_CONVENTIONS
    assert fixture["h5rad_version"] == native.ODIM_VERSION
    assert fixture["quantity"] == native.ODIM_QUANTITY
    assert fixture["rows"] == native.GRID_ROWS
    assert fixture["columns"] == native.GRID_COLUMNS
    assert fixture["dtype"] == "float32"
    assert fixture["product_timestamp"] == PRODUCT_TIME
    assert fixture["input_window_start"] != fixture["product_timestamp"]
    assert fixture["input_window_end"] != fixture["product_timestamp"]
    assert fixture["pixel_values_read"] is False
    assert fixture["operational_admission_eligible"] is False


@pytest.mark.skipif(
    not os.environ.get("STV3_MET_OFFICE_GOLDEN_HDF5"),
    reason="read-only live raster is intentionally external to git",
)
def test_optional_read_only_live_golden_raster_statistics():
    fixture = json.loads(
        Path("tests/fixtures/met_office_uk_odim_golden.json").read_text(encoding="utf-8")
    )
    path = Path(os.environ["STV3_MET_OFFICE_GOLDEN_HDF5"]).resolve()
    payload = path.read_bytes()
    assert hashlib.sha256(payload).hexdigest() == fixture["sha256"]
    selected = native.MetOfficeObject(
        key=fixture["key"],
        product_timestamp=fixture["product_timestamp"],
        last_modified=LAST_MODIFIED,
        etag=fixture["etag"],
        size_bytes=fixture["size_bytes"],
        storage_class="STANDARD",
        checksum_algorithm="SHA256",
        checksum_type="FULL_OBJECT",
    )
    identity = native._inspect_odim(payload, selected=selected)
    assert identity.input_window_start == fixture["input_window_start"]
    assert identity.input_window_end == fixture["input_window_end"]
    with h5py.File(io.BytesIO(payload), "r") as root:
        raster = root["dataset1/data1/data"][:]
    valid = np.isfinite(raster) & (raster != -1.0) & (raster != 0.0)
    assert np.all(raster[valid] >= 0.0)
    assert int(np.count_nonzero(valid)) >= 0
