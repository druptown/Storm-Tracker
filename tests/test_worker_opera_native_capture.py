from __future__ import annotations

from copy import deepcopy
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
from types import SimpleNamespace
import time
import tracemalloc
import threading

import h5py
import numpy as np
import pytest

from storm_tracker_worker import opera_native
from storm_tracker_worker.raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RawArtifactStore,
)


PRODUCT_TIMESTAMP = datetime(
    2026, 8, 2, 12, 0, tzinfo=timezone.utc
).timestamp()
ROWS = 8
COLUMNS = 10
CHUNK_ROWS = 4
CHUNK_COLUMNS = 5
CANONICAL_JOB_ID = "opera-dbzh-20260802T1200"
OBJECT_KEY = "2026/08/02/OPERA/COMP/OPERA@20260802T1200@0@DBZH.h5"
FIXTURE_NODES = "'behel', 'bejab'"


def _small_grid(monkeypatch) -> None:
    monkeypatch.setattr(opera_native, "OPERA_GRID_ROWS", ROWS)
    monkeypatch.setattr(opera_native, "OPERA_GRID_COLUMNS", COLUMNS)
    monkeypatch.setattr(opera_native, "OPERA_CHUNK_ROWS", CHUNK_ROWS)
    monkeypatch.setattr(opera_native, "OPERA_CHUNK_COLUMNS", CHUNK_COLUMNS)


def _bytes(value: str) -> np.bytes_:
    return np.bytes_(value.encode("ascii"))


def _attrs(base: dict[str, object], changes: dict[str, object] | None) -> dict[str, object]:
    result = dict(base)
    for key, value in (changes or {}).items():
        if value is None:
            result.pop(key, None)
        else:
            result[key] = value
    return result


def _make_raster(
    group: h5py.Group,
    name: str,
    *,
    rows: int,
    columns: int,
    dtype: str = "<f8",
    chunks: tuple[int, int] = (CHUNK_ROWS, CHUNK_COLUMNS),
    compression: str | None = "gzip",
    compression_opts: int | None = 9,
    shuffle: bool = False,
    fletcher32: bool = False,
    scaleoffset: int | None = None,
    sparse: bool = False,
    skipped_filter: bool = False,
    image_class: str = opera_native.OPERA_IMAGE_CLASS,
    image_version: str = opera_native.OPERA_IMAGE_VERSION,
) -> h5py.Dataset:
    options: dict[str, object] = {
        "chunks": chunks,
        "shuffle": shuffle,
        "fletcher32": fletcher32,
    }
    if compression is not None:
        options["compression"] = compression
        if compression_opts is not None:
            options["compression_opts"] = compression_opts
    if scaleoffset is not None:
        options["scaleoffset"] = scaleoffset
    if sparse:
        dataset = group.create_dataset(
            name, shape=(rows, columns), dtype=np.dtype(dtype), **options
        )
        dataset[0, 0] = 0.0
    else:
        dataset = group.create_dataset(
            name,
            data=np.zeros((rows, columns), dtype=np.dtype(dtype)),
            **options,
        )
    if skipped_filter:
        raw = np.zeros(chunks, dtype=np.dtype(dtype)).tobytes(order="C")
        dataset.id.write_direct_chunk((0, 0), raw, filter_mask=1)
    dataset.attrs["CLASS"] = _bytes(image_class)
    dataset.attrs["IMAGE_VERSION"] = _bytes(image_version)
    return dataset


def _hdf5(
    *,
    product_timestamp: float = PRODUCT_TIMESTAMP,
    conventions: str = opera_native.OPERA_ODIM_CONVENTIONS,
    h5rad_version: str = opera_native.OPERA_H5RAD_VERSION,
    source: str = opera_native.OPERA_SOURCE,
    object_name: str = "COMP",
    product_name: str = "MAX",
    product_description: str = opera_native.OPERA_PRODUCT_NAME,
    quantity: str = "DBZH",
    projdef: str = opera_native.OPERA_PROJDEF,
    corners: dict[str, float] | None = None,
    rows: int = ROWS,
    columns: int = COLUMNS,
    data_attrs: dict[str, object] | None = None,
    quality_attrs: dict[str, object] | None = None,
    quality_task: str = opera_native.OPERA_QUALITY_TASK,
    data_spec: dict[str, object] | None = None,
    quality_spec: dict[str, object] | None = None,
    root_how_attrs: dict[str, object] | None = None,
    nodes: str = FIXTURE_NODES,
    include_root_how: bool = True,
    include_quality: bool = True,
    add_external_link: bool = False,
    add_extra_dataset: bool = False,
    alias_quality_to_data: bool = False,
    marker: str | None = None,
) -> bytes:
    buffer = io.BytesIO()
    moment = datetime.fromtimestamp(product_timestamp, tz=timezone.utc)
    start = datetime.fromtimestamp(product_timestamp - 599, tz=timezone.utc)
    with h5py.File(buffer, "w") as root:
        root.attrs["Conventions"] = _bytes(conventions)
        if marker is not None:
            root.attrs["test_marker"] = _bytes(marker)
        what = root.create_group("what")
        what.attrs["object"] = _bytes(object_name)
        what.attrs["version"] = _bytes(h5rad_version)
        what.attrs["source"] = _bytes(source)
        what.attrs["date"] = _bytes(moment.strftime("%Y%m%d"))
        what.attrs["time"] = _bytes(moment.strftime("%H%M%S"))

        if include_root_how:
            root_how = root.create_group("how")
            how_values = {
                name: _bytes(value)
                for name, value in opera_native.OPERA_HOW_FIXED_TEXT.items()
            }
            how_values["nodes"] = _bytes(nodes)
            for name, value in _attrs(how_values, root_how_attrs).items():
                root_how.attrs[name] = value

        where = root.create_group("where")
        where.attrs["xsize"] = columns
        where.attrs["ysize"] = rows
        where.attrs["xscale"] = opera_native.OPERA_GRID_SCALE_METRES
        where.attrs["yscale"] = opera_native.OPERA_GRID_SCALE_METRES
        where.attrs["projdef"] = _bytes(projdef)
        for name, value in (corners or opera_native.OPERA_CORNERS).items():
            where.attrs[name] = value

        dataset_what = root.create_group("dataset1/what")
        dataset_what.attrs["product"] = _bytes(product_name)
        dataset_what.attrs["prodname"] = _bytes(product_description)
        dataset_what.attrs["startdate"] = _bytes(start.strftime("%Y%m%d"))
        dataset_what.attrs["starttime"] = _bytes(start.strftime("%H%M%S"))
        dataset_what.attrs["enddate"] = _bytes(moment.strftime("%Y%m%d"))
        dataset_what.attrs["endtime"] = _bytes(moment.strftime("%H%M%S"))

        data1 = root.create_group("dataset1/data1")
        data_what = data1.create_group("what")
        for name, value in _attrs(
            {
                "quantity": _bytes(quantity),
                "gain": opera_native.OPERA_DBZH_GAIN,
                "offset": opera_native.OPERA_DBZH_OFFSET,
                "nodata": opera_native.OPERA_DBZH_NODATA,
                "undetect": opera_native.OPERA_DBZH_UNDETECT,
            },
            data_attrs,
        ).items():
            data_what.attrs[name] = value
        data_dataset = _make_raster(
            data1, "data", rows=rows, columns=columns, **(data_spec or {})
        )

        if include_quality:
            quality = data1.create_group("quality1")
            quality_what = quality.create_group("what")
            for name, value in _attrs(
                {
                    "gain": opera_native.OPERA_QUALITY_GAIN,
                    "offset": opera_native.OPERA_QUALITY_OFFSET,
                    "nodata": opera_native.OPERA_QUALITY_NODATA,
                },
                quality_attrs,
            ).items():
                quality_what.attrs[name] = value
            quality_how = quality.create_group("how")
            quality_how.attrs["task"] = _bytes(quality_task)
            if alias_quality_to_data:
                quality["data"] = data_dataset
            else:
                _make_raster(
                    quality,
                    "data",
                    rows=rows,
                    columns=columns,
                    **(quality_spec or {}),
                )
        if add_extra_dataset:
            root.create_group("dataset2")
        if add_external_link:
            root["outside"] = h5py.ExternalLink("outside.h5", "/data")
    return buffer.getvalue()


def _job(
    job_id: str = CANONICAL_JOB_ID,
    *,
    product_timestamp: float = PRODUCT_TIMESTAMP,
    payload: dict[str, object] | None = None,
):
    return SimpleNamespace(
        job_id=job_id,
        payload=(
            {"product_timestamp": product_timestamp}
            if payload is None
            else payload
        ),
    )


def _revision(
    payload: bytes,
    *,
    etag: str = '"0123456789abcdef0123456789abcdef"',
    version_id: str | None = None,
    content_type: str = "application/octet-stream",
) -> opera_native.OperaRevisionIdentity:
    return opera_native.OperaRevisionIdentity(
        etag=etag,
        version_id=version_id,
        content_length=len(payload),
        content_type=content_type,
    )


def _install_transport(
    monkeypatch,
    tmp_path: Path,
    payload: bytes,
    *,
    revision: opera_native.OperaRevisionIdentity | None = None,
):
    expected_revision = revision or _revision(payload)
    calls: dict[str, list[object]] = {"head": [], "download": [], "deadlines": []}
    counter = 0

    def head(object_key: str, *, deadline: float):
        calls["head"].append(object_key)
        calls["deadlines"].append(deadline)
        return expected_revision

    def download(
        object_key: str,
        observed_revision: opera_native.OperaRevisionIdentity,
        *,
        deadline: float,
    ):
        nonlocal counter
        assert observed_revision == expected_revision
        calls["download"].append(object_key)
        calls["deadlines"].append(deadline)
        counter += 1
        path = tmp_path / f"transport-{counter}.h5.part"
        path.write_bytes(payload)
        return opera_native.OperaDownloadedFile(
            path=str(path),
            size_bytes=len(payload),
            content_sha256=hashlib.sha256(payload).hexdigest(),
            content_type=expected_revision.content_type,
            received_at=PRODUCT_TIMESTAMP + 5,
            etag=expected_revision.etag,
            version_id=expected_revision.version_id,
        )

    monkeypatch.setattr(opera_native, "_head_hdf5", head)
    monkeypatch.setattr(opera_native, "_download_hdf5", download)
    return expected_revision, calls


def _capture(monkeypatch, tmp_path: Path, *, payload: bytes | None = None):
    _small_grid(monkeypatch)
    exact = payload or _hdf5()
    revision, calls = _install_transport(monkeypatch, tmp_path, exact)
    store = RawArtifactStore(tmp_path / "raw")
    result = opera_native.capture_opera_dbzh_hdf5(_job(), raw_store=store)
    return store, result, revision, calls, exact


def _inspect(monkeypatch, tmp_path: Path, payload: bytes):
    _small_grid(monkeypatch)
    path = tmp_path / "fixture.h5"
    path.write_bytes(payload)
    return opera_native._inspect_opera_hdf5(
        path, expected_size=len(payload), deadline=time.monotonic() + 30
    )


def test_official_key_and_canonical_job_id_are_derived_from_one_slot():
    job_id, timestamp, key = opera_native._job_identity(_job())
    assert job_id == CANONICAL_JOB_ID
    assert timestamp == PRODUCT_TIMESTAMP
    assert key == OBJECT_KEY


@pytest.mark.parametrize(
    "job",
    [
        _job(payload={}),
        _job(payload={"product_timestamp": PRODUCT_TIMESTAMP, "url": "https://x"}),
        _job(payload={"product_timestamp": PRODUCT_TIMESTAMP, "token": "secret"}),
        _job(product_timestamp=PRODUCT_TIMESTAMP + 60),
        _job(product_timestamp=float("nan")),
        _job(job_id="opera-dbzh-20260802T1205"),
        _job(job_id="bad job id"),
    ],
)
def test_ambiguous_non_slot_or_noncanonical_jobs_fail_before_head(
    monkeypatch, tmp_path, job
):
    monkeypatch.setattr(
        opera_native,
        "_head_hdf5",
        lambda *_args, **_kwargs: pytest.fail("ongeldig jobcontract gebruikte HEAD"),
    )
    with pytest.raises(ValueError):
        opera_native.capture_opera_dbzh_hdf5(
            job, raw_store=RawArtifactStore(tmp_path / "raw")
        )


def test_exact_file_is_durable_before_hdf5_inspection(monkeypatch, tmp_path):
    _small_grid(monkeypatch)
    payload = _hdf5()
    _revision_value, calls = _install_transport(monkeypatch, tmp_path, payload)
    store = RawArtifactStore(tmp_path / "raw")
    real_inspector = opera_native._inspect_opera_hdf5
    inspected: list[Path] = []

    def inspect(path, *, expected_size, deadline):
        durable = Path(path)
        assert store.stats()["receipt_count"] == 1
        assert store.stats()["capture_count"] == 0
        assert durable.parent == tmp_path / "raw" / "objects"
        assert durable.read_bytes() == payload
        assert expected_size == len(payload)
        inspected.append(durable)
        return real_inspector(path, expected_size=expected_size, deadline=deadline)

    monkeypatch.setattr(opera_native, "_inspect_opera_hdf5", inspect)
    opera_native.capture_opera_dbzh_hdf5(_job(), raw_store=store)
    assert len(inspected) == 1
    assert calls["download"] == [OBJECT_KEY]


def test_complete_capture_retains_exact_revision_and_contract(monkeypatch, tmp_path):
    store, result, revision, calls, payload = _capture(monkeypatch, tmp_path)
    assert calls["head"] == [OBJECT_KEY]
    assert calls["download"] == [OBJECT_KEY]
    assert store.stats()["object_count"] == 1
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 1
    raw = result["raw_provenance"]
    capture = store.load_capture(raw["raw_capture_id"])
    assert capture is not None
    assert tuple(member.role for member in capture.members) == ("hdf5",)
    source = raw["raw_sources"]["hdf5"]
    assert store.read_bytes(source["raw_receipt_id"]) == payload
    assert source["upstream_etag"] == revision.etag
    assert source["upstream_version_id"] is None
    assert source["raw_size_bytes"] == len(payload)
    assert source["upstream_record_id"].endswith(
        f"|version_id=none|sha256={hashlib.sha256(payload).hexdigest()}"
    )
    assert source["h5rad_version"] == opera_native.OPERA_H5RAD_VERSION
    assert source["source"] == opera_native.OPERA_SOURCE
    assert source["grid_rows"] == ROWS
    assert source["grid_columns"] == COLUMNS
    assert source["reflectivity_dtype"] == "<f8"
    assert source["quality_dtype"] == "<f8"
    assert source["chunk_shape"] == [CHUNK_ROWS, CHUNK_COLUMNS]
    assert source["compression_level"] == 9
    assert source["chunk_filter_mask"] == 0
    assert source["node_count"] == 2
    assert source["nodes_sha256"] == hashlib.sha256(
        json.dumps(["behel", "bejab"], separators=(",", ":")).encode()
    ).hexdigest()
    assert source["upstream_license"] == opera_native.OPERA_HOW_LICENSE
    assert source["upstream_advected_contributions"] is True
    assert source["motion_truth_eligible"] is False
    assert result["validation_status"] == "validated_complete"
    assert result["upstream_advected_contributions"] is True
    assert result["motion_truth_eligible"] is False


def test_complete_retry_heads_but_skips_body_and_hdf5(monkeypatch, tmp_path):
    _small_grid(monkeypatch)
    payload = _hdf5()
    _revision_value, calls = _install_transport(monkeypatch, tmp_path, payload)
    store = RawArtifactStore(tmp_path / "raw")
    real_inspector = opera_native._inspect_opera_hdf5
    inspections = 0

    def inspect(*args, **kwargs):
        nonlocal inspections
        inspections += 1
        return real_inspector(*args, **kwargs)

    monkeypatch.setattr(opera_native, "_inspect_opera_hdf5", inspect)
    first = opera_native.capture_opera_dbzh_hdf5(_job(), raw_store=store)
    second = opera_native.capture_opera_dbzh_hdf5(_job(), raw_store=store)
    assert len(calls["head"]) == 2
    assert len(calls["download"]) == 1
    assert inspections == 1
    assert first["raw_provenance"]["raw_capture_id"] == second["raw_provenance"][
        "raw_capture_id"
    ]
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 1


def test_complete_retry_verifies_raw_object_exactly_once(monkeypatch, tmp_path):
    _small_grid(monkeypatch)
    payload = _hdf5()
    _revision_value, calls = _install_transport(monkeypatch, tmp_path, payload)
    store = RawArtifactStore(tmp_path / "raw")
    opera_native.capture_opera_dbzh_hdf5(_job(), raw_store=store)

    real_read_verified = RawArtifactStore._read_verified
    verification_calls = 0

    def counted(
        self,
        path,
        *,
        expected_size,
        expected_sha256,
        return_bytes,
        allow_staging=False,
    ):
        nonlocal verification_calls
        verification_calls += 1
        return real_read_verified(
            self,
            path,
            expected_size=expected_size,
            expected_sha256=expected_sha256,
            return_bytes=return_bytes,
            allow_staging=allow_staging,
        )

    monkeypatch.setattr(RawArtifactStore, "_read_verified", counted)
    monkeypatch.setattr(
        opera_native,
        "_hash_file",
        lambda *_args, **_kwargs: pytest.fail("complete retry deed extra raw hash"),
    )
    monkeypatch.setattr(
        opera_native,
        "_inspect_opera_hdf5",
        lambda *_args, **_kwargs: pytest.fail("complete retry inspecteerde HDF5"),
    )
    result = opera_native.capture_opera_dbzh_hdf5(_job(), raw_store=store)
    assert result["validation_status"] == "validated_complete"
    assert verification_calls == 1
    assert len(calls["download"]) == 1


@pytest.mark.parametrize(
    "error",
    [TimeoutError("synthetic timeout"), OSError(5, "synthetic I/O")],
)
def test_timeout_and_io_failures_are_never_cached_as_validation_verdicts(
    monkeypatch, tmp_path, error
):
    _small_grid(monkeypatch)
    payload = _hdf5()
    _revision_value, calls = _install_transport(monkeypatch, tmp_path, payload)
    store = RawArtifactStore(tmp_path / "raw")
    inspections = 0

    def fail(*_args, **_kwargs):
        nonlocal inspections
        inspections += 1
        raise type(error)(*error.args)

    monkeypatch.setattr(opera_native, "_inspect_opera_hdf5", fail)
    for _ in range(2):
        with pytest.raises(type(error)):
            opera_native.capture_opera_dbzh_hdf5(_job(), raw_store=store)
    assert inspections == 2
    assert len(calls["download"]) == 1
    verdict_dir = store.root / "opera_validation_verdicts"
    assert not verdict_dir.exists() or not list(verdict_dir.glob("*.json"))


def test_valid_verdict_survives_capture_commit_failure_without_reinspection(
    monkeypatch, tmp_path
):
    _small_grid(monkeypatch)
    payload = _hdf5()
    _revision_value, calls = _install_transport(monkeypatch, tmp_path, payload)
    store = RawArtifactStore(tmp_path / "raw")
    real_inspector = opera_native._inspect_opera_hdf5
    real_commit = store.commit_capture
    inspections = 0

    def inspect(*args, **kwargs):
        nonlocal inspections
        inspections += 1
        return real_inspector(*args, **kwargs)

    def fail_commit(*_args, **_kwargs):
        raise RuntimeError("synthetic capture commit failure")

    monkeypatch.setattr(opera_native, "_inspect_opera_hdf5", inspect)
    monkeypatch.setattr(store, "commit_capture", fail_commit)
    with pytest.raises(RuntimeError, match="synthetic capture commit failure"):
        opera_native.capture_opera_dbzh_hdf5(_job(), raw_store=store)
    assert inspections == 1
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0
    assert len(list((store.root / "opera_validation_verdicts").glob("*.json"))) == 1

    monkeypatch.setattr(store, "commit_capture", real_commit)
    result = opera_native.capture_opera_dbzh_hdf5(_job(), raw_store=store)
    assert result["validation_status"] == "validated_complete"
    assert inspections == 1
    assert len(calls["download"]) == 1
    assert store.stats()["capture_count"] == 1


def test_new_upstream_revision_of_same_slot_gets_distinct_receipt_and_capture(
    monkeypatch, tmp_path
):
    _small_grid(monkeypatch)
    payload = _hdf5()
    store = RawArtifactStore(tmp_path / "raw")
    first_revision = _revision(payload, etag='"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"')
    _install_transport(monkeypatch, tmp_path, payload, revision=first_revision)
    first = opera_native.capture_opera_dbzh_hdf5(_job(), raw_store=store)

    second_revision = _revision(
        payload,
        etag='"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"',
        version_id="version-2",
    )
    _install_transport(monkeypatch, tmp_path, payload, revision=second_revision)
    second = opera_native.capture_opera_dbzh_hdf5(_job(), raw_store=store)

    assert first["raw_provenance"]["raw_capture_id"] != second["raw_provenance"][
        "raw_capture_id"
    ]
    assert store.stats()["object_count"] == 1
    assert store.stats()["receipt_count"] == 2
    assert store.stats()["capture_count"] == 2
    second_source = second["raw_provenance"]["raw_sources"]["hdf5"]
    assert second_source["upstream_version_id"] == "version-2"
    assert "etag=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" in second_source[
        "upstream_record_id"
    ]


def test_v2_raw_and_capture_can_coexist_with_v3_recertification(monkeypatch, tmp_path):
    _small_grid(monkeypatch)
    payload = _hdf5()
    revision = _revision(payload)
    digest = hashlib.sha256(payload).hexdigest()
    store = RawArtifactStore(tmp_path / "raw")
    store.initialize()
    token = opera_native._revision_token(revision)
    old_raw_fetch_id = (
        f"{CANONICAL_JOB_ID}:hdf5:worker-opera-cirrus-dbzh-exact-raw-v2:"
        f"revision-{token}"
    )
    old_provenance = opera_native._native_provenance(
        object_key=OBJECT_KEY,
        nominal_timestamp=PRODUCT_TIMESTAMP,
        revision=revision,
        content_sha256=digest,
    )
    old_provenance["collector_version"] = (
        "worker-opera-cirrus-dbzh-exact-raw-v2"
    )
    old_receipt = store.store(
        provider="opera",
        product=opera_native.OPERA_HDF5_PRODUCT,
        fetch_id=old_raw_fetch_id,
        content_type=revision.content_type,
        received_at=PRODUCT_TIMESTAMP + 1,
        upstream_id=opera_native._revision_upstream_id(
            OBJECT_KEY, revision, digest
        ),
        provenance=old_provenance,
        payload=payload,
    )
    old_capture_fetch_id = (
        f"{CANONICAL_JOB_ID}:capture:opera-cirrus-certification-v2:"
        f"revision-{token}"
    )
    store.commit_capture(
        provider="opera",
        product=opera_native.OPERA_CAPTURE_PRODUCT,
        fetch_id=old_capture_fetch_id,
        upstream_capture_id="legacy-v2-certification",
        product_timestamp=PRODUCT_TIMESTAMP,
        members={"hdf5": old_receipt.receipt_id},
    )

    _install_transport(monkeypatch, tmp_path, payload, revision=revision)
    result = opera_native.capture_opera_dbzh_hdf5(_job(), raw_store=store)
    assert result["certification_contract"] == (
        opera_native.OPERA_CERTIFICATION_CONTRACT_VERSION
    )
    assert store.stats()["object_count"] == 1
    assert store.stats()["receipt_count"] == 2
    assert store.stats()["capture_count"] == 2


def test_malformed_hdf5_is_retained_but_never_committed(monkeypatch, tmp_path):
    payload = b"not-an-hdf5-container"
    revision, calls = _install_transport(monkeypatch, tmp_path, payload)
    store = RawArtifactStore(tmp_path / "raw")
    result = opera_native.capture_opera_dbzh_hdf5(_job(), raw_store=store)
    assert result["capture_state"] == "validation_rejected_non_retryable"
    assert result["validation_status"] == "deterministic_rejection"
    assert result["retryable"] is False
    assert result["weather_state"] == "unknown"
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0
    fetch_id, _capture_id = opera_native._revision_fetch_ids(CANONICAL_JOB_ID, revision)
    receipt_id = RawArtifactStore._receipt_id(
        provider="opera", product=opera_native.OPERA_HDF5_PRODUCT, fetch_id=fetch_id
    )
    assert store.read_bytes(receipt_id) == payload
    assert calls["download"] == [OBJECT_KEY]


def test_malformed_receipt_retry_is_local_for_same_revision(monkeypatch, tmp_path):
    payload = b"still-not-hdf5"
    _revision_value, calls = _install_transport(monkeypatch, tmp_path, payload)
    store_root = tmp_path / "raw"
    store = RawArtifactStore(store_root)
    real_inspector = opera_native._inspect_opera_hdf5
    inspections = 0

    def inspect(*args, **kwargs):
        nonlocal inspections
        inspections += 1
        return real_inspector(*args, **kwargs)

    monkeypatch.setattr(opera_native, "_inspect_opera_hdf5", inspect)
    first = opera_native.capture_opera_dbzh_hdf5(_job(), raw_store=store)
    reopened_store = RawArtifactStore(store_root)
    second = opera_native.capture_opera_dbzh_hdf5(
        _job(), raw_store=reopened_store
    )
    assert len(calls["head"]) == 2
    assert len(calls["download"]) == 1
    assert inspections == 1
    assert first == second
    assert first["retryable"] is False
    assert reopened_store.stats()["receipt_count"] == 1
    assert reopened_store.stats()["capture_count"] == 0


def test_resume_rejects_receipt_from_another_collector_contract(monkeypatch, tmp_path):
    _small_grid(monkeypatch)
    payload = _hdf5()
    revision = _revision(payload)
    store = RawArtifactStore(tmp_path / "raw")
    store.initialize()
    fetch_id, _capture_fetch_id = opera_native._revision_fetch_ids(
        CANONICAL_JOB_ID, revision
    )
    provenance = opera_native._native_provenance(
        object_key=OBJECT_KEY,
        nominal_timestamp=PRODUCT_TIMESTAMP,
        revision=revision,
        content_sha256=hashlib.sha256(payload).hexdigest(),
    )
    provenance["collector_version"] = "obsolete-contract"
    store.store(
        provider="opera",
        product=opera_native.OPERA_HDF5_PRODUCT,
        fetch_id=fetch_id,
        content_type=revision.content_type,
        received_at=PRODUCT_TIMESTAMP + 5,
        upstream_id=opera_native._revision_upstream_id(
            OBJECT_KEY, revision, hashlib.sha256(payload).hexdigest()
        ),
        provenance=provenance,
        payload=payload,
    )
    monkeypatch.setattr(
        opera_native,
        "_head_hdf5",
        lambda _key, *, deadline: revision,
    )
    monkeypatch.setattr(
        opera_native,
        "_download_hdf5",
        lambda *_args, **_kwargs: pytest.fail("bestaande receipt mag geen GET doen"),
    )
    with pytest.raises(ValueError, match="hervatte receipt"):
        opera_native.capture_opera_dbzh_hdf5(_job(), raw_store=store)


@pytest.mark.parametrize(
    "changes, match",
    [
        ({"conventions": "ODIM_H5/V2_3"}, "Conventions"),
        ({"h5rad_version": "H5rad 2.3"}, "productidentiteit"),
        ({"source": "ORG:eu.opera"}, "productidentiteit"),
        ({"object_name": "PVOL"}, "productidentiteit"),
        ({"product_name": "CAPPI"}, "MAX-product"),
        ({"product_description": "another product"}, "productnaam"),
        ({"quantity": "RATE"}, "DBZH-product"),
        ({"projdef": "+proj=laea +lat_0=55"}, "LAEA-projectie"),
        ({"include_quality": False}, "quality1"),
        ({"quality_task": "other.quality"}, "quality-task"),
    ],
)
def test_exact_product_identity_drift_is_rejected(monkeypatch, tmp_path, changes, match):
    with pytest.raises(ValueError, match=match):
        _inspect(monkeypatch, tmp_path, _hdf5(**changes))


@pytest.mark.parametrize("corner", sorted(opera_native.OPERA_CORNERS))
def test_every_laea_corner_coordinate_is_exact(monkeypatch, tmp_path, corner):
    corners = dict(opera_native.OPERA_CORNERS)
    corners[corner] += 0.000001
    with pytest.raises(ValueError, match=rf"where\.{corner}.*wijkt af"):
        _inspect(monkeypatch, tmp_path, _hdf5(corners=corners))


@pytest.mark.parametrize(
    "attribute, value",
    [
        ("gain", 0.5),
        ("offset", 1.0),
        ("nodata", -1.0),
        ("undetect", -2.0),
        ("quantity", None),
    ],
)
def test_dbzh_what_metadata_is_exact(monkeypatch, tmp_path, attribute, value):
    with pytest.raises(ValueError):
        _inspect(
            monkeypatch,
            tmp_path,
            _hdf5(data_attrs={attribute: value}),
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"gain": 0.5},
        {"offset": 1.0},
        {"nodata": -1.0},
        {"gain": None},
        {"quantity": _bytes("QIND")},
    ],
)
def test_quality_what_metadata_and_absence_of_quantity_are_exact(
    monkeypatch, tmp_path, changes
):
    with pytest.raises(ValueError, match="quality1/what|quality1.what"):
        _inspect(monkeypatch, tmp_path, _hdf5(quality_attrs=changes))


@pytest.mark.parametrize(
    "target, spec",
    [
        ("data", {"dtype": "<f4"}),
        ("data", {"chunks": (2, 5)}),
        ("data", {"compression_opts": 6}),
        ("data", {"shuffle": True}),
        ("data", {"fletcher32": True}),
        ("data", {"scaleoffset": 2}),
        ("data", {"compression": "lzf", "compression_opts": None}),
        ("quality", {"dtype": "<f4"}),
        ("quality", {"chunks": (2, 5)}),
        ("quality", {"compression_opts": 6}),
        ("quality", {"shuffle": True}),
        ("quality", {"fletcher32": True}),
        ("quality", {"scaleoffset": 2}),
        ("quality", {"compression": "lzf", "compression_opts": None}),
    ],
)
def test_both_rasters_require_exact_dtype_chunks_and_full_dcpl_pipeline(
    monkeypatch, tmp_path, target, spec
):
    kwargs = {"data_spec" if target == "data" else "quality_spec": spec}
    with pytest.raises(ValueError, match="DCPL|filterpipeline|shape of dtype|chunk"):
        _inspect(monkeypatch, tmp_path, _hdf5(**kwargs))


@pytest.mark.parametrize("target", ["data", "quality"])
def test_every_raster_chunk_must_be_allocated(monkeypatch, tmp_path, target):
    kwargs = {
        "data_spec" if target == "data" else "quality_spec": {"sparse": True}
    }
    with pytest.raises(ValueError, match="ontbrekende chunks"):
        _inspect(monkeypatch, tmp_path, _hdf5(**kwargs))


@pytest.mark.parametrize("target", ["data", "quality"])
def test_every_raster_chunk_filter_mask_must_be_zero(monkeypatch, tmp_path, target):
    kwargs = {
        "data_spec" if target == "data" else "quality_spec": {
            "skipped_filter": True
        }
    }
    with pytest.raises(ValueError, match="filtermask"):
        _inspect(monkeypatch, tmp_path, _hdf5(**kwargs))


def test_exact_hdf_identity_passes_without_reading_raster_values(monkeypatch, tmp_path):
    identity = _inspect(monkeypatch, tmp_path, _hdf5())
    assert identity.conventions == "ODIM_H5/V2_4"
    assert identity.h5rad_version == "H5rad 2.4"
    assert identity.source == "ORG:247"
    assert identity.product_name == "MAX"
    assert identity.quantity == "DBZH"
    assert identity.quality_task == opera_native.OPERA_QUALITY_TASK
    assert identity.reflectivity_dtype == "<f8"
    assert identity.quality_dtype == "<f8"
    assert identity.chunk_count_per_raster == 4
    assert identity.node_count == 2
    assert identity.nodes_sha256 == hashlib.sha256(
        json.dumps(
            ["behel", "bejab"], separators=(",", ":"), sort_keys=True
        ).encode()
    ).hexdigest()
    assert identity.license == opera_native.OPERA_HOW_LICENSE
    assert identity.upstream_comment == opera_native.OPERA_HOW_COMMENT
    assert identity.upstream_advected_contributions is True
    assert identity.motion_truth_eligible is False


@pytest.mark.parametrize(
    "hdf5_kwargs, match",
    [
        ({"include_root_how": False}, "how|objectboom"),
        ({"add_extra_dataset": True}, "objectboom"),
        ({"alias_quality_to_data": True}, "hardlink-alias|datasetobject"),
        (
            {
                "root_how_attrs": {
                    "license": _bytes("ALL RIGHTS RESERVED")
                }
            },
            "how.license",
        ),
        (
            {
                "root_how_attrs": {
                    "publisher_name": _bytes("untrusted publisher")
                }
            },
            "how.publisher_name",
        ),
        ({"nodes": "'bejab', 'behel'"}, "how.nodes"),
        ({"data_spec": {"image_class": "NOT_IMAGE"}}, "datasetidentiteit"),
        ({"quality_spec": {"image_version": "9.9"}}, "datasetidentiteit"),
    ],
    ids=(
        "root-how-missing",
        "extra-root-object",
        "dbzh-qi-hardlink-alias",
        "license-mismatch",
        "publisher-mismatch",
        "noncanonical-nodes",
        "dbzh-image-class",
        "qi-image-version",
    ),
)
def test_exact_golden_tree_rights_nodes_and_distinct_rasters_are_mandatory(
    monkeypatch, tmp_path, hdf5_kwargs, match
):
    with pytest.raises(ValueError, match=match):
        _inspect(monkeypatch, tmp_path, _hdf5(**hdf5_kwargs))


def test_object_key_internal_nominal_time_and_window_must_match(monkeypatch, tmp_path):
    identity = _inspect(
        monkeypatch,
        tmp_path,
        _hdf5(product_timestamp=PRODUCT_TIMESTAMP + 300),
    )
    with pytest.raises(ValueError, match="verschillen"):
        opera_native._assert_identity_matches_request(
            object_key=OBJECT_KEY,
            requested_timestamp=PRODUCT_TIMESTAMP,
            identity=identity,
        )


def test_external_hdf_link_is_rejected_without_following(monkeypatch, tmp_path):
    with pytest.raises(ValueError, match="externe of symbolische link"):
        _inspect(monkeypatch, tmp_path, _hdf5(add_external_link=True))


def test_grid_is_bound_to_exact_cirrus_dimensions(monkeypatch, tmp_path):
    with pytest.raises(ValueError, match="3800x4400"):
        _inspect(monkeypatch, tmp_path, _hdf5(rows=ROWS + 1))


def test_result_cannot_claim_rain_dry_cells_or_admission(monkeypatch, tmp_path):
    _store, result, _revision_value, _calls, _payload = _capture(
        monkeypatch, tmp_path
    )
    assert result["revision_aware_identity"] is True
    assert result["quantitative_semantics_eligible"] is False
    assert result["operational_admission_eligible"] is False
    assert result["operational_memory_eligible"] is False
    assert result["memory_activation_blocker"] == (
        "schema3_store_requires_one_complete_bytes_object"
    )
    assert result["decode_status"] == "not_decoded"
    assert result["weather_state"] == "unknown"
    encoded = json.dumps(result, sort_keys=True).lower()
    for forbidden in ('"raining"', '"dry"', '"cells"', '"observations"'):
        assert forbidden not in encoded


def test_payload_free_result_has_no_path_url_or_credentials(monkeypatch, tmp_path):
    _store, result, _revision_value, _calls, _payload = _capture(
        monkeypatch, tmp_path
    )
    encoded = json.dumps(result["raw_provenance"], sort_keys=True).lower()
    assert "artifact_path" not in encoded
    assert "https://" not in encoded.replace(
        opera_native.OPERA_HOW_LICENSE.lower(), ""
    )
    assert "authorization" not in encoded
    assert "password" not in encoded
    assert "token=" not in encoded
    assert "?" not in encoded


def test_input_payload_is_not_mutated(monkeypatch, tmp_path):
    job = _job()
    original = deepcopy(job.payload)
    _small_grid(monkeypatch)
    _install_transport(monkeypatch, tmp_path, _hdf5())
    opera_native.capture_opera_dbzh_hdf5(
        job, raw_store=RawArtifactStore(tmp_path / "raw")
    )
    assert job.payload == original


def test_provenance_is_native_revision_aware_and_predecode():
    payload = b"fixture"
    revision = _revision(payload, version_id="v1")
    digest = hashlib.sha256(payload).hexdigest()
    provenance = opera_native._native_provenance(
        object_key=OBJECT_KEY,
        nominal_timestamp=PRODUCT_TIMESTAMP,
        revision=revision,
        content_sha256=digest,
    )
    assert provenance["representation"] == NATIVE_REPRESENTATION
    assert provenance["capture_stage"] == NATIVE_CAPTURE_STAGE
    assert provenance["upstream_etag"] == revision.etag
    assert provenance["upstream_version_id"] == "v1"
    assert provenance["upstream_content_sha256"] == digest
    assert provenance["schema3_full_object_buffer_required"] is True
    assert provenance["operational_memory_certified"] is False
    assert provenance["decoded_or_admitted"] is False


class _FakeHttpResponse(io.BytesIO):
    def __init__(
        self,
        payload: bytes,
        *,
        url: str,
        status: int = 200,
        content_type: str = "application/octet-stream",
        content_encoding: str = "identity",
        declared_length: str | None = None,
        etag: str = '"0123456789abcdef0123456789abcdef"',
        version_id: str | None = None,
    ):
        super().__init__(payload)
        self.status = status
        self._url = url
        self.read_sizes: list[int] = []
        self.headers = {
            "Content-Type": content_type,
            "Content-Encoding": content_encoding,
            "Content-Length": declared_length or str(len(payload)),
            "ETag": etag,
        }
        if version_id is not None:
            self.headers["x-amz-version-id"] = version_id

    def geturl(self):
        return self._url

    def read(self, size=-1):
        self.read_sizes.append(size)
        return super().read(size)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


def _object_url(key: str = OBJECT_KEY) -> str:
    return (
        f"{opera_native.OPERA_S3_ENDPOINT}/"
        f"{opera_native.OPERA_S3_BUCKET}/{key}"
    )


def test_head_requires_and_returns_full_revision_identity():
    response = _FakeHttpResponse(
        b"ignored",
        url=_object_url(),
        declared_length="1234",
        content_type="binary/octet-stream",
        version_id="object-version-7",
    )
    revision = opera_native._head_hdf5(
        OBJECT_KEY,
        deadline=time.monotonic() + 30,
        opener=lambda request, **_kwargs: response,
    )
    assert revision == opera_native.OperaRevisionIdentity(
        etag='"0123456789abcdef0123456789abcdef"',
        version_id="object-version-7",
        content_length=1234,
        content_type="binary/octet-stream",
    )


def test_uppercase_etag_is_preserved_exactly_into_if_match_and_revision():
    uppercase = '"ABCDEFABCDEFABCDEFABCDEFABCDEFAB"'
    head_response = _FakeHttpResponse(
        b"x",
        url=_object_url(),
        declared_length="1",
        etag=uppercase,
    )
    revision = opera_native._head_hdf5(
        OBJECT_KEY,
        deadline=time.monotonic() + 30,
        opener=lambda *_args, **_kwargs: head_response,
    )
    assert revision.etag == uppercase
    get_response = _FakeHttpResponse(
        b"x",
        url=_object_url(),
        etag=uppercase,
    )
    requests = []

    def opener(request, **_kwargs):
        requests.append(request)
        return get_response

    downloaded = opera_native._download_hdf5(
        OBJECT_KEY,
        revision,
        deadline=time.monotonic() + 30,
        opener=opener,
    )
    try:
        assert requests[0].get_header("If-match") == uppercase
        assert downloaded.etag == uppercase
    finally:
        Path(downloaded.path).unlink()


@pytest.mark.parametrize(
    "changes, match",
    [
        ({"status": 302}, "HTTP 200"),
        ({"url": "https://evil.invalid/file.h5"}, "onverwacht endpoint"),
        ({"content_encoding": "gzip"}, "transportcompressie"),
        ({"content_type": "text/html"}, "Content-Type"),
        ({"declared_length": "not-a-number"}, "Content-Length"),
        ({"etag": "weak-etag"}, "ETag"),
        ({"version_id": "bad version id"}, "version-id"),
    ],
)
def test_head_contract_rejects_status_origin_encoding_type_length_and_revision(
    changes, match
):
    defaults = {
        "url": _object_url(),
        "status": 200,
        "content_type": "application/octet-stream",
        "content_encoding": "identity",
        "declared_length": "10",
        "etag": '"0123456789abcdef0123456789abcdef"',
        "version_id": None,
    }
    defaults.update(changes)
    response = _FakeHttpResponse(b"ignored", **defaults)
    with pytest.raises(ValueError, match=match):
        opera_native._head_hdf5(
            OBJECT_KEY,
            deadline=time.monotonic() + 30,
            opener=lambda *_args, **_kwargs: response,
        )


def test_get_is_conditioned_on_head_and_streams_exact_declared_bytes(tmp_path):
    payload = b"hdf5-placeholder"
    revision = _revision(payload, version_id="v5")
    response = _FakeHttpResponse(
        payload,
        url=_object_url(),
        version_id="v5",
    )
    requests = []

    def opener(request, **_kwargs):
        requests.append(request)
        return response

    downloaded = opera_native._download_hdf5(
        OBJECT_KEY,
        revision,
        deadline=time.monotonic() + 30,
        opener=opener,
        temporary_directory=tmp_path,
    )
    try:
        assert Path(downloaded.path).read_bytes() == payload
        assert downloaded.content_sha256 == hashlib.sha256(payload).hexdigest()
        assert requests[0].get_header("If-match") == revision.etag
        assert response.read_sizes[0] == len(payload) + 1
        assert not hasattr(downloaded, "payload")
    finally:
        Path(downloaded.path).unlink()


@pytest.mark.parametrize(
    "changes, match",
    [
        ({"etag": '"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"'}, "GET-revision"),
        ({"version_id": "changed"}, "GET-revision"),
        ({"content_type": "binary/octet-stream"}, "GET-revision"),
        ({"declared_length": "17"}, "GET-revision"),
    ],
)
def test_get_must_match_every_head_revision_field(tmp_path, changes, match):
    payload = b"0123456789abcdef"
    revision = _revision(payload, version_id="stable")
    defaults = {
        "url": _object_url(),
        "etag": revision.etag,
        "version_id": revision.version_id,
        "content_type": revision.content_type,
        "declared_length": str(revision.content_length),
    }
    defaults.update(changes)
    response = _FakeHttpResponse(payload, **defaults)
    with pytest.raises(ValueError, match=match):
        opera_native._download_hdf5(
            OBJECT_KEY,
            revision,
            deadline=time.monotonic() + 30,
            opener=lambda *_args, **_kwargs: response,
            temporary_directory=tmp_path,
        )
    assert not list(tmp_path.glob("*.part"))


def test_content_length_plus_one_is_the_effective_body_boundary(tmp_path):
    declared = 16
    payload = b"x" * (declared + 1)
    revision = opera_native.OperaRevisionIdentity(
        etag='"0123456789abcdef0123456789abcdef"',
        version_id=None,
        content_length=declared,
        content_type="application/octet-stream",
    )
    response = _FakeHttpResponse(
        payload,
        url=_object_url(),
        declared_length=str(declared),
    )
    with pytest.raises(ValueError, match="voorbij Content-Length"):
        opera_native._download_hdf5(
            OBJECT_KEY,
            revision,
            deadline=time.monotonic() + 30,
            opener=lambda *_args, **_kwargs: response,
            temporary_directory=tmp_path,
        )
    assert response.read_sizes == [declared + 1]
    assert not list(tmp_path.glob("*.part"))


def test_short_body_is_rejected_and_temporary_file_is_removed(tmp_path):
    payload = b"short"
    revision = opera_native.OperaRevisionIdentity(
        etag='"0123456789abcdef0123456789abcdef"',
        version_id=None,
        content_length=len(payload) + 1,
        content_type="application/octet-stream",
    )
    response = _FakeHttpResponse(
        payload,
        url=_object_url(),
        declared_length=str(len(payload) + 1),
    )
    with pytest.raises(ValueError, match="payloadlengte"):
        opera_native._download_hdf5(
            OBJECT_KEY,
            revision,
            deadline=time.monotonic() + 30,
            opener=lambda *_args, **_kwargs: response,
            temporary_directory=tmp_path,
        )
    assert not list(tmp_path.glob("*.part"))


class _GeneratedResponse:
    status = 200

    def __init__(self, size: int):
        self.remaining = size
        self._url = _object_url()
        self.max_requested = 0
        self.headers = {
            "Content-Type": "application/octet-stream",
            "Content-Encoding": "identity",
            "Content-Length": str(size),
            "ETag": '"0123456789abcdef0123456789abcdef"',
        }

    def geturl(self):
        return self._url

    def read(self, size: int):
        self.max_requested = max(self.max_requested, size)
        amount = min(size, self.remaining)
        self.remaining -= amount
        return b"x" * amount

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


def test_transport_is_file_backed_and_peak_memory_is_chunk_bounded(tmp_path):
    size = 8 * 1024 * 1024
    response = _GeneratedResponse(size)
    revision = opera_native.OperaRevisionIdentity(
        etag=response.headers["ETag"],
        version_id=None,
        content_length=size,
        content_type="application/octet-stream",
    )
    tracemalloc.start()
    try:
        downloaded = opera_native._download_hdf5(
            OBJECT_KEY,
            revision,
            deadline=time.monotonic() + 30,
            opener=lambda *_args, **_kwargs: response,
            temporary_directory=tmp_path,
        )
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    try:
        assert Path(downloaded.path).stat().st_size == size
        assert downloaded.content_sha256 == hashlib.sha256(b"x" * size).hexdigest()
        assert response.max_requested <= opera_native.STREAM_CHUNK_BYTES
        assert peak < 6 * 1024 * 1024
        assert not hasattr(downloaded, "payload")
    finally:
        Path(downloaded.path).unlink()


def test_one_shared_monotonic_deadline_reaches_head_download_and_inspector(
    monkeypatch, tmp_path
):
    _small_grid(monkeypatch)
    payload = _hdf5()
    _revision_value, calls = _install_transport(monkeypatch, tmp_path, payload)
    real_inspector = opera_native._inspect_opera_hdf5

    def inspect(path, *, expected_size, deadline):
        calls["deadlines"].append(deadline)
        return real_inspector(path, expected_size=expected_size, deadline=deadline)

    monkeypatch.setattr(opera_native, "_inspect_opera_hdf5", inspect)
    opera_native.capture_opera_dbzh_hdf5(
        _job(), raw_store=RawArtifactStore(tmp_path / "raw")
    )
    assert len(calls["deadlines"]) == 3
    assert len(set(calls["deadlines"])) == 1


def test_total_deadline_includes_store_initialization_before_head(monkeypatch):
    clock = [100.0]
    monkeypatch.setattr(opera_native.time, "monotonic", lambda: clock[0])

    def delayed_store(_injected):
        clock[0] = 401.0
        return object()

    monkeypatch.setattr(opera_native, "_raw_store", delayed_store)
    monkeypatch.setattr(
        opera_native,
        "_head_hdf5",
        lambda *_args, **_kwargs: pytest.fail("verstreken deadline deed HEAD"),
    )
    with pytest.raises(TimeoutError, match="jobdeadline"):
        opera_native.capture_opera_dbzh_hdf5(_job())


def test_expired_deadline_fails_before_network_opener():
    with pytest.raises(TimeoutError, match="jobdeadline"):
        opera_native._head_hdf5(
            OBJECT_KEY,
            deadline=time.monotonic() - 1,
            opener=lambda *_args, **_kwargs: pytest.fail("deadline deed netwerk"),
        )


def test_capture_lock_wait_is_bounded_by_the_same_total_deadline(monkeypatch):
    class BusyLock:
        def __init__(self):
            self.timeout = None

        def acquire(self, *, timeout):
            self.timeout = timeout
            return False

        def release(self):
            pytest.fail("niet-verkregen lock werd vrijgegeven")

    lock = BusyLock()
    monkeypatch.setattr(opera_native, "_OPERA_CAPTURE_LOCK", lock)
    with pytest.raises(TimeoutError, match="capturelock"):
        opera_native.capture_opera_dbzh_hdf5(_job())
    assert 0 < lock.timeout <= opera_native.JOB_TOTAL_DEADLINE_SECONDS


def test_cross_process_file_lock_excludes_another_holder_until_deadline(tmp_path):
    path = tmp_path / "shared.lock"
    ready = threading.Event()
    release = threading.Event()
    failures = []

    def holder():
        try:
            with opera_native._deadline_file_lock(
                path, deadline=time.monotonic() + 5
            ):
                ready.set()
                release.wait(timeout=5)
        except BaseException as err:  # pragma: no cover - assertion relay
            failures.append(err)
            ready.set()

    thread = threading.Thread(target=holder)
    thread.start()
    assert ready.wait(timeout=5)
    try:
        with pytest.raises(TimeoutError, match="jobdeadline"):
            with opera_native._deadline_file_lock(
                path, deadline=time.monotonic() + 0.15
            ):
                pytest.fail("tweede houder verkreeg dezelfde file lock")
    finally:
        release.set()
        thread.join(timeout=5)
    assert not thread.is_alive()
    assert not failures


def test_slot_and_global_large_buffer_locks_share_the_job_deadline(
    monkeypatch, tmp_path
):
    _small_grid(monkeypatch)
    payload = _hdf5()
    _install_transport(monkeypatch, tmp_path, payload)
    observations = []

    @contextmanager
    def observed(path, *, deadline):
        observations.append((Path(path).name, deadline))
        yield

    monkeypatch.setattr(opera_native, "_deadline_file_lock", observed)
    opera_native.capture_opera_dbzh_hdf5(
        _job(), raw_store=RawArtifactStore(tmp_path / "raw")
    )
    names = [name for name, _deadline in observations]
    assert len([name for name in names if name.startswith("slot-")]) == 1
    assert names.count("global-buffer.lock") == 1
    assert len({deadline for _name, deadline in observations}) == 1


def test_source_url_validator_rejects_redirect_and_ssrf_shapes():
    for url in (
        f"http://{opera_native.OPERA_S3_HOST}/{opera_native.OPERA_S3_BUCKET}/{OBJECT_KEY}",
        f"https://user@{opera_native.OPERA_S3_HOST}/{opera_native.OPERA_S3_BUCKET}/{OBJECT_KEY}",
        f"https://{opera_native.OPERA_S3_HOST}:444/{opera_native.OPERA_S3_BUCKET}/{OBJECT_KEY}",
        f"https://127.0.0.1/{opera_native.OPERA_S3_BUCKET}/{OBJECT_KEY}",
        f"{_object_url()}?versionId=x",
        f"{_object_url()}#fragment",
    ):
        with pytest.raises(ValueError):
            opera_native._validate_object_url(url, expected_key=OBJECT_KEY)


def test_candidate_is_not_wired_into_production_worker_service():
    service_path = Path(opera_native.__file__).with_name("service.py")
    service_source = service_path.read_text(encoding="utf-8")
    assert "storm_tracker_worker.opera_native" not in service_source
    assert "capture_opera_dbzh_hdf5" not in service_source
