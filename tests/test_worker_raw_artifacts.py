from __future__ import annotations

import ast
from contextlib import closing
import hashlib
import os
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

from storm_tracker_worker.raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RawArtifactCapacityError,
    RawArtifactConflictError,
    RawArtifactEntryCapacityError,
    RawArtifactIntegrityError,
    RawArtifactLimits,
    RawArtifactNotFoundError,
    RawArtifactProvenanceError,
    RawArtifactStore,
)


PROVENANCE = {
    "representation": NATIVE_REPRESENTATION,
    "capture_stage": NATIVE_CAPTURE_STAGE,
    "collector": "provider-http-adapter",
    "collector_version": "2026.08.01",
    "transport": {"method": "GET", "content_encoding": "identity"},
}


def _store(root: Path, **kwargs) -> RawArtifactStore:
    store = RawArtifactStore(root, **kwargs)
    store.initialize()
    return store


def _put(store: RawArtifactStore, **overrides):
    values = {
        "provider": "kmi",
        "product": "generic-native-product",
        "fetch_id": "job-1:native-object",
        "content_type": "application/x-hdf5",
        "received_at": 1_800_000_000.25,
        "upstream_id": "native/2027-01-15T12:00:00Z/revision-1",
        "provenance": PROVENANCE,
        "payload": b"\x89HDF\r\n\x1a\n\x00\xffexact-upstream-bytes",
    }
    values.update(overrides)
    return store.store(**values)


def test_capture_child_attaches_without_repeating_full_startup_audit(
    monkeypatch, tmp_path
):
    root = tmp_path / "raw"
    parent = _store(root)
    assert parent.integrity_check()["healthy"] is True

    def forbidden(*_args, **_kwargs):
        raise AssertionError("capture child repeated the parent startup audit")

    monkeypatch.setattr(RawArtifactStore, "_validate_schema", forbidden)
    monkeypatch.setattr(RawArtifactStore, "stats", forbidden)

    child = RawArtifactStore(root)
    child.attach_existing()
    receipt = _put(child, fetch_id="child-attach:native-object")
    assert receipt.fetch_id == "child-attach:native-object"


def test_capture_child_attach_is_fail_closed_and_never_creates_store(tmp_path):
    root = tmp_path / "absent"
    child = RawArtifactStore(root)

    with pytest.raises(RawArtifactIntegrityError):
        child.attach_existing()

    assert not root.exists()


def test_exact_upstream_bytes_and_explicit_metadata_are_preserved(tmp_path):
    store = _store(tmp_path / "raw")
    payload = b"\x1f\x8b\x08\x00\x00\xff\x00native\x00product\xff"

    receipt = _put(store, payload=payload)
    metadata = receipt.to_metadata()
    path = Path(receipt.artifact_path)

    assert path.name == f"{hashlib.sha256(payload).hexdigest()}.raw"
    assert path.parent == tmp_path / "raw" / "objects"
    assert path.read_bytes() == payload
    assert store.read_bytes(receipt.receipt_id) == payload
    assert metadata["provider"] == "kmi"
    assert metadata["product"] == "generic-native-product"
    assert metadata["fetch_id"] == "job-1:native-object"
    assert metadata["content_type"] == "application/x-hdf5"
    assert metadata["received_at"] == 1_800_000_000.25
    assert metadata["upstream_id"].endswith("revision-1")
    assert metadata["provenance"] == PROVENANCE
    assert metadata["provenance"]["representation"] == NATIVE_REPRESENTATION
    assert metadata["provenance"]["capture_stage"] == NATIVE_CAPTURE_STAGE
    assert receipt.content_sha256 == hashlib.sha256(payload).hexdigest()
    assert receipt.size_bytes == len(payload)
    assert store.integrity_check()["healthy"] is True


def test_startup_integrity_check_does_not_rehash_historical_object_bytes(
    tmp_path, monkeypatch
):
    store = _store(tmp_path / "raw")
    receipt = _put(store)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("startup rehashed historical raw bytes")

    monkeypatch.setattr(store, "_read_verified", forbidden)

    report = store.startup_integrity_check()

    assert report["healthy"] is True
    assert report["checked_objects"] == 1
    assert report["checked_receipts"] == 1
    assert report["verification_scope"] == "startup_catalog_metadata"
    with pytest.raises(AssertionError, match="startup rehashed"):
        store.read_bytes(receipt.receipt_id)


def test_startup_recovery_catalogs_verified_crash_published_object(tmp_path):
    store = _store(tmp_path / "raw")
    payload = b"exact-crash-published-object"
    content_sha256 = hashlib.sha256(payload).hexdigest()
    orphan = store.objects_path / f"{content_sha256}.raw"
    orphan.write_bytes(payload)

    before = store.stats()
    assert before["unmanaged_file_count"] == 1
    assert before["object_count"] == 0

    assert store.recover_unmanaged_content_objects() == 1
    after = store.stats()
    assert after["unmanaged_file_count"] == 0
    assert after["object_count"] == 1
    assert after["receipt_count"] == 0
    assert after["capture_count"] == 0
    assert store.integrity_check()["healthy"] is True
    assert store.recover_unmanaged_content_objects() == 0


def test_startup_recovery_promotes_complete_crash_stage(tmp_path):
    store = _store(tmp_path / "raw")
    payload = b"complete-crash-stage"
    content_sha256 = hashlib.sha256(payload).hexdigest()
    stage = store.staging_path / f".{content_sha256}.file_v1"
    stage.write_bytes(payload)

    assert store.recover_unmanaged_content_objects() == 1
    target = store.objects_path / f"{content_sha256}.raw"
    assert not stage.exists()
    assert target.read_bytes() == payload
    assert store.stats()["filesystem_staging_entries"] == 0
    assert store.stats()["unmanaged_file_count"] == 0
    assert store.stats()["object_count"] == 1


def test_startup_recovery_removes_only_incomplete_private_crash_stage(tmp_path):
    store = _store(tmp_path / "raw")
    expected_sha256 = hashlib.sha256(b"complete-payload").hexdigest()
    stage = store.staging_path / f".{expected_sha256}.file_v1"
    stage.write_bytes(b"partial")

    assert store.recover_unmanaged_content_objects() == 0
    assert not stage.exists()
    assert not (store.objects_path / f"{expected_sha256}.raw").exists()
    assert store.stats()["filesystem_staging_entries"] == 0
    assert store.integrity_check()["healthy"] is True


def test_startup_recovery_rejects_misnamed_or_corrupt_unmanaged_object(tmp_path):
    store = _store(tmp_path / "raw")
    claimed_sha256 = hashlib.sha256(b"expected").hexdigest()
    orphan = store.objects_path / f"{claimed_sha256}.raw"
    orphan.write_bytes(b"different")

    with pytest.raises(RawArtifactIntegrityError):
        store.recover_unmanaged_content_objects()

    assert store.stats()["unmanaged_file_count"] == 1
    assert store.stats()["object_count"] == 0


def test_combined_receipt_read_hashes_large_object_exactly_once(
    tmp_path,
    monkeypatch,
):
    store = _store(tmp_path / "raw")
    expected = _put(store, payload=b"x" * (2 * 1024 * 1024))
    original = store._read_verified
    calls: list[bool] = []

    def counted(*args, **kwargs):
        calls.append(bool(kwargs.get("return_bytes")))
        return original(*args, **kwargs)

    monkeypatch.setattr(store, "_read_verified", counted)

    receipt, payload = store.read_receipt_bytes(expected.receipt_id)
    assert receipt == expected
    assert payload == b"x" * (2 * 1024 * 1024)
    assert calls == [True]

    assert store.load_receipt_bytes("raw-" + "0" * 64) is None

    calls.clear()
    assert store.read_bytes(expected.receipt_id) == payload
    assert calls == [True]


def test_exact_retry_is_idempotent_and_content_is_deduplicated(tmp_path):
    store = _store(tmp_path / "raw")
    first = _put(store)
    repeated = _put(store)
    other_identity = _put(
        store,
        fetch_id="job-2:native-object",
        upstream_id="native/other-frame/revision-1",
    )

    assert repeated == first
    assert other_identity.receipt_id != first.receipt_id
    assert other_identity.artifact_path == first.artifact_path
    assert store.stats()["object_count"] == 1
    assert store.stats()["receipt_count"] == 2


def test_first_store_returns_transaction_verified_receipt_without_rehash(
    tmp_path,
    monkeypatch,
):
    store = _store(tmp_path / "raw")
    monkeypatch.setattr(
        store,
        "load_receipt",
        lambda *_args, **_kwargs: pytest.fail(
            "first store mag na commit hetzelfde object niet opnieuw hashen"
        ),
    )

    receipt = _put(store, payload=b"one-pass-return" * 4096)

    assert receipt.content_sha256 == hashlib.sha256(
        b"one-pass-return" * 4096
    ).hexdigest()


def test_same_stable_upstream_identity_cannot_point_to_different_bytes(tmp_path):
    store = _store(tmp_path / "raw")
    first = _put(store)
    original = store.read_bytes(first.receipt_id)

    with pytest.raises(RawArtifactConflictError):
        _put(store, payload=b"different exact bytes")

    assert store.read_bytes(first.receipt_id) == original
    assert store.stats()["receipt_count"] == 1


def test_retry_keeps_first_received_at_but_next_job_records_a_new_fetch(tmp_path):
    store = _store(tmp_path / "raw")
    first = _put(store, received_at=1_800_000_000.25)
    retry = _put(store, received_at=1_800_000_600.75)
    next_job = _put(
        store,
        fetch_id="job-2:native-object",
        received_at=1_800_000_600.75,
    )

    assert retry == first
    assert retry.received_at == 1_800_000_000.25
    assert next_job.receipt_id != first.receipt_id
    assert next_job.artifact_path == first.artifact_path
    assert store.load_receipt(first.receipt_id).received_at == 1_800_000_000.25
    assert store.load_receipt(next_job.receipt_id).received_at == 1_800_000_600.75
    assert store.stats()["object_count"] == 1
    assert store.stats()["receipt_count"] == 2


def test_capture_manifest_is_atomic_idempotent_and_product_time_is_separate(
    tmp_path,
):
    store = _store(tmp_path / "raw")
    forecast = _put(
        store,
        product="kmi_appv4_forecast_manifest",
        fetch_id="job-1:forecast-json",
    )
    image = _put(
        store,
        product="kmi_appv4_visualisation_png",
        fetch_id="job-1:radar-png",
        upstream_id="visualisation/frame-1",
        received_at=1_800_000_001.5,
        payload=b"exact-rendered-png",
    )

    capture = store.commit_capture(
        provider="kmi",
        product="kmi_appv4_visualisation_capture",
        fetch_id="job-1:capture",
        upstream_capture_id="frame-1-content-fallback",
        product_timestamp=1_700_000_000.0,
        members={
            "forecast_json": forecast.receipt_id,
            "radar_png": image.receipt_id,
        },
    )
    repeated = store.commit_capture(
        provider="kmi",
        product="kmi_appv4_visualisation_capture",
        fetch_id="job-1:capture",
        upstream_capture_id="frame-1-content-fallback",
        product_timestamp=1_700_000_000.0,
        members={
            "radar_png": image.receipt_id,
            "forecast_json": forecast.receipt_id,
        },
    )

    assert repeated == capture
    assert capture.product_timestamp == 1_700_000_000.0
    assert {member.received_at for member in capture.members} == {
        1_800_000_000.25,
        1_800_000_001.5,
    }
    with pytest.raises(RawArtifactConflictError, match="capture-fetch-id"):
        store.commit_capture(
            provider="kmi",
            product="kmi_appv4_visualisation_capture",
            fetch_id="job-1:capture",
            upstream_capture_id="frame-1-content-fallback",
            product_timestamp=1_700_000_300.0,
            members={
                "forecast_json": forecast.receipt_id,
                "radar_png": image.receipt_id,
            },
        )
    assert store.stats()["capture_count"] == 1


def test_capture_bundle_verifies_each_member_once_and_reuses_receipts(
    tmp_path,
    monkeypatch,
):
    store = _store(tmp_path / "raw")
    first = _put(
        store,
        product="first-native",
        fetch_id="bundle:first",
        upstream_id="bundle/first",
        payload=b"a" * (1024 * 1024),
    )
    second = _put(
        store,
        product="second-native",
        fetch_id="bundle:second",
        upstream_id="bundle/second",
        payload=b"b" * (1024 * 1024),
    )
    capture = store.commit_capture(
        provider="kmi",
        product="bundle-capture",
        fetch_id="bundle:capture",
        upstream_capture_id="bundle/upstream",
        product_timestamp=1_700_000_000.0,
        members={"first": first.receipt_id, "second": second.receipt_id},
    )
    original = store._read_verified
    calls: list[str] = []

    def counted(path, **kwargs):
        calls.append(Path(path).name)
        return original(path, **kwargs)

    monkeypatch.setattr(store, "_read_verified", counted)

    loaded, receipts = store.load_capture_bundle(capture.capture_id)
    assert loaded == capture
    assert receipts == {"first": first, "second": second}
    assert len(calls) == 2
    assert len(set(calls)) == 2

    calls.clear()
    loaded, receipts, payloads = store.load_capture_bundle_bytes(
        capture.capture_id
    )
    assert loaded == capture
    assert receipts == {"first": first, "second": second}
    assert payloads == {
        "first": b"a" * (1024 * 1024),
        "second": b"b" * (1024 * 1024),
    }
    assert len(calls) == 2
    assert len(set(calls)) == 2


def test_first_capture_commit_does_not_rehash_members_after_commit(
    tmp_path,
    monkeypatch,
):
    store = _store(tmp_path / "raw")
    first = _put(
        store,
        product="first-native",
        fetch_id="capture-return:first",
        upstream_id="capture-return/first",
    )
    second = _put(
        store,
        product="second-native",
        fetch_id="capture-return:second",
        upstream_id="capture-return/second",
        payload=b"other-native-product",
    )
    monkeypatch.setattr(
        store,
        "load_capture",
        lambda *_args, **_kwargs: pytest.fail(
            "capturecommit mag leden na commit niet opnieuw hashen"
        ),
    )

    capture = store.commit_capture(
        provider="kmi",
        product="capture-return",
        fetch_id="capture-return:capture",
        upstream_capture_id="capture-return/upstream",
        product_timestamp=1_700_000_000.0,
        members={"first": first.receipt_id, "second": second.receipt_id},
    )

    assert {member.role for member in capture.members} == {"first", "second"}


def test_capture_manifest_failure_never_exposes_partial_capture(tmp_path):
    root = tmp_path / "raw"

    def fail(name: str) -> None:
        if name == "capture_before_commit":
            raise RuntimeError("capture crash")

    failing = _store(root, fault_injector=fail)
    forecast = _put(
        failing,
        product="kmi_appv4_forecast_manifest",
        fetch_id="job-1:forecast-json",
    )
    image = _put(
        failing,
        product="kmi_appv4_visualisation_png",
        fetch_id="job-1:radar-png",
        upstream_id="visualisation/frame-1",
        payload=b"exact-rendered-png",
    )
    with pytest.raises(RuntimeError, match="capture crash"):
        failing.commit_capture(
            provider="kmi",
            product="kmi_appv4_visualisation_capture",
            fetch_id="job-1:capture",
            upstream_capture_id="frame-1-content-fallback",
            product_timestamp=1_700_000_000.0,
            members={
                "forecast_json": forecast.receipt_id,
                "radar_png": image.receipt_id,
            },
        )

    reopened = _store(root)
    assert reopened.stats()["receipt_count"] == 2
    assert reopened.stats()["capture_count"] == 0
    capture = reopened.commit_capture(
        provider="kmi",
        product="kmi_appv4_visualisation_capture",
        fetch_id="job-1:capture",
        upstream_capture_id="frame-1-content-fallback",
        product_timestamp=1_700_000_000.0,
        members={
            "forecast_json": forecast.receipt_id,
            "radar_png": image.receipt_id,
        },
    )
    assert reopened.load_capture(capture.capture_id) == capture


def test_capture_can_be_recovered_by_exact_fetch_identity(tmp_path):
    store = _store(tmp_path / "raw")
    receipt = _put(
        store,
        provider="dwd",
        product="dwd_open_data_composite_rv_tar",
        fetch_id="job-restore:rv_tar",
    )
    capture = store.commit_capture(
        provider="dwd",
        product="dwd_open_data_composite_rv_raw_capture",
        fetch_id="job-restore:capture",
        upstream_capture_id="dwd-restore-test",
        product_timestamp=1_700_000_000.0,
        members={"rv_tar": receipt.receipt_id},
    )

    assert store.load_capture_by_fetch(
        provider="dwd",
        product="dwd_open_data_composite_rv_raw_capture",
        fetch_id="job-restore:capture",
    ) == capture
    assert store.load_capture_by_fetch(
        provider="dwd",
        product="dwd_open_data_composite_rv_raw_capture",
        fetch_id="job-missing:capture",
    ) is None


def test_abrupt_process_exit_between_capture_members_is_retryable(tmp_path):
    root = tmp_path / "raw"
    _store(root)
    helper = (
        Path(__file__).resolve().parent
        / "helpers/worker_raw_artifacts_crash_child.py"
    )
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])

    result = subprocess.run(
        [sys.executable, str(helper), str(root), "between_capture_members"],
        cwd=Path(__file__).resolve().parents[1],
        env=environment,
        capture_output=True,
        check=False,
        timeout=20,
    )

    assert result.returncode == 73, result.stderr.decode(errors="replace")
    reopened = _store(root)
    assert reopened.stats()["receipt_count"] == 1
    assert reopened.stats()["capture_count"] == 0
    forecast = _put(
        reopened,
        product="kmi_appv4_forecast_manifest",
        fetch_id="job-crash:forecast-json",
        content_type="application/json",
        received_at=1_800_000_600.75,
        upstream_id="forecast/frame-content-fallback",
        payload=b"exact-forecast-json",
    )
    image = _put(
        reopened,
        product="kmi_appv4_visualisation_png",
        fetch_id="job-crash:radar-png",
        content_type="image/png",
        received_at=1_800_000_601.25,
        upstream_id="visualisation/frame-content-fallback",
        payload=b"exact-rendered-png",
    )
    capture = reopened.commit_capture(
        provider="kmi",
        product="kmi_appv4_visualisation_capture",
        fetch_id="job-crash:capture",
        upstream_capture_id="capture/frame-content-fallback",
        product_timestamp=1_700_000_000.0,
        members={
            "forecast_json": forecast.receipt_id,
            "radar_png": image.receipt_id,
        },
    )

    assert forecast.received_at == 1_800_000_000.25
    assert reopened.load_capture(capture.capture_id) == capture
    assert reopened.stats()["receipt_count"] == 2
    assert reopened.stats()["capture_count"] == 1
    assert reopened.integrity_check()["healthy"] is True


@pytest.mark.parametrize(
    "provenance",
    (
        {
            **PROVENANCE,
            "representation": "decoded_provider_result",
            "capture_stage": "after_decode",
        },
        {key: value for key, value in PROVENANCE.items() if key != "representation"},
        {
            **PROVENANCE,
            "capture_stage": "after_decode",
        },
    ),
)
def test_decoded_or_implicit_provenance_is_never_promoted_to_raw(
    tmp_path, provenance,
):
    store = _store(tmp_path / "raw")

    with pytest.raises(RawArtifactProvenanceError):
        _put(store, provenance=provenance)

    assert store.stats()["object_count"] == 0
    assert store.stats()["receipt_count"] == 0


def test_per_object_capacity_error_is_explicit_and_writes_nothing(tmp_path):
    store = _store(
        tmp_path / "raw",
        limits=RawArtifactLimits(max_object_bytes=4, max_total_bytes=10),
    )

    with pytest.raises(RawArtifactCapacityError) as raised:
        _put(store, payload=b"12345")

    assert raised.value.scope == "object"
    assert raised.value.attempted_bytes == 5
    assert store.stats()["object_count"] == 0
    assert store.stats()["receipt_count"] == 0


def test_total_capacity_rejects_whole_object_without_dry_fallback(tmp_path):
    store = _store(
        tmp_path / "raw",
        limits=RawArtifactLimits(max_object_bytes=4, max_total_bytes=6),
    )
    first = _put(store, payload=b"1234")

    with pytest.raises(RawArtifactCapacityError) as raised:
        _put(
            store,
            fetch_id="job-2:native-object",
            upstream_id="second-object",
            payload=b"567",
        )

    assert raised.value.scope == "total"
    assert raised.value.current_bytes == 4
    assert raised.value.attempted_bytes == 3
    assert store.read_bytes(first.receipt_id) == b"1234"
    assert store.stats()["object_count"] == 1
    assert store.stats()["receipt_count"] == 1
    with pytest.raises(RawArtifactNotFoundError):
        store.read_bytes("raw-" + "0" * 64)


def test_full_capacity_still_allows_new_receipt_for_existing_exact_bytes(tmp_path):
    store = _store(
        tmp_path / "raw",
        limits=RawArtifactLimits(max_object_bytes=4, max_total_bytes=4),
    )
    first = _put(store, payload=b"1234")
    second = _put(
        store,
        fetch_id="job-2:native-object",
        upstream_id="same-bytes-new-id",
        payload=b"1234",
    )

    assert first.artifact_path == second.artifact_path
    assert store.stats()["filesystem_bytes"] == 4
    assert store.stats()["receipt_count"] == 2


def test_entry_capacity_bounds_objects_and_staging_without_breaking_dedup(tmp_path):
    limits = RawArtifactLimits(
        max_object_bytes=16,
        max_total_bytes=64,
        max_object_entries=1,
        max_staging_entries=1,
    )
    object_store = _store(tmp_path / "object-bounded", limits=limits)
    first = _put(object_store, payload=b"first")
    repeated_bytes = _put(
        object_store,
        fetch_id="job-2:native-object",
        upstream_id="same-bytes-second-identity",
        payload=b"first",
    )
    assert repeated_bytes.artifact_path == first.artifact_path
    with pytest.raises(RawArtifactEntryCapacityError) as object_error:
        _put(
            object_store,
            fetch_id="job-3:native-object",
            upstream_id="different-bytes",
            payload=b"second",
        )
    assert object_error.value.scope == "objects"
    assert object_error.value.current_entries == 1
    assert object_error.value.attempted_entries == 1

    staging_store = _store(tmp_path / "staging-bounded", limits=limits)
    orphan = staging_store.staging_path / f".{('0' * 64)}.orphan"
    orphan.write_bytes(b"orphan")
    with pytest.raises(RawArtifactEntryCapacityError) as staging_error:
        _put(staging_store, payload=b"new")
    assert staging_error.value.scope == "staging"
    assert staging_error.value.current_entries == 1


def test_store_profile_binds_limits_immutably_across_instances(tmp_path):
    root = tmp_path / "raw"
    limits = RawArtifactLimits(max_object_entries=1)
    store = _store(root, limits=limits)
    first = _put(store, payload=b"bound-profile")

    reopened = _store(root, limits=limits)
    assert reopened.read_bytes(first.receipt_id) == b"bound-profile"
    assert len(reopened.stats()["store_profile_sha256"]) == 64

    with pytest.raises(RawArtifactConflictError, match="limietprofiel"):
        _store(root, limits=RawArtifactLimits(max_object_entries=2))


def test_receipt_capacity_allows_exact_retry_but_no_new_identity(tmp_path):
    store = _store(
        tmp_path / "raw",
        limits=RawArtifactLimits(max_receipt_entries=1),
    )
    first = _put(store, payload=b"one-object")
    assert _put(store, payload=b"one-object") == first

    with pytest.raises(RawArtifactEntryCapacityError) as raised:
        _put(
            store,
            fetch_id="job-2:native-object",
            upstream_id="second-identity",
            payload=b"one-object",
        )

    assert raised.value.scope == "receipts"
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["object_count"] == 1


def test_capture_and_member_capacities_allow_only_exact_capture_retry(tmp_path):
    store = _store(
        tmp_path / "raw",
        limits=RawArtifactLimits(
            max_capture_entries=1,
            max_capture_members=2,
        ),
    )
    first = _put(store, payload=b"first-member")
    second = _put(
        store,
        fetch_id="job-2:native-object",
        upstream_id="second-member",
        payload=b"second-member",
    )
    arguments = {
        "provider": "kmi",
        "product": "bounded-capture",
        "fetch_id": "capture-1",
        "upstream_capture_id": "upstream-capture-1",
        "product_timestamp": 1_800_000_000.0,
        "members": {
            "first": first.receipt_id,
            "second": second.receipt_id,
        },
    }
    capture = store.commit_capture(**arguments)
    assert store.commit_capture(**arguments) == capture

    with pytest.raises(RawArtifactEntryCapacityError) as capture_error:
        store.commit_capture(
            **{
                **arguments,
                "fetch_id": "capture-2",
                "upstream_capture_id": "upstream-capture-2",
            }
        )
    assert capture_error.value.scope == "captures"
    assert store.stats()["capture_count"] == 1

    member_limited = _store(
        tmp_path / "member-limited",
        limits=RawArtifactLimits(max_capture_members=1),
    )
    member_a = _put(member_limited, payload=b"a")
    member_b = _put(
        member_limited,
        fetch_id="job-2:native-object",
        upstream_id="b",
        payload=b"b",
    )
    with pytest.raises(RawArtifactEntryCapacityError) as member_error:
        member_limited.commit_capture(
            provider="kmi",
            product="too-many-members",
            fetch_id="capture-1",
            upstream_capture_id="upstream-capture-1",
            product_timestamp=None,
            members={"a": member_a.receipt_id, "b": member_b.receipt_id},
        )
    assert member_error.value.scope == "capture_members"
    assert member_limited.stats()["capture_count"] == 0


def test_authoritative_scan_catches_in_place_growth_and_unknown_entry_on_retry(
    tmp_path,
):
    store = _store(
        tmp_path / "raw",
        limits=RawArtifactLimits(
            max_object_bytes=20,
            max_total_bytes=20,
        ),
    )
    first = _put(store, payload=b"abc")
    Path(first.artifact_path).write_bytes(b"x" * 19)

    with pytest.raises(RawArtifactCapacityError) as capacity:
        _put(
            store,
            fetch_id="job-2:native-object",
            upstream_id="second",
            payload=b"yz",
        )
    assert capacity.value.scope == "total"
    assert store.stats()["receipt_count"] == 1

    unknown_store = _store(tmp_path / "unknown")
    receipt = _put(unknown_store, payload=b"exact")
    capture_arguments = {
        "provider": "kmi",
        "product": "one-member-capture",
        "fetch_id": "capture-1",
        "upstream_capture_id": "upstream-capture-1",
        "product_timestamp": None,
        "members": {"native": receipt.receipt_id},
    }
    unknown_store.commit_capture(**capture_arguments)
    (unknown_store.objects_path / "not-content-addressed").write_bytes(b"junk")
    with pytest.raises(RawArtifactIntegrityError, match="onbekende objects-entry"):
        _put(unknown_store, payload=b"exact")
    with pytest.raises(RawArtifactIntegrityError, match="onbekende objects-entry"):
        unknown_store.commit_capture(**capture_arguments)
    assert unknown_store.load_receipt(receipt.receipt_id) is not None


def test_catalog_uses_conservative_delete_journal_physical_envelope(tmp_path):
    maximum = 1024 * 1024
    store = _store(
        tmp_path / "raw",
        limits=RawArtifactLimits(max_catalog_bytes=maximum),
    )
    with closing(store._connect()) as database:
        mode = str(database.execute("PRAGMA journal_mode").fetchone()[0]).lower()
        page_size = int(database.execute("PRAGMA page_size").fetchone()[0])
        page_count = int(database.execute("PRAGMA page_count").fetchone()[0])
        max_pages = int(database.execute("PRAGMA max_page_count").fetchone()[0])

    stats = store.stats()
    assert mode == "delete"
    assert max_pages * page_size <= maximum // 4
    assert stats["catalog_page_size_bytes"] == page_size
    assert stats["catalog_main_database_bytes"] == page_count * page_size
    assert stats["catalog_main_database_max_bytes"] == max_pages * page_size
    assert stats["catalog_main_database_bytes"] <= (
        stats["catalog_main_database_max_bytes"]
    )
    assert stats["catalog_database_bytes"] <= maximum


def test_sqlite_full_is_normalized_and_never_exceeds_physical_envelope(tmp_path):
    maximum = 1024 * 1024
    store = _store(
        tmp_path / "raw",
        limits=RawArtifactLimits(max_catalog_bytes=maximum),
    )
    large_provenance = {
        **PROVENANCE,
        "bounded_note": "x" * 60_000,
    }
    raised = None
    for index in range(20):
        try:
            _put(
                store,
                fetch_id=f"job-{index}:native-object",
                upstream_id=f"frame-{index}",
                provenance=large_provenance,
                payload=b"catalog-bound",
            )
        except RawArtifactCapacityError as exc:
            raised = exc
            break

    assert raised is not None
    assert raised.scope == "catalog"
    assert store.stats()["catalog_database_bytes"] <= maximum


def test_pinned_reader_blocks_delete_journal_writer_without_wal_growth(
    tmp_path,
    monkeypatch,
):
    maximum = 1024 * 1024
    store = _store(
        tmp_path / "raw",
        limits=RawArtifactLimits(max_catalog_bytes=maximum),
    )
    _put(store, payload=b"shared-object")
    original_connect = store._connect

    def short_busy_connect():
        database = original_connect()
        database.execute("PRAGMA busy_timeout=100")
        return database

    reader = sqlite3.connect(store.catalog_path, isolation_level=None)
    try:
        assert str(reader.execute("PRAGMA journal_mode").fetchone()[0]).lower() == "delete"
        reader.execute("BEGIN")
        reader.execute("SELECT count(*) FROM raw_artifact_receipts").fetchone()
        monkeypatch.setattr(store, "_connect", short_busy_connect)

        with pytest.raises(sqlite3.OperationalError, match="locked"):
            _put(
                store,
                fetch_id="job-2:native-object",
                upstream_id="second-identity",
                payload=b"shared-object",
            )
    finally:
        reader.rollback()
        reader.close()

    assert store.stats()["receipt_count"] == 1
    assert store.stats()["catalog_database_bytes"] <= maximum
    assert not Path(f"{store.catalog_path}-wal").exists()


def test_unknown_storage_filename_fails_closed(tmp_path):
    store = _store(tmp_path / "raw")
    (store.objects_path / "not-content-addressed").write_bytes(b"junk")
    with pytest.raises(RawArtifactIntegrityError, match="onbekende objects-entry"):
        store.stats()


def test_usage_cache_is_telemetry_only_and_admission_always_rescans(
    tmp_path,
    monkeypatch,
):
    store = _store(tmp_path / "raw")
    original = store._scan_storage_uncached
    scans: list[object] = []

    def counted(fingerprints):
        scans.append(fingerprints)
        return original(fingerprints)

    monkeypatch.setattr(store, "_scan_storage_uncached", counted)
    first = _put(store, payload=b"first")
    assert store.read_bytes(first.receipt_id) == b"first"
    assert store.stats()["filesystem_bytes"] == len(b"first")
    assert store.stats()["unmanaged_file_count"] == 0
    assert len(scans) == 1

    # Read-only telemetry can reuse the hint, but an exact retry is still an
    # admission operation and must re-establish the complete physical layout.
    assert store.stats()["filesystem_bytes"] == len(b"first")
    assert len(scans) == 1
    assert _put(store, payload=b"first") == first
    assert len(scans) == 2

    external_payload = b"external-unmanaged"
    external_digest = hashlib.sha256(external_payload).hexdigest()
    (store.objects_path / f"{external_digest}.raw").write_bytes(external_payload)
    changed = store.stats()
    assert changed["unmanaged_file_count"] == 1
    assert len(scans) == 3
    assert store.stats()["unmanaged_file_count"] == 1
    assert len(scans) == 3


def test_publish_fsyncs_object_and_staging_directory_boundaries(
    tmp_path,
    monkeypatch,
):
    store = _store(tmp_path / "raw")
    synced: list[Path] = []
    monkeypatch.setattr(store, "_fsync_directory", lambda path: synced.append(path))

    _put(store, payload=b"durable")

    assert synced.count(store.objects_path) == 1
    assert synced.count(store.staging_path) == 2


def test_existing_hash_path_with_wrong_bytes_is_never_overwritten(tmp_path):
    store = _store(tmp_path / "raw")
    payload = b"expected-native-bytes"
    digest = hashlib.sha256(payload).hexdigest()
    target = tmp_path / "raw" / "objects" / f"{digest}.raw"
    target.write_bytes(b"occupied-by-different-bytes")

    with pytest.raises(RawArtifactIntegrityError):
        _put(store, payload=payload)

    assert target.read_bytes() == b"occupied-by-different-bytes"
    assert store.stats()["receipt_count"] == 0


def test_same_size_tampering_is_detected_before_replay(tmp_path):
    store = _store(tmp_path / "raw")
    receipt = _put(store, payload=b"native-123")
    Path(receipt.artifact_path).write_bytes(b"forged-123")

    with pytest.raises(RawArtifactIntegrityError, match="SHA-256"):
        store.read_bytes(receipt.receipt_id)


def test_catalog_path_traversal_is_rejected_before_filesystem_access(tmp_path):
    root = tmp_path / "raw"
    store = _store(root)
    receipt = _put(store)
    outside = tmp_path / "outside.raw"
    outside.write_bytes(b"do-not-touch")
    with closing(sqlite3.connect(root / "raw_artifacts.sqlite3")) as database:
        database.execute("PRAGMA ignore_check_constraints=ON")
        database.execute(
            "UPDATE raw_artifact_objects SET relative_path='../outside.raw' "
            "WHERE content_sha256=?",
            (receipt.content_sha256,),
        )
        database.commit()

    with pytest.raises(RawArtifactIntegrityError, match="pad"):
        store.load_receipt(receipt.receipt_id)

    assert outside.read_bytes() == b"do-not-touch"


def test_symlink_at_content_address_is_rejected_without_touching_target(tmp_path):
    root = tmp_path / "raw"
    store = _store(root)
    payload = b"native-symlink-test"
    digest = hashlib.sha256(payload).hexdigest()
    outside = tmp_path / "outside.bin"
    outside.write_bytes(b"outside-evidence")
    target = root / "objects" / f"{digest}.raw"
    try:
        target.symlink_to(outside)
    except OSError as exc:
        pytest.skip(f"symlink creation unavailable: {exc}")

    with pytest.raises(RawArtifactIntegrityError, match="symlink"):
        _put(store, payload=payload)

    assert outside.read_bytes() == b"outside-evidence"


def test_old_artifact_is_not_removed_or_touched_by_reinitialize(tmp_path):
    root = tmp_path / "raw"
    store = _store(root)
    receipt = _put(store)
    path = Path(receipt.artifact_path)
    old_time = 1_000_000_000
    os.utime(path, (old_time, old_time))
    before = path.read_bytes()

    reopened = _store(root)

    assert path.read_bytes() == before
    assert int(path.stat().st_mtime) == old_time
    assert reopened.read_bytes(receipt.receipt_id) == before
    assert reopened.stats()["retention_policy"] == "none"


def test_exception_after_publish_leaves_no_partial_receipt_and_retry_adopts(
    tmp_path,
):
    root = tmp_path / "raw"

    def fail(name: str) -> None:
        if name == "store_after_object_publish":
            raise RuntimeError("simulated process failure")

    failing = _store(root, fault_injector=fail)
    with pytest.raises(RuntimeError, match="simulated"):
        _put(failing)

    reopened = _store(root)
    failed_stats = reopened.stats()
    assert failed_stats["object_count"] == 0
    assert failed_stats["receipt_count"] == 0
    assert failed_stats["unmanaged_file_count"] == 1

    receipt = _put(reopened)
    assert reopened.read_bytes(receipt.receipt_id).endswith(b"upstream-bytes")
    assert reopened.stats()["unmanaged_file_count"] == 0


@pytest.mark.parametrize(
    ("checkpoint", "committed"),
    (
        ("store_after_object_publish", False),
        ("store_before_commit", False),
        ("store_after_commit", True),
    ),
)
def test_abrupt_exit_exposes_no_partial_receipt(
    tmp_path, checkpoint, committed,
):
    root = tmp_path / checkpoint
    _store(root)
    helper = (
        Path(__file__).resolve().parent
        / "helpers/worker_raw_artifacts_crash_child.py"
    )
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])

    result = subprocess.run(
        [sys.executable, str(helper), str(root), checkpoint],
        cwd=Path(__file__).resolve().parents[1],
        env=environment,
        capture_output=True,
        check=False,
        timeout=20,
    )

    assert result.returncode == 73, result.stderr.decode(errors="replace")
    reopened = _store(root)
    stats = reopened.stats()
    assert reopened.integrity_check()["healthy"] is True
    if committed:
        assert stats["object_count"] == 1
        assert stats["receipt_count"] == 1
    else:
        assert stats["object_count"] == 0
        assert stats["receipt_count"] == 0
    receipt = _put(
        reopened,
        provider="test-provider",
        product="native-crash-product",
        fetch_id="crash-job:native-object",
        content_type="application/octet-stream",
        received_at=1_800_000_000.0,
        upstream_id="frame/2027-01-15T12:00:00Z",
        provenance={
            "representation": NATIVE_REPRESENTATION,
            "capture_stage": NATIVE_CAPTURE_STAGE,
            "collector": "crash-test",
            "collector_version": "1",
        },
        payload=b"\x00native-crash-evidence",
    )
    assert reopened.read_bytes(receipt.receipt_id) == b"\x00native-crash-evidence"
    assert reopened.stats()["object_count"] == 1
    assert reopened.stats()["receipt_count"] == 1


_RAW_STORE_LEGACY_NATIVE_POINTS = frozenset({
    "store_after_object_publish",
    "store_before_commit",
    "store_after_commit",
})
_RAW_STORE_FILE_NATIVE_POINTS = frozenset({
    "store_file_after_stage_open",
    "store_file_after_stage_fsync",
    "store_file_after_object_link",
})
_RAW_STORE_RECLAMATION_PHASE_NATIVE_POINTS = frozenset({
    "reclamation_prepare_before_commit",
    "reclamation_prepare_after_commit",
    "reclamation_abort_before_commit",
    "reclamation_abort_after_commit",
})
_RAW_STORE_RECLAMATION_APPLY_NATIVE_POINTS = frozenset({
    "reclamation_catalog_before_commit",
    "reclamation_catalog_after_commit",
    "reclamation_unlink_after_preunlink_reproof",
    "reclamation_unlink_after_unlink",
    "reclamation_unlink_event_after_commit",
    "reclamation_unlinked_before_commit",
    "reclamation_unlinked_after_commit",
    "reclamation_apply_before_commit",
    "reclamation_apply_after_commit",
})
_RAW_STORE_REMAINING_NATIVE_CASES = (
    ("store", "store_after_begin", False, 0),
    ("store", "store_before_receipt", False, 1),
    ("capture", "capture_after_begin", False, 0),
    ("capture", "capture_before_commit", False, 0),
    ("capture", "capture_after_commit", True, 0),
)


def _raw_store_checkpoint_inventory() -> tuple[str, ...]:
    source_path = Path(sys.modules[RawArtifactStore.__module__].__file__)
    tree = ast.parse(
        source_path.read_text(encoding="utf-8"),
        filename=str(source_path),
    )
    store_class = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "RawArtifactStore"
    )
    calls = tuple(
        call
        for call in ast.walk(store_class)
        if (
            isinstance(call, ast.Call)
            and isinstance(call.func, ast.Attribute)
            and isinstance(call.func.value, ast.Name)
            and call.func.value.id == "self"
            and call.func.attr == "_checkpoint"
            and len(call.args) == 1
            and isinstance(call.args[0], ast.Constant)
            and isinstance(call.args[0].value, str)
        )
    )
    return tuple(
        str(call.args[0].value)
        for call in sorted(calls, key=lambda item: (item.lineno, item.col_offset))
    )


def test_raw_store_native_crash_inventory_deduplicates_exact_remaining_five():
    discovered = _raw_store_checkpoint_inventory()
    discovered_set = frozenset(discovered)
    remaining = frozenset(
        checkpoint
        for _operation, checkpoint, _committed, _unmanaged
        in _RAW_STORE_REMAINING_NATIVE_CASES
    )
    partitions = (
        _RAW_STORE_LEGACY_NATIVE_POINTS,
        _RAW_STORE_FILE_NATIVE_POINTS,
        _RAW_STORE_RECLAMATION_PHASE_NATIVE_POINTS,
        _RAW_STORE_RECLAMATION_APPLY_NATIVE_POINTS,
        remaining,
    )
    assert len(discovered) == len(discovered_set) == 24
    assert tuple(map(len, partitions)) == (3, 3, 4, 9, 5)
    assert all(
        left.isdisjoint(right)
        for index, left in enumerate(partitions)
        for right in partitions[index + 1:]
    )
    assert frozenset().union(*partitions) == discovered_set


def _raw_store_remaining_put(
    store: RawArtifactStore,
    *,
    product: str,
    role: str,
    payload: bytes,
):
    return store.store(
        provider="kmi",
        product=product,
        fetch_id=f"raw-store-crash:{role}",
        content_type="application/octet-stream",
        received_at=1_800_200_000.0 if role == "first" else 1_800_200_001.0,
        upstream_id=f"raw-store-crash/{role}/revision-1",
        provenance={
            "representation": NATIVE_REPRESENTATION,
            "capture_stage": NATIVE_CAPTURE_STAGE,
            "collector": "raw-store-crash-test",
            "collector_version": "1",
        },
        payload=payload,
    )


def _raw_store_remaining_capture(
    store: RawArtifactStore,
    first,
    second,
):
    return store.commit_capture(
        provider="kmi",
        product="raw-store-crash-capture",
        fetch_id="raw-store-crash:capture",
        upstream_capture_id="raw-store-crash/capture/revision-1",
        product_timestamp=1_700_000_000.0,
        members={"first": first.receipt_id, "second": second.receipt_id},
    )


@pytest.mark.parametrize(
    ("operation", "checkpoint", "committed", "unmanaged_at_crash"),
    _RAW_STORE_REMAINING_NATIVE_CASES,
    ids=(
        "store-after-begin-old",
        "store-before-receipt-old-orphan-adopted",
        "capture-after-begin-old",
        "capture-before-commit-old",
        "capture-after-commit-new",
    ),
)
def test_raw_store_remaining_native_crash_recovers_and_replays_exactly(
    tmp_path: Path,
    operation: str,
    checkpoint: str,
    committed: bool,
    unmanaged_at_crash: int,
) -> None:
    root = (tmp_path / checkpoint).resolve()
    _store(root)
    helper = (
        Path(__file__).resolve().parent
        / "helpers"
        / "worker_raw_artifacts_remaining_crash_child.py"
    )
    environment = dict(os.environ)
    repository = str(Path(__file__).resolve().parents[1])
    environment["PYTHONPATH"] = os.pathsep.join(
        value
        for value in (
            str(Path(repository) / ".testdeps"),
            repository,
            environment.get("PYTHONPATH", ""),
        )
        if value
    )
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    crashed = subprocess.run(
        [sys.executable, str(helper), str(root), operation, checkpoint],
        cwd=repository,
        env=environment,
        capture_output=True,
        check=False,
        timeout=30.0,
    )
    assert crashed.returncode == 73
    assert crashed.stdout == b""
    assert crashed.stderr == b""

    reopened = _store(root)
    at_crash = reopened.stats()
    assert reopened.integrity_check()["healthy"] is True
    if operation == "store":
        assert at_crash["object_count"] == 0
        assert at_crash["receipt_count"] == 0
        assert at_crash["capture_count"] == 0
        assert at_crash["unmanaged_file_count"] == unmanaged_at_crash
        receipt = _raw_store_remaining_put(
            reopened,
            product="first-native",
            role="first",
            payload=b"exact-first-native-bytes",
        )
        assert reopened.read_bytes(receipt.receipt_id) == b"exact-first-native-bytes"
        stable = reopened.stats()
        replay_store = _store(root)
        replay = _raw_store_remaining_put(
            replay_store,
            product="first-native",
            role="first",
            payload=b"exact-first-native-bytes",
        )
        assert replay == receipt
        assert replay.receipt_sha256 == receipt.receipt_sha256
        assert replay_store.stats() == stable
        assert stable["object_count"] == stable["receipt_count"] == 1
        assert stable["capture_count"] == stable["unmanaged_file_count"] == 0
    else:
        assert at_crash["object_count"] == 2
        assert at_crash["receipt_count"] == 2
        assert at_crash["capture_count"] == int(committed)
        assert at_crash["unmanaged_file_count"] == 0
        first = _raw_store_remaining_put(
            reopened,
            product="first-native",
            role="first",
            payload=b"exact-first-native-bytes",
        )
        second = _raw_store_remaining_put(
            reopened,
            product="second-native",
            role="second",
            payload=b"exact-second-native-bytes",
        )
        capture = _raw_store_remaining_capture(reopened, first, second)
        assert reopened.load_capture(capture.capture_id) == capture
        stable = reopened.stats()
        replay_store = _store(root)
        replay_first = _raw_store_remaining_put(
            replay_store,
            product="first-native",
            role="first",
            payload=b"exact-first-native-bytes",
        )
        replay_second = _raw_store_remaining_put(
            replay_store,
            product="second-native",
            role="second",
            payload=b"exact-second-native-bytes",
        )
        replay = _raw_store_remaining_capture(
            replay_store,
            replay_first,
            replay_second,
        )
        assert replay == capture
        assert replay.capture_receipt_sha256 == capture.capture_receipt_sha256
        assert replay_store.stats() == stable
        assert stable["object_count"] == stable["receipt_count"] == 2
        assert stable["capture_count"] == 1
        assert stable["unmanaged_file_count"] == 0
        assert replay_store.load_capture_bundle_bytes(capture.capture_id)[2] == {
            "first": b"exact-first-native-bytes",
            "second": b"exact-second-native-bytes",
        }
    assert replay_store.integrity_check()["healthy"] is True


def test_catalog_is_explicit_and_receipt_commit_is_atomic(tmp_path):
    root = tmp_path / "raw"
    store = _store(root)
    receipt = _put(store)
    with closing(sqlite3.connect(root / "raw_artifacts.sqlite3")) as database:
        row = database.execute(
            """
            SELECT provider, product, fetch_id, content_type, received_at, upstream_id,
                   provenance_json, content_sha256, size_bytes
            FROM raw_artifact_receipts WHERE receipt_id=?
            """,
            (receipt.receipt_id,),
        ).fetchone()

    assert row[:6] == (
        "kmi",
        "generic-native-product",
        "job-1:native-object",
        "application/x-hdf5",
        1_800_000_000.25,
        "native/2027-01-15T12:00:00Z/revision-1",
    )
    assert row[7:] == (receipt.content_sha256, receipt.size_bytes)


def test_store_requires_exact_bytes_and_explicit_initialize(tmp_path):
    root = tmp_path / "raw"
    store = RawArtifactStore(root)
    with pytest.raises(RuntimeError, match="initialize"):
        _put(store)

    store.initialize()
    with pytest.raises(TypeError, match="exact bytes"):
        _put(store, payload=bytearray(b"mutable"))
    assert store.stats()["receipt_count"] == 0
