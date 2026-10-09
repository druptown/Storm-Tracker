"""Red-team proofs for raw-capture/capacity-ledger crash recovery."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import csv
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import tarfile
from types import SimpleNamespace

import pytest

from storm_tracker_worker import acquisition_activation as activation
from storm_tracker_worker import acquisition_capture_recovery as recovery
from storm_tracker_worker import dwd_rv_native
from storm_tracker_worker import knmi_native
from storm_tracker_worker import metar_native
from storm_tracker_worker import opera_native
from storm_tracker_worker.raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RawArtifactLimits,
    RawArtifactStore,
)
from storm_tracker_worker.raw_capacity_ledger import (
    ProviderCapacityLimit,
    RawCapacityLedger,
    RawCapacityProfile,
)


NOW = 1_800_000_000.0
JOB_A = "acquisition:" + "a" * 64
JOB_B = "acquisition:" + "b" * 64
SECRET_SENTINEL = "must-not-enter-recovery-7f52"
REPOSITORY = Path(__file__).resolve().parents[1]
CRASH_HELPER = (
    REPOSITORY / "tests" / "helpers" / "provider_crash_durable_bridge_child.py"
)


def _limits(
    *,
    total: int = 64 * 1024 * 1024,
    max_object: int | None = None,
) -> RawArtifactLimits:
    return RawArtifactLimits(
        max_object_bytes=min(
            40 * 1024 * 1024 if max_object is None else max_object,
            total,
        ),
        max_total_bytes=total,
        max_object_entries=32,
        max_staging_entries=4,
        max_receipt_entries=64,
        max_capture_entries=64,
        max_capture_members=4,
        max_catalog_bytes=8 * 1024 * 1024,
    )


def _profile(*, total: int = 64 * 1024 * 1024) -> RawCapacityProfile:
    return RawCapacityProfile(
        global_max_bytes=total,
        global_max_entries=32,
        providers=(
            ProviderCapacityLimit("aviationweather", total, 32),
            ProviderCapacityLimit("dwd", total, 32),
            ProviderCapacityLimit("knmi", total, 32),
            ProviderCapacityLimit("opera", total, 32),
        ),
        max_reservations=32,
        max_catalog_bytes=8 * 1024 * 1024,
        sqlite_busy_timeout_seconds=2.0,
    )


def _stores(
    tmp_path: Path,
    *,
    total: int = 64 * 1024 * 1024,
    max_object: int | None = None,
):
    store = RawArtifactStore(
        (tmp_path / "raw").resolve(),
        limits=_limits(total=total, max_object=max_object),
    )
    store.initialize()
    ledger = RawCapacityLedger(
        (tmp_path / "capacity.sqlite3").resolve(),
        profile=_profile(total=total),
        clock=lambda: NOW,
    )
    return store, ledger


def _reserve(
    ledger: RawCapacityLedger,
    binding: recovery.RawRecoveryBinding,
    *,
    job_id: str,
) -> str:
    reservation_id = "acqcap:" + hashlib.sha256(job_id.encode()).hexdigest()
    ledger.reserve(
        reservation_id=reservation_id,
        provider=binding.ledger_provider,
        job_identity=job_id,
        requested_bytes=binding.maximum_transport_bytes,
        requested_entries=1,
    )
    return reservation_id


def _provenance(
    binding: recovery.RawRecoveryBinding,
    *,
    size: int,
) -> dict:
    if binding.provenance_kind == "metar_v2":
        return {
            "representation": NATIVE_REPRESENTATION,
            "capture_stage": NATIVE_CAPTURE_STAGE,
            "collector": "aviationweather_metars_cache",
            "collector_version": "worker-metar-cache-exact-raw-v2",
            "structure_inspector_version": "aviationweather-metars-csv-structure-v2",
            "upstream_filename": "metars.cache.csv.gz",
            "declared_semantic_class": "raw_terminal_observation_reports",
            "coverage": "worldwide_current_reports",
            "provider_update_cadence_seconds": 60,
            "meteorological_interpretation_verified_after_decode": False,
        }
    if binding.provenance_kind == "dwd_rv_v2":
        return {
            "representation": NATIVE_REPRESENTATION,
            "capture_stage": NATIVE_CAPTURE_STAGE,
            "collector": "dwd_open_data_composite_rv",
            "collector_version": "worker-dwd-rv-tar-exact-raw-v2",
            "structure_inspector_version": "dwd-rv-tar-layout-only-v1",
            "provenance_scope": "exact_upstream_tar_bytes_only",
        }
    return {
        "representation": NATIVE_REPRESENTATION,
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": "knmi_open_data_native_file",
        "collector_version": "worker-knmi-rtcor-5m-exact-raw-v1",
        "structure_inspector_version": "knmi-rtcor-live-golden-identity-v2",
        "dataset_name": "nl_rdr_data_rtcor_5m",
        "dataset_version": "1.0",
        "dataset_license": "CC-BY-4.0",
        "selection_order": "created_descending",
        "selection_limit": 1,
        "upstream_filename": "RAD_NL25_RAC_RT_202701151200.h5",
        "listing_created": "2027-01-15T12:00:00Z",
        "listing_last_modified": "2027-01-15T12:00:00Z",
        "listing_size_bytes": size,
        "download_last_modified": "2027-01-15T12:00:00Z",
        "download_size_bytes": size,
        "download_content_type": "application/x-hdf5",
        "declared_semantic_class": "quantitative_precipitation_accumulation",
        "declared_quantity": "precipitation_accumulation",
        "declared_unit": "mm",
        "declared_accumulation_minutes": 5.0,
        "quantitative_semantics_verified_after_decode": False,
    }


def _materialize(
    store: RawArtifactStore,
    binding: recovery.RawRecoveryBinding,
    *,
    job_id: str,
    size: int = 211,
    commit_capture: bool = True,
):
    payload = (binding.handler_id.encode() * (size // len(binding.handler_id) + 1))[:size]
    content_sha = hashlib.sha256(payload).hexdigest()
    provenance = _provenance(binding, size=size)
    if binding.provenance_kind == "metar_v2":
        content_type = "application/gzip"
        upstream_id = f"aviationweather/metars.cache.csv.gz|content_sha256={content_sha}"
        capture_upstream = (
            f"{binding.capture_product}|content_sha256={content_sha}"
        )
        product_timestamp = None
    elif binding.provenance_kind == "dwd_rv_v2":
        content_type = "application/x-tar"
        upstream_id = f"{binding.raw_product}|latest|content_sha256={content_sha}"
        capture_upstream = (
            f"{binding.capture_product}|member=composite_rv_20270115_1200_000-hd5"
            f"|content_sha256={content_sha}"
        )
        product_timestamp = 1_800_014_400.0
    else:
        content_type = "application/x-hdf5"
        filename = provenance["upstream_filename"]
        upstream_id = f"nl_rdr_data_rtcor_5m/1.0|filename={filename}"
        product_timestamp = 1_800_014_400.0
        capture_upstream = (
            f"{binding.capture_product}|filename={filename}"
            f"|product_timestamp={product_timestamp:.6f}"
            f"|content_sha256={content_sha}"
        )
    receipt = store.store(
        provider=binding.raw_provider,
        product=binding.raw_product,
        fetch_id=f"{job_id}{binding.receipt_suffix}",
        content_type=content_type,
        received_at=NOW,
        upstream_id=upstream_id,
        provenance=provenance,
        payload=payload,
    )
    if not commit_capture:
        return receipt, None
    capture = store.commit_capture(
        provider=binding.raw_provider,
        product=binding.capture_product,
        fetch_id=f"{job_id}:capture",
        upstream_capture_id=capture_upstream,
        product_timestamp=product_timestamp,
        members={binding.role: receipt.receipt_id},
    )
    return receipt, capture


def _binding(workload_id: str = "radar.dwd.radolan_rv"):
    rule = activation.ACTIVATION_RULES[workload_id]
    return recovery.RECOVERY_BINDINGS[rule.handler_id]


def _raw_growth_proof(store: RawArtifactStore) -> tuple[int, ...]:
    stats = store.stats()
    return tuple(
        stats[field]
        for field in (
            "object_count",
            "receipt_count",
            "capture_count",
            "catalog_bytes",
            "filesystem_bytes",
            "filesystem_object_entries",
            "filesystem_staging_entries",
            "unmanaged_file_count",
        )
    )


def _raw_identity_proof(store: RawArtifactStore) -> tuple[tuple, tuple]:
    return (
        tuple(
            (
                item.receipt_id,
                item.content_sha256,
                item.provenance_sha256,
                item.receipt_sha256,
            )
            for item in store.accounting_receipts()
        ),
        tuple(
            (
                item.capture_id,
                item.manifest_sha256,
                item.capture_receipt_sha256,
                tuple(
                    (
                        member.role,
                        member.receipt_id,
                        member.content_sha256,
                        member.receipt_sha256,
                    )
                    for member in item.members
                ),
            )
            for item in store.accounting_captures()
        ),
    )


def _ledger_growth_proof(ledger: RawCapacityLedger) -> tuple[int, ...]:
    stats = ledger.stats()
    return tuple(
        stats[field]
        for field in (
            "reservation_rows",
            "reserved_count",
            "committed_count",
            "reserved_bytes",
            "committed_bytes",
            "reserved_entries",
            "committed_entries",
        )
    )


def test_recovery_bindings_leave_activation_matrix_exactly_unchanged():
    active = {
        rule.workload_id
        for rule in activation.ACTIVATION_RULES.values()
        if rule.executable
    }
    assert active == {
        binding.workload_id for binding in recovery.RECOVERY_BINDINGS.values()
    }
    assert set(recovery.RECOVERY_BINDINGS) == {
        rule.handler_id
        for rule in activation.ACTIVATION_RULES.values()
        if rule.executable
    }
    assert activation.ACTIVATION_RULES["radar.opera.dbzh"].executable
    assert not activation.ACTIVATION_RULES["radar.kmi.radqpe_native"].executable


@pytest.mark.parametrize(
    "workload_id",
    ["ground.metar.global", "radar.dwd.radolan_rv", "radar.knmi.rtcor"],
)
def test_startup_audit_recovers_each_exact_active_raw_binding_once(
    tmp_path,
    workload_id,
    monkeypatch,
):
    binding = _binding(workload_id)
    store, ledger = _stores(tmp_path)
    reservation_id = _reserve(ledger, binding, job_id=JOB_A)
    receipt, capture = _materialize(store, binding, job_id=JOB_A)
    object_path = Path(receipt.artifact_path)
    before = (object_path.read_bytes(), object_path.stat().st_mtime_ns)

    first = recovery.CaptureCapacityReconciler(
        raw_store=store,
        capacity_ledger=ledger,
    ).audit_existing()
    committed = ledger.load(reservation_id)
    assert first.recovered_commit_count == 1
    assert committed is not None and committed.status == "COMMITTED"
    assert committed.content_sha256 == receipt.content_sha256
    assert committed.receipt_sha256 == receipt.receipt_sha256
    assert committed.capture_sha256 == capture.capture_receipt_sha256
    assert (object_path.read_bytes(), object_path.stat().st_mtime_ns) == before

    reopened_store = RawArtifactStore(store.root, limits=store.limits)
    reopened_store.initialize()
    monkeypatch.setattr(
        reopened_store,
        "_read_verified",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError(
                "already committed startup recovery may not rehash payloads"
            )
        ),
    )
    replay = recovery.CaptureCapacityReconciler(
        raw_store=reopened_store,
        capacity_ledger=RawCapacityLedger(
            ledger.path,
            profile=ledger.profile,
            clock=lambda: NOW + 1,
        ),
    ).audit_existing()
    assert replay.recovered_commit_count == 0
    assert replay.already_committed_count == 1
    assert ledger.stats()["reservation_rows"] == 1
    assert ledger.stats()["committed_count"] == 1


def test_actual_metar_native_capture_contract_is_recoverable(monkeypatch, tmp_path):
    binding = _binding("ground.metar.global")
    store, ledger = _stores(tmp_path)
    reservation_id = _reserve(ledger, binding, job_id=JOB_A)
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=list(metar_native.METAR_EXPECTED_HEADER))
    writer.writeheader()
    writer.writerow({
        "raw_text": "METAR EBAW 151200Z -RA",
        "station_id": "EBAW",
        "observation_time": "2027-01-15T12:00:00Z",
        "latitude": "51.1894",
        "longitude": "4.4603",
        "wx_string": "-RA",
        "metar_type": "METAR",
    })
    payload = gzip.compress(stream.getvalue().encode(), mtime=0)
    monkeypatch.setattr(
        metar_native,
        "_download_cache",
        lambda: metar_native._MetarHttpResponse(
            payload,
            "application/gzip",
            datetime(2027, 1, 15, 12, 1, tzinfo=timezone.utc).timestamp(),
        ),
    )
    metar_native.capture_latest_metar_cache(
        SimpleNamespace(job_id=JOB_A, payload={}),
        raw_store=store,
    )
    recovery.CaptureCapacityReconciler(
        raw_store=store,
        capacity_ledger=ledger,
    ).audit_existing()
    assert ledger.load(reservation_id).status == "COMMITTED"


def test_actual_dwd_raw_only_capture_contract_is_recoverable(monkeypatch, tmp_path):
    binding = _binding("radar.dwd.radolan_rv")
    store, ledger = _stores(tmp_path)
    reservation_id = _reserve(ledger, binding, job_id=JOB_A)
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w") as archive:
        for suffix in ("000", "005"):
            payload = b"opaque-hdf5-member"
            member = tarfile.TarInfo(
                f"composite_rv_20270115_1200_{suffix}-hd5"
            )
            member.size = len(payload)
            archive.addfile(member, io.BytesIO(payload))
    payload = stream.getvalue()
    monkeypatch.setattr(
        dwd_rv_native,
        "_download",
        lambda: dwd_rv_native.DwdRvHttpResponse(
            payload,
            "application/x-tar",
            datetime(2027, 1, 15, 12, 1, tzinfo=timezone.utc).timestamp(),
        ),
    )
    dwd_rv_native.capture_latest_dwd_rv_tar(
        SimpleNamespace(job_id=JOB_A, payload={}),
        raw_store=store,
    )
    recovery.CaptureCapacityReconciler(
        raw_store=store,
        capacity_ledger=ledger,
    ).audit_existing()
    assert ledger.load(reservation_id).status == "COMMITTED"


def test_actual_opera_stream_capture_recovery_is_reference_hash_only(
    monkeypatch,
    tmp_path,
):
    binding = _binding("radar.opera.dbzh")
    total = 768 * 1024 * 1024
    store, ledger = _stores(
        tmp_path,
        total=total,
        max_object=384 * 1024 * 1024,
    )
    reservation_id = _reserve(ledger, binding, job_id=JOB_A)
    payload = b"opaque-opera-object-for-reference-only-recovery"
    revision = opera_native.OperaRevisionIdentity(
        etag='"0123456789abcdef0123456789abcdef"',
        version_id=None,
        content_length=len(payload),
        content_type="application/x-hdf5",
    )

    monkeypatch.setattr(
        opera_native,
        "_head_hdf5",
        lambda _object_key, *, deadline: revision,
    )

    def download(_object_key, observed, *, deadline, destination_path):
        assert observed == revision and deadline > 0
        path = Path(destination_path)
        path.write_bytes(payload)
        return opera_native.OperaDownloadedFile(
            path=str(path),
            size_bytes=len(payload),
            content_sha256=hashlib.sha256(payload).hexdigest(),
            content_type=revision.content_type,
            received_at=NOW,
            etag=revision.etag,
            version_id=revision.version_id,
        )

    monkeypatch.setattr(opera_native, "_download_hdf5", download)
    result = opera_native.capture_opera_dbzh_hdf5_raw_only(
        SimpleNamespace(
            job_id=opera_native._canonical_job_id(NOW),
            payload={"product_timestamp": NOW},
        ),
        raw_store=store,
        raw_identity=JOB_A,
    )
    capture_id = result["raw_provenance"]["raw_capture_id"]
    receipt_id = result["raw_provenance"]["raw_sources"]["hdf5"][
        "raw_receipt_id"
    ]
    receipt = store.load_receipt(receipt_id)
    assert receipt is not None
    object_path = Path(receipt.artifact_path)
    before = (object_path.stat().st_size, object_path.stat().st_mtime_ns)
    monkeypatch.setattr(
        store,
        "load_capture_bundle_bytes",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("OPERA recovery mag geen HDF5-payload materialiseren")
        ),
    )

    first = recovery.CaptureCapacityReconciler(
        raw_store=store,
        capacity_ledger=ledger,
    ).audit_existing()
    committed = ledger.load(reservation_id)
    assert first.recovered_commit_count == 1
    assert committed is not None and committed.status == "COMMITTED"
    assert committed.actual_bytes == len(payload)
    assert committed.actual_entries == 1
    assert store.load_capture(capture_id) is not None
    assert (object_path.stat().st_size, object_path.stat().st_mtime_ns) == before

    replay = recovery.CaptureCapacityReconciler(
        raw_store=store,
        capacity_ledger=ledger,
    ).audit_existing()
    assert replay.recovered_commit_count == 0
    assert replay.already_committed_count == 1
    assert (object_path.stat().st_size, object_path.stat().st_mtime_ns) == before

    raw_capture = store.load_capture(capture_id)
    assert raw_capture is not None
    certification_sha256 = "a" * 64
    store.commit_capture(
        provider=raw_capture.provider,
        product=raw_capture.product,
        fetch_id=(
            f"{raw_capture.fetch_id}:certified:{certification_sha256}"
        ),
        upstream_capture_id=(
            f"{raw_capture.upstream_capture_id}"
            f"|decoder_certification_sha256={certification_sha256}"
        ),
        product_timestamp=NOW,
        members={
            member.role: member.receipt_id
            for member in raw_capture.members
        },
    )
    certified_replay = recovery.CaptureCapacityReconciler(
        raw_store=store,
        capacity_ledger=ledger,
    ).audit_existing()
    assert certified_replay.capture_count == 2
    assert certified_replay.recovered_commit_count == 0
    assert certified_replay.already_committed_count == 1


def test_actual_knmi_native_writer_contract_is_secret_free_and_recoverable(
    monkeypatch,
    tmp_path,
):
    binding = _binding("radar.knmi.rtcor")
    store, ledger = _stores(tmp_path)
    reservation_id = _reserve(ledger, binding, job_id=JOB_A)
    payload = b"opaque-native-hdf5-bytes"
    filename = "RAD_NL25_RAC_RT_202701151200.h5"
    modified = "2027-01-15T12:00:15Z"
    summary = knmi_native.KnmiFileSummary(
        filename=filename,
        last_modified=modified,
        last_modified_timestamp=datetime.fromisoformat(
            modified.replace("Z", "+00:00")
        ).timestamp(),
        size_bytes=len(payload),
        created="2027-01-15T12:00:16Z",
        created_timestamp=datetime.fromisoformat(
            "2027-01-15T12:00:16+00:00"
        ).timestamp(),
    )
    descriptor = knmi_native.KnmiDownloadDescriptor(
        content_type="application/x-hdf5",
        size_bytes=len(payload),
        last_modified=modified,
        last_modified_timestamp=summary.last_modified_timestamp,
        temporary_download_url=(
            "https://knmi-shadow.s3.eu-west-1.amazonaws.com/native.h5"
            "?X-Amz-Credential=ephemeral&X-Amz-Signature=never-persist"
        ),
    )
    product_end = datetime(2027, 1, 15, 12, 0, tzinfo=timezone.utc).timestamp()
    identity = knmi_native.KnmiHdfIdentity(
        product_group_name="RAD_NL25_COR_NA",
        image_product_name="RAD_NL25_COR_NA",
        image_geo_parameter="PRECIP_[MM]",
        hdf_tag_version="3.6",
        product_start=product_end - 300.0,
        product_end=product_end,
        image_rows=765,
        image_columns=700,
    )
    monkeypatch.setenv("STV3_KNMI_API_KEY", SECRET_SENTINEL)
    monkeypatch.setattr(knmi_native, "_latest_filename", lambda **_kwargs: summary)
    monkeypatch.setattr(
        knmi_native,
        "_temporary_download_url",
        lambda *_args, **_kwargs: descriptor,
    )
    monkeypatch.setattr(
        knmi_native,
        "_download_hdf5",
        lambda _descriptor: knmi_native._HttpBytes(
            payload,
            "application/x-hdf5",
            product_end + 60.0,
        ),
    )
    monkeypatch.setattr(
        knmi_native,
        "_inspect_hdf_identity_cached",
        lambda *_args, **_kwargs: identity,
    )
    knmi_native.capture_latest_knmi_hdf5(
        SimpleNamespace(job_id=JOB_A, payload={}),
        raw_store=store,
    )
    recovery.CaptureCapacityReconciler(
        raw_store=store,
        capacity_ledger=ledger,
    ).audit_existing()
    assert ledger.load(reservation_id).status == "COMMITTED"
    for path in tmp_path.rglob("*"):
        if path.is_file():
            assert SECRET_SENTINEL.encode() not in path.read_bytes()


@pytest.mark.parametrize(
    ("fault_point", "status_after_crash"),
    (
        pytest.param(
            "reconcile.after_capture_verified",
            "RESERVED",
            id="after_capture_verified",
        ),
        pytest.param(
            "reconcile.before_capacity_commit",
            "RESERVED",
            id="before_capacity_commit",
        ),
        pytest.param(
            "reconcile.after_capacity_commit",
            "COMMITTED",
            id="after_capacity_commit",
        ),
    ),
)
def test_real_process_crash_recovery_commits_exact_evidence_once(
    tmp_path,
    fault_point,
    status_after_crash,
):
    binding = _binding()
    assert binding.workload_id == "radar.dwd.radolan_rv"
    assert binding.maximum_artifacts == 1
    store, ledger = _stores(tmp_path)
    reservation_id = _reserve(ledger, binding, job_id=JOB_A)
    receipt, capture = _materialize(
        store,
        binding,
        job_id=JOB_A,
        size=211,
    )
    expected_payload = store.read_bytes(receipt.receipt_id)
    assert len(expected_payload) == 211
    expected_evidence = recovery.recovery_evidence_digest(
        binding=binding,
        receipts_by_role={binding.role: receipt},
        capture=capture,
    )
    raw_growth = _raw_growth_proof(store)
    raw_identities = _raw_identity_proof(store)
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(
        value
        for value in (str(REPOSITORY), environment.get("PYTHONPATH"))
        if value
    )
    completed = subprocess.run(
        [
            sys.executable,
            str(CRASH_HELPER),
            "acquisition-recovery",
            str(tmp_path.resolve()),
            fault_point,
        ],
        cwd=REPOSITORY,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 73, (completed.stdout, completed.stderr)

    inspected_store = RawArtifactStore(store.root, limits=store.limits)
    inspected_store.initialize()
    inspected_ledger = RawCapacityLedger(
        ledger.path,
        profile=ledger.profile,
        clock=lambda: NOW + 2.0,
    )
    after_crash = inspected_ledger.load(reservation_id)
    assert after_crash is not None and after_crash.status == status_after_crash
    assert inspected_store.stats()["object_count"] == 1
    assert inspected_store.stats()["receipt_count"] == 1
    assert inspected_store.stats()["capture_count"] == 1
    assert _raw_growth_proof(inspected_store) == raw_growth
    assert _raw_identity_proof(inspected_store) == raw_identities
    assert inspected_store.integrity_check()["healthy"] is True
    inspected_audit = inspected_ledger.audit()
    assert inspected_audit["reservation_rows"] == 1
    assert inspected_audit["reserved_count"] == int(status_after_crash == "RESERVED")
    assert inspected_audit["committed_count"] == int(
        status_after_crash == "COMMITTED"
    )

    inspected_bundle = inspected_store.load_capture_bundle_bytes(capture.capture_id)
    assert inspected_bundle is not None
    inspected_evidence = recovery.recovery_evidence_digest(
        binding=binding,
        receipts_by_role=inspected_bundle[1],
        capture=inspected_bundle[0],
    )
    assert inspected_evidence == expected_evidence
    assert inspected_bundle[2] == {binding.role: expected_payload}

    recovery_store = RawArtifactStore(store.root, limits=store.limits)
    recovery_store.initialize()
    recovery_ledger = RawCapacityLedger(
        ledger.path,
        profile=ledger.profile,
        clock=lambda: NOW + 1.0,
    )
    recovered = recovery.CaptureCapacityReconciler(
        raw_store=recovery_store,
        capacity_ledger=recovery_ledger,
    ).audit_existing()
    assert recovered.recovered_commit_count == int(status_after_crash == "RESERVED")
    assert recovered.already_committed_count == int(
        status_after_crash == "COMMITTED"
    )
    committed = recovery_ledger.load(reservation_id)
    assert committed is not None and committed.status == "COMMITTED"
    assert committed.actual_bytes == expected_evidence.actual_bytes == 211
    assert committed.actual_entries == expected_evidence.actual_entries == 1
    assert committed.content_sha256 == expected_evidence.content_sha256
    assert committed.receipt_sha256 == expected_evidence.receipt_sha256
    assert committed.capture_sha256 == expected_evidence.capture_sha256
    recovered_raw_growth = _raw_growth_proof(recovery_store)
    recovered_raw_identities = _raw_identity_proof(recovery_store)
    recovered_ledger_growth = _ledger_growth_proof(recovery_ledger)

    replay_store = RawArtifactStore(store.root, limits=store.limits)
    replay_store.initialize()
    replay_ledger = RawCapacityLedger(
        ledger.path,
        profile=ledger.profile,
        clock=lambda: NOW + 3.0,
    )
    replayed = recovery.CaptureCapacityReconciler(
        raw_store=replay_store,
        capacity_ledger=replay_ledger,
    ).audit_existing()
    assert replayed.recovered_commit_count == 0
    assert replayed.already_committed_count == 1
    replayed_row = replay_ledger.load(reservation_id)
    assert replayed_row == committed
    replayed_rows = replay_ledger.accounting_reservations(max_rows=32)
    assert replayed_rows == (committed,)
    replay_bundle = replay_store.load_capture_bundle_bytes(capture.capture_id)
    assert replay_bundle is not None
    replay_evidence = recovery.recovery_evidence_digest(
        binding=binding,
        receipts_by_role=replay_bundle[1],
        capture=replay_bundle[0],
    )
    assert replay_evidence == inspected_evidence == expected_evidence
    assert _raw_growth_proof(replay_store) == recovered_raw_growth == raw_growth
    assert _raw_identity_proof(replay_store) == recovered_raw_identities == raw_identities
    assert _ledger_growth_proof(replay_ledger) == recovered_ledger_growth
    assert replay_store.integrity_check()["healthy"] is True
    final_audit = replay_ledger.audit()
    assert final_audit["reservation_rows"] == 1
    assert final_audit["reserved_count"] == 0
    assert final_audit["committed_count"] == 1
    assert final_audit["committed_bytes"] == 211
    assert final_audit["committed_entries"] == 1


def test_two_concurrent_reconcilers_use_ledger_cas_without_double_accounting(tmp_path):
    binding = _binding()
    store, ledger = _stores(tmp_path)
    _reserve(ledger, binding, job_id=JOB_A)
    _materialize(store, binding, job_id=JOB_A, size=333)
    barrier = threading.Barrier(2)

    def run_one(_index):
        local_store = RawArtifactStore(store.root, limits=store.limits)
        local_store.initialize()
        local_ledger = RawCapacityLedger(
            ledger.path,
            profile=ledger.profile,
            clock=lambda: NOW + 1,
        )
        waited = {"value": False}

        def fault(point):
            if point == "reconcile.before_capacity_commit" and not waited["value"]:
                waited["value"] = True
                barrier.wait(timeout=10)

        return recovery.CaptureCapacityReconciler(
            raw_store=local_store,
            capacity_ledger=local_ledger,
            fault_injector=fault,
        ).audit_existing()

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(run_one, range(2)))
    assert len(results) == 2
    stats = ledger.stats()
    assert stats["reservation_rows"] == stats["committed_count"] == 1
    assert stats["committed_bytes"] == 333
    assert stats["committed_entries"] == 1


def test_corrupt_raw_object_fails_closed_without_capacity_commit(tmp_path):
    binding = _binding()
    store, ledger = _stores(tmp_path)
    reservation_id = _reserve(ledger, binding, job_id=JOB_A)
    receipt, _capture = _materialize(store, binding, job_id=JOB_A)
    path = Path(receipt.artifact_path)
    data = bytearray(path.read_bytes())
    data[0] ^= 0xFF
    path.write_bytes(bytes(data))

    reconciler = recovery.CaptureCapacityReconciler(
        raw_store=store,
        capacity_ledger=ledger,
    )
    with pytest.raises(recovery.CaptureRecoveryIntegrityError):
        reconciler.audit_existing()
    assert ledger.load(reservation_id).status == "RESERVED"
    assert reconciler.diagnostics()["failures"] == 1


def test_committed_ledger_with_missing_raw_catalog_fails_closed(tmp_path):
    binding = _binding()
    store, ledger = _stores(tmp_path)
    _reserve(ledger, binding, job_id=JOB_A)
    _materialize(store, binding, job_id=JOB_A)
    recovery.CaptureCapacityReconciler(
        raw_store=store,
        capacity_ledger=ledger,
    ).audit_existing()
    assert ledger.stats()["committed_count"] == 1

    wrong_empty_store = RawArtifactStore(
        (tmp_path / "wrong-empty-raw-root").resolve(),
        limits=store.limits,
    )
    wrong_empty_store.initialize()
    with pytest.raises(
        recovery.CaptureRecoveryIntegrityError,
        match="no durable raw capture",
    ):
        recovery.CaptureCapacityReconciler(
            raw_store=wrong_empty_store,
            capacity_ledger=ledger,
        ).audit_existing()


def test_reservation_inventory_bound_fails_closed_until_retention_exists(
    tmp_path,
    monkeypatch,
):
    store, ledger = _stores(tmp_path)
    dwd = _binding("radar.dwd.radolan_rv")
    metar = _binding("ground.metar.global")
    _reserve(ledger, dwd, job_id=JOB_A)
    _reserve(ledger, metar, job_id=JOB_B)
    monkeypatch.setattr(recovery, "MAX_RECOVERY_RESERVATIONS", 1)
    with pytest.raises(recovery.CaptureRecoveryCapacityError, match="recovery bound"):
        recovery.CaptureCapacityReconciler(
            raw_store=store,
            capacity_ledger=ledger,
        ).audit_existing()


def test_existing_unaccounted_bytes_are_rejected_even_inside_reserved_envelope(tmp_path):
    binding = _binding()
    store, ledger = _stores(tmp_path)
    _reserve(ledger, binding, job_id=JOB_A)
    # Aggregate bytes are smaller than JOB_A's reservation, but the receipt is
    # cryptographically bound to JOB_B and may never borrow JOB_A's capacity.
    _materialize(
        store,
        binding,
        job_id=JOB_B,
        size=17,
        commit_capture=False,
    )
    assert store.stats()["catalog_bytes"] < ledger.stats()["reserved_bytes"]
    with pytest.raises(
        recovery.CaptureRecoveryIntegrityError,
        match="without a durable prior",
    ):
        recovery.CaptureCapacityReconciler(
            raw_store=store,
            capacity_ledger=ledger,
        ).audit_existing()
    assert ledger.stats()["committed_count"] == 0


def test_fully_exhausted_capacity_still_closes_only_its_existing_reservation(tmp_path):
    binding = _binding()
    total = binding.maximum_transport_bytes
    store, ledger = _stores(tmp_path, total=total)
    reservation_id = _reserve(ledger, binding, job_id=JOB_A)
    assert ledger.stats()["reserved_bytes"] == total
    receipt, _capture = _materialize(store, binding, job_id=JOB_A, size=4096)

    recovery.CaptureCapacityReconciler(
        raw_store=store,
        capacity_ledger=ledger,
    ).audit_existing()
    committed = ledger.load(reservation_id)
    assert committed.status == "COMMITTED"
    assert committed.actual_bytes == receipt.size_bytes == 4096
    assert ledger.stats()["reservation_rows"] == 1


def test_recovery_diagnostics_are_bounded_and_identity_secret_free(tmp_path):
    binding = _binding()
    store, ledger = _stores(tmp_path)
    _reserve(ledger, binding, job_id=JOB_A)
    _materialize(store, binding, job_id=JOB_A)
    reconciler = recovery.CaptureCapacityReconciler(
        raw_store=store,
        capacity_ledger=ledger,
    )
    reconciler.audit_existing()
    diagnostics = reconciler.diagnostics()
    encoded = json.dumps(diagnostics, sort_keys=True).encode()
    assert len(encoded) <= recovery.MAX_DIAGNOSTIC_BYTES
    text = encoded.decode()
    assert JOB_A not in text
    assert SECRET_SENTINEL not in text
    assert str(store.root) not in text
    assert "handler_id" not in text and "workload_id" not in text
