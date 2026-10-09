from __future__ import annotations

from datetime import datetime, timezone
import gzip
from io import BytesIO
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
import time

import h5py
import numpy as np
import pytest

from storm_tracker_worker import hsaf_h40b_native as native
from storm_tracker_worker.raw_artifacts import (
    RawArtifactCapacityError,
    RawArtifactIntegrityError,
    RawArtifactLimits,
    RawArtifactStore,
)


ROOT = Path(__file__).resolve().parents[1]
CRASH_CHILD = (
    Path(__file__).parent / "helpers" / "hsaf_meteofrance_os_exit73_child.py"
)
CRASH_TIMEOUT_SECONDS = 20
OFFICIAL_CONTRACT = json.loads(
    (ROOT / "tests/fixtures/hsaf_h40b_official_contract.json").read_text(
        encoding="utf-8"
    )
)
USERNAME = "not-a-real-hsaf-user"
PASSWORD = "not-a-real-hsaf-password"
LATEST = "h40_20260802_1210_fdk.nc.gz"
LATEST_PATH = f"{native.FTP_DIRECTORY}/{LATEST}"
LATEST_TIME = datetime(2026, 8, 2, 12, 10, tzinfo=timezone.utc).timestamp()
LISTING = (
    f"{native.FTP_DIRECTORY}/h40_20260802_1150_fdk.nc.gz\r\n"
    f"{native.FTP_DIRECTORY}/h40_20260802_1200_fdk.nc.gz\r\n"
    f"{native.FTP_DIRECTORY}/{LATEST}\r\n"
).encode("ascii")


def _job(job_id: str = "hsaf-h40b-native-test"):
    return SimpleNamespace(job_id=job_id, payload={})


def _netcdf_gzip(
    *,
    include_qind: bool = True,
    rr_units: str = native.RR_UNIT,
    external_link: bool = False,
    extra_objects: int = 0,
) -> bytes:
    output = BytesIO()
    with h5py.File(output, "w") as product:
        product.attrs["Conventions"] = "CF-1.7"
        product.attrs["title"] = "Instantaneous rain rate"
        product.attrs["license"] = (
            "All intellectual property rights belong to EUMETSAT; "
            "Copyright EUMETSAT"
        )
        product.attrs["platform"] = "MTG1"
        product.attrs["algorithm_version"] = "1.0"
        product.attrs["parallax_correction"] = "Mode_on"

        nx = product.create_dataset(
            "nx", shape=(native.GRID_SIZE,), dtype="float32", chunks=(1024,)
        )
        nx.attrs["units"] = "m"
        nx.attrs["standard_name"] = "projection_x_coordinate"
        ny = product.create_dataset(
            "ny", shape=(native.GRID_SIZE,), dtype="float32", chunks=(1024,)
        )
        ny.attrs["units"] = "m"
        ny.attrs["standard_name"] = "projection_y_coordinate"

        projection = product.create_dataset(
            native.PROJECTION_VARIABLE, data=np.int8(0)
        )
        for name, value in native.PROJECTION_PARAMETERS.items():
            projection.attrs[name] = value

        rr = product.create_dataset(
            native.RR_VARIABLE,
            shape=(native.GRID_SIZE, native.GRID_SIZE),
            dtype=native.RR_DTYPE,
            chunks=(256, 256),
            fillvalue=native.RR_MISSING,
        )
        rr.attrs["valid_min"] = np.int16(native.RR_VALID_RANGE[0])
        rr.attrs["valid_max"] = np.int16(native.RR_VALID_RANGE[1])
        rr.attrs["long_name"] = "Instantaneous rain rate"
        rr.attrs["units"] = rr_units
        rr.attrs["add_offset"] = np.float64(native.RR_ADD_OFFSET)
        rr.attrs["scale_factor"] = np.float64(native.RR_SCALE_FACTOR)
        rr.attrs["missing_value"] = np.int16(native.RR_MISSING)
        rr.attrs["grid_mapping"] = native.PROJECTION_VARIABLE

        if include_qind:
            qind = product.create_dataset(
                native.QIND_VARIABLE,
                shape=(native.GRID_SIZE, native.GRID_SIZE),
                dtype=native.QIND_DTYPE,
                chunks=(256, 256),
                fillvalue=native.QIND_MISSING,
            )
            qind.attrs["valid_min"] = np.int8(native.QIND_VALID_RANGE[0])
            qind.attrs["valid_max"] = np.int8(native.QIND_VALID_RANGE[1])
            qind.attrs["long_name"] = "pixel quality index"
            qind.attrs["units"] = native.QIND_UNIT
            qind.attrs["missing_value"] = np.int8(native.QIND_MISSING)
            qind.attrs["grid_mapping"] = native.PROJECTION_VARIABLE
        if external_link:
            product["unsafe"] = h5py.ExternalLink(
                "missing-local-product.h5", "/rr"
            )
        for index in range(extra_objects):
            product.create_dataset(f"extra_{index:03d}", data=np.int8(index % 100))
    return gzip.compress(output.getvalue(), compresslevel=6, mtime=0)


def _response(
    payload: bytes,
    received_at: float,
    *,
    declared_size: int | None = None,
) -> native.HsafFtpResponse:
    return native.HsafFtpResponse(
        payload=payload,
        received_at=received_at,
        completion="226 Transfer complete",
        declared_size=declared_size,
    )


def _install_responses(monkeypatch, *, product: bytes | None = None):
    compressed = _netcdf_gzip() if product is None else product
    calls = {"credential": 0, "listing": 0, "product": 0}

    def credential():
        calls["credential"] += 1
        return native.HsafCredentials(USERNAME, PASSWORD)

    def listing(credentials, *, deadline):
        assert credentials == native.HsafCredentials(USERNAME, PASSWORD)
        assert deadline > 0
        calls["listing"] += 1
        return _response(LISTING, LATEST_TIME + 60)

    def fetch_product(credentials, remote_path, *, deadline):
        assert credentials == native.HsafCredentials(USERNAME, PASSWORD)
        assert remote_path == LATEST_PATH
        assert deadline > 0
        calls["product"] += 1
        return _response(
            compressed, LATEST_TIME + 61, declared_size=len(compressed)
        )

    monkeypatch.setattr(native, "_credentials_from_environment", credential)
    monkeypatch.setattr(native, "_fetch_listing", listing)
    monkeypatch.setattr(native, "_fetch_product", fetch_product)
    return calls, compressed


def _capture(monkeypatch, tmp_path, *, product: bytes | None = None):
    store = RawArtifactStore(tmp_path / "raw")
    calls, compressed = _install_responses(monkeypatch, product=product)
    result = native.capture_latest_hsaf_h40b(_job(), raw_store=store)
    return store, calls, compressed, result


def test_constants_match_official_pum_and_eumetsat_catalogue():
    assert OFFICIAL_CONTRACT["product_id"] == native.PRODUCT_ID
    assert OFFICIAL_CONTRACT["product_acronym"] == native.PRODUCT_ACRONYM
    assert OFFICIAL_CONTRACT["collection_reference"] == native.COLLECTION_REFERENCE
    assert OFFICIAL_CONTRACT["pum"]["reference"] == native.PUM_REFERENCE
    assert OFFICIAL_CONTRACT["pum"]["revision"] == native.PUM_REVISION
    assert OFFICIAL_CONTRACT["transport"]["host"] == native.FTP_HOST
    assert OFFICIAL_CONTRACT["transport"]["directory"] == native.FTP_DIRECTORY
    assert OFFICIAL_CONTRACT["product"]["grid_rows"] == native.GRID_SIZE
    assert OFFICIAL_CONTRACT["rr"]["units"] == native.RR_UNIT
    assert OFFICIAL_CONTRACT["qind"]["units"] == native.QIND_UNIT
    assert OFFICIAL_CONTRACT["data_policy"] == (
        "Free and unrestricted - CC-BY-4.0"
    )


def test_exact_listing_and_original_product_are_durable_and_not_admitted(
    monkeypatch, tmp_path
):
    store, calls, compressed, result = _capture(monkeypatch, tmp_path)
    assert calls == {"credential": 1, "listing": 1, "product": 1}
    assert store.stats()["receipt_count"] == 2
    assert store.stats()["capture_count"] == 1
    assert result["capture_state"] == "captured_not_decoded"
    assert result["weather_state"] == "unknown"
    assert result["pixel_values_read"] is False
    assert result["coordinate_values_read"] is False
    assert result["forecast_eligible"] is False
    assert result["operational_admission_eligible"] is False

    provenance = result["raw_provenance"]
    assert provenance["raw_store_schema_version"] == 3
    assert provenance["product_id"] == "H40B"
    assert provenance["product_acronym"] == "P-IN-FCI"
    assert provenance["selected_basename"] == LATEST
    assert provenance["selected_remote_path"] == LATEST_PATH
    assert provenance["nominal_slot_time"] == LATEST_TIME
    assert provenance["timestamp_semantics"].startswith("filename_nominal")
    assert provenance["transport_security"].startswith("plain_ftp")
    assert provenance["credentials_persisted"] is False
    assert provenance["licence"].startswith("CC-BY-4.0")
    assert provenance["unit"] == "mm/h"
    assert provenance["rr_raw_valid_range"] == [0, 2000]
    assert provenance["qind_valid_range"] == [0, 100]
    assert provenance["container"]["rr_shape"] == [5568, 5568]
    assert provenance["container"]["qind_shape"] == [5568, 5568]
    assert provenance["container"]["pixel_values_read"] is False

    sources = provenance["sources"]
    assert store.read_bytes(sources["listing_nlst"]["receipt_id"]) == LISTING
    assert (
        store.read_bytes(sources["netcdf_gzip"]["receipt_id"]) == compressed
    )
    durable = (
        store.read_bytes(sources["listing_nlst"]["receipt_id"])
        + store.read_bytes(sources["netcdf_gzip"]["receipt_id"])
        + json.dumps(result, sort_keys=True).encode()
    )
    assert USERNAME.encode() not in durable
    assert PASSWORD.encode() not in durable


def test_originals_are_stored_before_selection_and_container_inspection(
    monkeypatch, tmp_path
):
    store = RawArtifactStore(tmp_path / "raw")
    _, compressed = _install_responses(monkeypatch)
    inspect_listing = native._inspect_listing
    inspect_product = native._inspect_netcdf_gzip
    events: list[str] = []

    def listing(payload):
        assert payload == LISTING
        assert store.stats()["receipt_count"] == 1
        assert store.stats()["capture_count"] == 0
        events.append("listing")
        return inspect_listing(payload)

    def product(payload):
        assert payload == compressed
        assert store.stats()["receipt_count"] == 2
        assert store.stats()["capture_count"] == 0
        events.append("product")
        return inspect_product(payload)

    monkeypatch.setattr(native, "_inspect_listing", listing)
    monkeypatch.setattr(native, "_inspect_netcdf_gzip", product)
    native.capture_latest_hsaf_h40b(_job(), raw_store=store)
    assert events == ["listing", "product"]


def test_complete_retry_is_network_and_credential_free(monkeypatch, tmp_path):
    store, calls, _compressed, first = _capture(monkeypatch, tmp_path)

    def forbidden(*_args, **_kwargs):
        pytest.fail("complete H40B retry mag credential noch netwerk gebruiken")

    monkeypatch.setattr(native, "_credentials_from_environment", forbidden)
    monkeypatch.setattr(native, "_fetch_listing", forbidden)
    monkeypatch.setattr(native, "_fetch_product", forbidden)
    retry = native.capture_latest_hsaf_h40b(
        _job(), raw_store=RawArtifactStore(store.root)
    )
    assert retry == first
    assert calls == {"credential": 1, "listing": 1, "product": 1}


def test_process_lock_serializes_across_independent_store_instances(tmp_path):
    first_store = RawArtifactStore(tmp_path / "raw")
    second_store = RawArtifactStore(tmp_path / "raw")
    first_store.initialize()
    second_store.initialize()
    descriptor = native._acquire_process_lock(
        first_store,
        deadline=time.monotonic() + 1.0,
    )
    try:
        with pytest.raises(native.HsafH40bNativeDeadlineError):
            native._acquire_process_lock(
                second_store,
                deadline=time.monotonic() + 0.05,
            )
    finally:
        native._release_process_lock(descriptor)

    retry_descriptor = native._acquire_process_lock(
        second_store,
        deadline=time.monotonic() + 1.0,
    )
    native._release_process_lock(retry_descriptor)


class InjectedFailure(RuntimeError):
    pass


@pytest.mark.parametrize(
    ("checkpoint", "receipts"),
    [
        ("hsaf_h40b.after_listing_store", 1),
        ("hsaf_h40b.after_product_store", 2),
        ("hsaf_h40b.before_capture_commit", 2),
    ],
)
def test_partial_capture_is_never_complete_and_resumes(
    monkeypatch, tmp_path, checkpoint, receipts
):
    store = RawArtifactStore(tmp_path / "raw")
    calls, _compressed = _install_responses(monkeypatch)

    def fail(name):
        if name == checkpoint:
            raise InjectedFailure(name)

    with pytest.raises(InjectedFailure, match=checkpoint):
        native.capture_latest_hsaf_h40b(
            _job(), raw_store=store, _fault_injector=fail
        )
    assert store.stats()["receipt_count"] == receipts
    assert store.stats()["capture_count"] == 0
    result = native.capture_latest_hsaf_h40b(_job(), raw_store=store)
    assert result["capture_state"] == "captured_not_decoded"
    assert calls["listing"] == 1
    assert calls["product"] == 1


def test_after_commit_fault_leaves_complete_offline_retry(monkeypatch, tmp_path):
    store = RawArtifactStore(tmp_path / "raw")
    _install_responses(monkeypatch)

    def fail(name):
        if name == "hsaf_h40b.after_capture_commit":
            raise InjectedFailure(name)

    with pytest.raises(InjectedFailure):
        native.capture_latest_hsaf_h40b(
            _job(), raw_store=store, _fault_injector=fail
        )
    assert store.stats()["capture_count"] == 1
    monkeypatch.setattr(
        native,
        "_credentials_from_environment",
        lambda: pytest.fail("credential"),
    )
    assert native.capture_latest_hsaf_h40b(
        _job(), raw_store=store
    )["capture_state"] == "captured_not_decoded"


def _raw_inventory(store: RawArtifactStore) -> dict[str, object]:
    stats = store.stats()
    return {
        name: stats[name]
        for name in (
            "object_count",
            "receipt_count",
            "capture_count",
            "catalog_bytes",
            "filesystem_bytes",
            "filesystem_object_entries",
            "filesystem_staging_entries",
            "unmanaged_file_count",
            "store_profile_sha256",
        )
    }


def _receipt_hashes(receipt) -> tuple[str, str, str]:
    return (
        receipt.receipt_id,
        receipt.content_sha256,
        receipt.receipt_sha256,
    )


def _capture_hashes(capture) -> tuple[str, str, str]:
    return (
        capture.capture_id,
        capture.manifest_sha256,
        capture.capture_receipt_sha256,
    )


def _assert_hsaf_secrets_absent(root: Path, *diagnostics: object) -> None:
    secrets = (USERNAME.encode("utf-8"), PASSWORD.encode("utf-8"))
    serialized = json.dumps(
        diagnostics, sort_keys=True, default=str
    ).encode("utf-8")
    for secret in secrets:
        assert secret not in serialized
    for path in root.rglob("*"):
        if path.is_file():
            contents = path.read_bytes()
            for secret in secrets:
                assert secret not in contents, path


@pytest.mark.parametrize(
    ("checkpoint", "crashed_receipts", "crashed_captures", "recovery_calls"),
    [
        pytest.param(
            "hsaf_h40b.after_listing_store",
            1,
            0,
            {"credential": 1, "listing": 0, "product": 1},
            id="after_listing_store-R1-C0",
        ),
        pytest.param(
            "hsaf_h40b.after_product_store",
            2,
            0,
            {"credential": 0, "listing": 0, "product": 0},
            id="after_product_store-R2-C0",
        ),
        pytest.param(
            "hsaf_h40b.before_capture_commit",
            2,
            0,
            {"credential": 0, "listing": 0, "product": 0},
            id="before_capture_commit-R2-C0",
        ),
        pytest.param(
            "hsaf_h40b.after_capture_commit",
            2,
            1,
            {"credential": 0, "listing": 0, "product": 0},
            id="after_capture_commit-R2-C1",
        ),
    ],
)
def test_real_os_exit73_recovery_matrix(
    monkeypatch,
    tmp_path,
    checkpoint,
    crashed_receipts,
    crashed_captures,
    recovery_calls,
):
    raw_root = tmp_path / checkpoint.replace(".", "_")
    command = [
        sys.executable,
        str(CRASH_CHILD),
        "hsaf_h40b",
        str(raw_root),
        checkpoint,
    ]
    command_bytes = "\0".join(command).encode("utf-8")
    assert USERNAME.encode("utf-8") not in command_bytes
    assert PASSWORD.encode("utf-8") not in command_bytes
    environment = os.environ.copy()
    environment.pop("STV3_HSAF_USERNAME", None)
    environment.pop("STV3_HSAF_PASSWORD", None)
    environment["PYTHONPATH"] = os.pathsep.join(
        part
        for part in (str(ROOT), environment.get("PYTHONPATH", ""))
        if part
    )
    completed = subprocess.run(
        command,
        cwd=ROOT,
        env=environment,
        capture_output=True,
        check=False,
        timeout=CRASH_TIMEOUT_SECONDS,
    )
    _assert_hsaf_secrets_absent(
        raw_root, command, completed.stdout, completed.stderr
    )
    assert completed.returncode == 73, (completed.stdout, completed.stderr)

    preinspect_store = RawArtifactStore(raw_root)
    preinspect_store.initialize()
    preinspect_inventory = _raw_inventory(preinspect_store)
    assert (
        preinspect_inventory["object_count"],
        preinspect_inventory["receipt_count"],
        preinspect_inventory["capture_count"],
    ) == (crashed_receipts, crashed_receipts, crashed_captures)
    assert preinspect_inventory["filesystem_staging_entries"] == 0
    assert preinspect_inventory["unmanaged_file_count"] == 0
    preinspect_integrity = preinspect_store.integrity_check()
    assert preinspect_integrity["healthy"] is True, preinspect_integrity

    reopened_store = RawArtifactStore(raw_root)
    reopened_store.initialize()
    assert _raw_inventory(reopened_store) == preinspect_inventory
    job = _job()
    expected_receipt_ids = {
        role: native._receipt_id(reopened_store, job.job_id, role)
        for role in ("listing_nlst", "netcdf_gzip")
    }
    durable_roles = (
        ("listing_nlst",)
        if crashed_receipts == 1
        else ("listing_nlst", "netcdf_gzip")
    )
    fixture_product = _netcdf_gzip()
    expected_payloads = {
        "listing_nlst": LISTING,
        "netcdf_gzip": fixture_product,
    }
    crashed_receipt_hashes: dict[str, tuple[str, str, str]] = {}
    for role, receipt_id in expected_receipt_ids.items():
        receipt = reopened_store.load_receipt(receipt_id)
        if role not in durable_roles:
            assert receipt is None
            continue
        assert receipt is not None
        assert reopened_store.read_bytes(receipt_id) == expected_payloads[role]
        crashed_receipt_hashes[role] = _receipt_hashes(receipt)
    expected_capture_id = native._capture_id(reopened_store, job.job_id)
    crashed_capture = reopened_store.load_capture(expected_capture_id)
    assert (crashed_capture is not None) is bool(crashed_captures)
    crashed_capture_hashes = (
        None if crashed_capture is None else _capture_hashes(crashed_capture)
    )

    calls, _ = _install_responses(monkeypatch, product=fixture_product)
    recovered = native.capture_latest_hsaf_h40b(
        job, raw_store=RawArtifactStore(raw_root)
    )
    assert calls == recovery_calls
    assert recovered["capture_state"] == "captured_not_decoded"

    recovered_store = RawArtifactStore(raw_root)
    recovered_store.initialize()
    recovered_inventory = _raw_inventory(recovered_store)
    assert (
        recovered_inventory["object_count"],
        recovered_inventory["receipt_count"],
        recovered_inventory["capture_count"],
    ) == (2, 2, 1)
    assert recovered_inventory["filesystem_staging_entries"] == 0
    assert recovered_inventory["unmanaged_file_count"] == 0
    recovered_receipt_hashes: dict[str, tuple[str, str, str]] = {}
    for role, receipt_id in expected_receipt_ids.items():
        receipt = recovered_store.load_receipt(receipt_id)
        assert receipt is not None
        assert recovered_store.read_bytes(receipt_id) == expected_payloads[role]
        recovered_receipt_hashes[role] = _receipt_hashes(receipt)
    for role in durable_roles:
        assert recovered_receipt_hashes[role] == crashed_receipt_hashes[role]
    sources = recovered["raw_provenance"]["sources"]
    assert set(sources) == set(expected_receipt_ids)
    for role, hashes in recovered_receipt_hashes.items():
        assert (
            sources[role]["receipt_id"],
            sources[role]["content_sha256"],
            sources[role]["receipt_sha256"],
        ) == hashes
    recovered_capture = recovered_store.load_capture(expected_capture_id)
    assert recovered_capture is not None
    recovered_capture_hashes = _capture_hashes(recovered_capture)
    provenance = recovered["raw_provenance"]
    assert recovered_capture_hashes == (
        provenance["raw_capture_id"],
        provenance["raw_capture_manifest_sha256"],
        provenance["raw_capture_receipt_sha256"],
    )
    if crashed_capture_hashes is not None:
        assert recovered_capture_hashes == crashed_capture_hashes
    recovered_integrity = recovered_store.integrity_check()
    assert recovered_integrity["healthy"] is True, recovered_integrity

    replay = native.capture_latest_hsaf_h40b(
        job, raw_store=RawArtifactStore(raw_root)
    )
    assert calls == recovery_calls
    assert replay == recovered

    replay_store = RawArtifactStore(raw_root)
    replay_store.initialize()
    assert _raw_inventory(replay_store) == recovered_inventory
    for role, receipt_id in expected_receipt_ids.items():
        receipt = replay_store.load_receipt(receipt_id)
        assert receipt is not None
        assert _receipt_hashes(receipt) == recovered_receipt_hashes[role]
        assert replay_store.read_bytes(receipt_id) == expected_payloads[role]
    replay_capture = replay_store.load_capture(expected_capture_id)
    assert replay_capture is not None
    assert _capture_hashes(replay_capture) == recovered_capture_hashes
    replay_integrity = replay_store.integrity_check()
    assert replay_integrity["healthy"] is True, replay_integrity
    _assert_hsaf_secrets_absent(
        raw_root,
        command,
        completed.stdout,
        completed.stderr,
        recovered,
        replay,
        preinspect_integrity,
        recovered_integrity,
        replay_integrity,
    )


def test_listing_selects_latest_exact_ten_minute_slot():
    identity = native._inspect_listing(LISTING)
    assert identity.selected_basename == LATEST
    assert identity.selected_remote_path == LATEST_PATH
    assert identity.nominal_slot_time == LATEST_TIME
    assert identity.candidate_count == 3
    assert identity.first_slot_time < identity.last_slot_time


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"not-ascii-\xff\r\n",
        b"../h40_20260802_1210_fdk.nc.gz\r\n",
        b"h40_20260802_1211_fdk.nc.gz\r\n",
        b"h40_20260802_1210_fdk.nc.gz\r\nh40_20260802_1210_fdk.nc.gz\r\n",
        b"h40B_20260802_1210_fdk.nc.gz\r\n",
    ],
)
def test_listing_contract_fails_closed(payload):
    with pytest.raises(native.HsafH40bNativeContractError):
        native._inspect_listing(payload)


@pytest.mark.parametrize(
    "remote_path",
    [
        f"{native.FTP_DIRECTORY}/../{LATEST}",
        f"{native.FTP_DIRECTORY}/{LATEST}\r\nPASS secret",
        f"{native.FTP_DIRECTORY}/h40_20260802_1211_fdk.nc.gz",
        "/h40B/h40_cur_mon_data/h40_20260802_1210_fdk.nc.gz",
    ],
)
def test_product_fetch_rejects_every_noncontract_path(monkeypatch, remote_path):
    monkeypatch.setattr(
        native,
        "_ftp_retrieve",
        lambda *_args, **_kwargs: pytest.fail("ongeldig pad bereikte FTP"),
    )
    with pytest.raises(native.HsafH40bNativeContractError):
        native._fetch_product(
            native.HsafCredentials(USERNAME, PASSWORD),
            remote_path,
            deadline=10**9,
        )


def test_pum_netcdf_metadata_profile_reads_no_raster_or_axis_values():
    identity = native._inspect_netcdf_gzip(_netcdf_gzip())
    assert identity.rr_shape == (native.GRID_SIZE, native.GRID_SIZE)
    assert identity.rr_dtype == "int16"
    assert identity.qind_shape == (native.GRID_SIZE, native.GRID_SIZE)
    assert identity.qind_dtype == "int8"
    assert identity.nx_shape == (native.GRID_SIZE,)
    assert identity.ny_shape == (native.GRID_SIZE,)
    assert identity.conventions == "CF-1.7"
    assert identity.platform == "MTG1"
    assert identity.pixel_values_read is False
    assert identity.coordinate_values_read is False


@pytest.mark.parametrize(
    "payload",
    [
        b"not-gzip",
        _netcdf_gzip(include_qind=False),
        _netcdf_gzip(rr_units="dBZ"),
        _netcdf_gzip(external_link=True),
    ],
)
def test_container_profile_rejects_missing_qc_wrong_unit_or_unsafe_link(payload):
    with pytest.raises(native.HsafH40bNativeContractError):
        native._inspect_netcdf_gzip(payload)


def test_concatenated_gzip_members_are_rejected():
    payload = _netcdf_gzip() + gzip.compress(b"second", mtime=0)
    with pytest.raises(
        native.HsafH40bNativeContractError, match="aaneengeschakeld"
    ):
        native._inspect_netcdf_gzip(payload)


def test_hdf5_object_limit_fails_as_contract_error_without_callback_leak():
    payload = _netcdf_gzip(extra_objects=native.MAX_HDF5_OBJECTS)
    with pytest.raises(native.HsafH40bNativeContractError, match="te veel"):
        native._inspect_netcdf_gzip(payload)


def test_job_payload_cannot_carry_credentials_or_areas(monkeypatch, tmp_path):
    monkeypatch.setattr(
        native,
        "_credentials_from_environment",
        lambda: pytest.fail("credential resolution must not run"),
    )
    for payload in (
        {"username": USERNAME},
        {"password": PASSWORD},
        {"credential_ref": "worker_environment"},
        {"areas": []},
    ):
        with pytest.raises(
            native.HsafH40bNativeContractError, match="exact lege"
        ):
            native.capture_latest_hsaf_h40b(
                SimpleNamespace(job_id="hsaf-secret", payload=payload),
                raw_store=RawArtifactStore(tmp_path / "raw"),
            )


def test_environment_credentials_are_ephemeral(monkeypatch):
    monkeypatch.setenv("STV3_HSAF_USERNAME", USERNAME)
    monkeypatch.setenv("STV3_HSAF_PASSWORD", PASSWORD)
    assert native._credentials_from_environment() == native.HsafCredentials(
        USERNAME, PASSWORD
    )


def test_credential_bundle_repr_is_redacted_and_invalid_values_fail_closed(
    monkeypatch,
):
    credentials = native.HsafCredentials(USERNAME, PASSWORD)
    rendered = repr(credentials)
    assert USERNAME not in rendered
    assert PASSWORD not in rendered

    invalid = (
        ("ab", PASSWORD),
        (USERNAME, "1234567"),
        (f" {USERNAME}", PASSWORD),
        (USERNAME, f"{PASSWORD}\n"),
        ("gebruiker-é", PASSWORD),
    )
    for username, password in invalid:
        monkeypatch.setenv("STV3_HSAF_USERNAME", username)
        monkeypatch.setenv("STV3_HSAF_PASSWORD", password)
        with pytest.raises(
            native.HsafH40bNativeContractError, match="credential"
        ):
            native._credentials_from_environment()


@pytest.mark.parametrize("secret_field", ("listing", "product", "completion"))
def test_reflected_credentials_never_reach_durable_store(
    monkeypatch, tmp_path, secret_field
):
    store = RawArtifactStore(tmp_path / "raw")
    product = _netcdf_gzip()
    monkeypatch.setattr(
        native,
        "_credentials_from_environment",
        lambda: native.HsafCredentials(USERNAME, PASSWORD),
    )

    def listing(_credentials, *, deadline):
        del deadline
        payload = LISTING
        completion = "226 Transfer complete"
        if secret_field == "listing":
            payload += USERNAME.encode("ascii")
        if secret_field == "completion":
            completion += f" {PASSWORD}"
        return native.HsafFtpResponse(payload, LATEST_TIME + 60, completion)

    def fetch_product(_credentials, _remote_path, *, deadline):
        del deadline
        payload = product
        if secret_field == "product":
            payload += PASSWORD.encode("ascii")
        return native.HsafFtpResponse(
            payload,
            LATEST_TIME + 61,
            "226 Transfer complete",
            len(payload),
        )

    monkeypatch.setattr(native, "_fetch_listing", listing)
    monkeypatch.setattr(native, "_fetch_product", fetch_product)
    with pytest.raises(
        native.HsafH40bNativeContractError, match="credentialmateriaal"
    ):
        native.capture_latest_hsaf_h40b(_job(), raw_store=store)
    assert store.stats()["capture_count"] == 0
    assert store.stats()["receipt_count"] == (
        1 if secret_field == "product" else 0
    )
    for path in store.root.rglob("*"):
        if path.is_file():
            durable = path.read_bytes()
            assert USERNAME.encode("ascii") not in durable
            assert PASSWORD.encode("ascii") not in durable


def test_credential_shaped_job_identity_fails_before_network_or_store(
    monkeypatch, tmp_path
):
    store = RawArtifactStore(tmp_path / "raw")
    monkeypatch.setattr(
        native,
        "_credentials_from_environment",
        lambda: native.HsafCredentials(USERNAME, PASSWORD),
    )
    monkeypatch.setattr(
        native,
        "_fetch_listing",
        lambda *_args, **_kwargs: pytest.fail("credential-job_id reached network"),
    )
    with pytest.raises(
        native.HsafH40bNativeContractError, match="credentialmateriaal"
    ):
        native.capture_latest_hsaf_h40b(
            _job(f"hsaf-{PASSWORD}"), raw_store=store
        )
    assert store.stats()["receipt_count"] == 0


def test_raw_object_cap_leaves_only_complete_listing_and_no_capture(
    monkeypatch, tmp_path
):
    limits = RawArtifactLimits(
        max_object_bytes=len(LISTING),
        max_total_bytes=2 * 1024 * 1024,
        max_object_entries=8,
        max_staging_entries=4,
        max_receipt_entries=8,
        max_capture_entries=4,
        max_capture_members=2,
        max_catalog_bytes=2 * 1024 * 1024,
    )
    store = RawArtifactStore(tmp_path / "raw", limits=limits)
    _install_responses(monkeypatch)
    with pytest.raises(RawArtifactCapacityError):
        native.capture_latest_hsaf_h40b(_job(), raw_store=store)
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["capture_count"] == 0
    listing = store.load_receipt_bytes(
        native._receipt_id(store, _job().job_id, "listing_nlst")
    )
    assert listing is not None and listing[1] == LISTING


def test_exact_raw_tamper_fails_offline_before_any_retry_network(
    monkeypatch, tmp_path
):
    store, _calls, _compressed, result = _capture(monkeypatch, tmp_path)
    source = result["raw_provenance"]["sources"]["netcdf_gzip"]
    receipt = store.load_receipt(source["receipt_id"])
    assert receipt is not None
    artifact = Path(receipt.artifact_path)
    payload = artifact.read_bytes()
    artifact.write_bytes(bytes([payload[0] ^ 0x01]) + payload[1:])

    monkeypatch.setattr(
        native,
        "_credentials_from_environment",
        lambda: pytest.fail("tamper retry resolved credential"),
    )
    monkeypatch.setattr(
        native,
        "_fetch_listing",
        lambda *_args, **_kwargs: pytest.fail("tamper retry reached network"),
    )
    with pytest.raises(RawArtifactIntegrityError):
        native.capture_latest_hsaf_h40b(
            _job(), raw_store=RawArtifactStore(store.root)
        )


def test_expired_deadline_fails_before_credentials_network_or_evidence(
    monkeypatch, tmp_path
):
    store = RawArtifactStore(tmp_path / "raw")
    monkeypatch.setattr(
        native,
        "_credentials_from_environment",
        lambda: pytest.fail("expired capture resolved credential"),
    )
    with pytest.raises(native.HsafH40bNativeDeadlineError):
        native.capture_latest_hsaf_h40b(
            _job(), raw_store=store, deadline_at=time.monotonic() - 1
        )
    assert store.stats()["receipt_count"] == 0


def test_candidate_is_absent_from_all_live_wiring():
    paths = (
        ROOT / "storm_tracker_worker/service.py",
        ROOT / "storm_tracker_worker/autonomous_scheduler.py",
        ROOT / "storm_tracker_worker/config.yaml",
        ROOT / "storm_tracker_worker/run.sh",
    )
    module_name = "storm_tracker_worker.hsaf_h40b_native"
    handler_name = "capture_latest_hsaf_h40b"
    for path in paths:
        text = path.read_text(encoding="utf-8")
        assert module_name not in text
        assert handler_name not in text
    registry = (
        ROOT / "storm_tracker_worker/provider_capture_registry.py"
    ).read_text(encoding="utf-8")
    assert registry.count(module_name) == 1
    assert registry.count(handler_name) == 1
