from __future__ import annotations

from datetime import datetime, timezone
from io import BytesIO
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import h5py
import pytest

from storm_tracker_worker import meteofrance_native as native
from storm_tracker_worker.raw_artifacts import RawArtifactStore


ROOT = Path(__file__).resolve().parents[1]
CRASH_CHILD = (
    Path(__file__).parent / "helpers" / "hsaf_meteofrance_os_exit73_child.py"
)
CRASH_TIMEOUT_SECONDS = 20
OFFICIAL_EXAMPLE = (
    ROOT / "tests/fixtures/meteofrance_dpradar_official_catalog_example.json"
).read_bytes()
VALIDITY = datetime(2024, 6, 18, 19, 45, tzinfo=timezone.utc).timestamp()
FILENAME = "T_IPRN20_C_LFPW_20240618194500.h5"
TOKEN = "not-a-real-meteofrance-token-0123456789"


class _TransportResponse:
    def __init__(self, payload: bytes, *, headers: dict[str, str]) -> None:
        self._stream = BytesIO(payload)
        self.status = 200
        self.url = native.PRODUCT_URL
        self.headers = headers

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self, size: int = -1) -> bytes:
        return self._stream.read(size)


def _job(job_id: str = "meteofrance-capture-test"):
    return SimpleNamespace(job_id=job_id, payload={})


def test_product_transport_accepts_missing_content_length(monkeypatch):
    payload = b"\x89HDF\r\n\x1a\nchunked-product"
    response = _TransportResponse(
        payload,
        headers={
            "Content-Type": "application/x-hdf",
            "Content-Disposition": f'attachment; filename="{FILENAME}"',
        },
    )
    monkeypatch.setattr(
        native, "_open_without_redirects", lambda *_args, **_kwargs: response
    )

    fetched = native._fetch_product(
        TOKEN, deadline=native.time.monotonic() + 5.0
    )

    assert fetched.payload == payload
    assert fetched.content_length == len(payload)


def test_chunked_product_transport_remains_hard_bounded():
    with pytest.raises(native.MeteoFranceNativeContractError, match="bytegrens"):
        native._read_bounded(BytesIO(b"12345"), maximum=4, declared=None)


def test_declared_product_length_mismatch_remains_rejected():
    with pytest.raises(
        native.MeteoFranceNativeContractError, match="Content-Length"
    ):
        native._read_bounded(BytesIO(b"1234"), maximum=8, declared=5)


def _hdf5(*, expected_grid: bool = True) -> bytes:
    stream = BytesIO()
    with h5py.File(stream, "w") as handle:
        metadata = handle.create_group("metadata")
        metadata.attrs["fixture"] = "container-only-no-pixel-golden"
        shape = (
            (native.GRID_ROWS, native.GRID_COLUMNS)
            if expected_grid
            else (12, 13)
        )
        handle.create_dataset(
            "fields/accumulation",
            shape=shape,
            dtype="uint16",
            chunks=(64, 64) if expected_grid else None,
            fillvalue=native.DECLARED_MISSING_CODE,
        )
        handle.create_dataset(
            "fields/quality",
            shape=shape,
            dtype="uint8",
            chunks=(64, 64) if expected_grid else None,
        )
    return stream.getvalue()


def _response(
    payload: bytes,
    content_type: str,
    received_at: float,
    *,
    disposition: str | None = None,
) -> native.MeteoFranceHttpResponse:
    return native.MeteoFranceHttpResponse(
        payload=payload,
        content_type=content_type,
        received_at=received_at,
        content_length=len(payload),
        content_disposition=disposition,
        etag='"fixture-etag"',
        last_modified="Tue, 18 Jun 2024 19:46:00 GMT",
    )


def _install_responses(monkeypatch, *, hdf5: bytes | None = None):
    product = _hdf5() if hdf5 is None else hdf5
    calls = {"credential": 0, "catalog": 0, "product": 0}

    def credential(*, deadline):
        assert deadline > 0
        calls["credential"] += 1
        return TOKEN

    def catalog(token, *, deadline):
        assert token == TOKEN
        assert deadline > 0
        calls["catalog"] += 1
        return _response(
            OFFICIAL_EXAMPLE, "application/json", VALIDITY + 60
        )

    def fetch_product(token, *, deadline):
        assert token == TOKEN
        assert deadline > 0
        calls["product"] += 1
        return _response(
            product,
            "application/x-hdf",
            VALIDITY + 61,
            disposition=f'attachment; filename="{FILENAME}"',
        )

    monkeypatch.setattr(native, "_token_from_environment", credential)
    monkeypatch.setattr(native, "_fetch_catalog", catalog)
    monkeypatch.setattr(native, "_fetch_product", fetch_product)
    return calls, product


def _capture(monkeypatch, tmp_path, *, hdf5: bytes | None = None):
    store = RawArtifactStore(tmp_path / "raw")
    calls, product = _install_responses(monkeypatch, hdf5=hdf5)
    result = native.capture_latest_meteofrance_hdf5(
        _job(), raw_store=store
    )
    return store, calls, product, result


def test_official_catalog_example_selects_exact_hdf5_contract():
    identity = native._inspect_catalog(OFFICIAL_EXAMPLE)
    assert identity.validity_time == VALIDITY
    assert identity.media_type == "application/x-hdf"
    assert identity.attribution == "Source : Météo-France"
    assert identity.link_href.endswith("produit?maille=500")


def test_exact_catalog_and_hdf5_are_durable_and_never_admitted(
    monkeypatch, tmp_path
):
    store, calls, hdf5, result = _capture(monkeypatch, tmp_path)
    assert calls == {"credential": 1, "catalog": 1, "product": 1}
    assert store.stats()["receipt_count"] == 2
    assert store.stats()["capture_count"] == 1
    assert result["capture_state"] == "captured_not_decoded"
    assert result["weather_state"] == "unknown"
    assert result["decode_status"] == "blocked_incomplete_hdf5_semantics"
    assert result["pixel_values_read"] is False
    assert result["forecast_eligible"] is False
    assert result["operational_admission_eligible"] is False

    provenance = result["raw_provenance"]
    assert provenance["raw_store_schema_version"] == 3
    assert provenance["product_reference"] == native.PRODUCT_REFERENCE
    assert provenance["product_timestamp"] == VALIDITY
    assert provenance["filename"] == FILENAME
    assert provenance["authorization_persisted"] is False
    assert provenance["declared_raw_unit"] == "0.01_mm"
    assert provenance["declared_quality_range"] == [0, 100]
    assert provenance["container"]["raster_shaped_dataset_count"] == 2
    assert provenance["container"]["pixel_values_read"] is False
    assert len(provenance["unresolved_semantics"]) == 5

    sources = provenance["sources"]
    assert store.read_bytes(sources["catalog_json"]["receipt_id"]) == OFFICIAL_EXAMPLE
    assert store.read_bytes(sources["hdf5"]["receipt_id"]) == hdf5
    durable = (
        store.read_bytes(sources["catalog_json"]["receipt_id"])
        + store.read_bytes(sources["hdf5"]["receipt_id"])
        + json.dumps(result, sort_keys=True).encode()
    )
    assert TOKEN.encode() not in durable
    assert b"Authorization" not in durable
    assert b"Bearer " not in durable


def test_each_original_is_stored_before_its_inspection(monkeypatch, tmp_path):
    store = RawArtifactStore(tmp_path / "raw")
    _, hdf5 = _install_responses(monkeypatch)
    inspect_catalog = native._inspect_catalog
    inspect_hdf5 = native._inspect_hdf5
    events: list[str] = []

    def catalog(payload):
        assert payload == OFFICIAL_EXAMPLE
        assert store.stats()["receipt_count"] == 1
        assert store.stats()["capture_count"] == 0
        events.append("catalog")
        return inspect_catalog(payload)

    def product(payload):
        assert payload == hdf5
        assert store.stats()["receipt_count"] == 2
        assert store.stats()["capture_count"] == 0
        events.append("hdf5")
        return inspect_hdf5(payload)

    monkeypatch.setattr(native, "_inspect_catalog", catalog)
    monkeypatch.setattr(native, "_inspect_hdf5", product)
    native.capture_latest_meteofrance_hdf5(_job(), raw_store=store)
    assert events == ["catalog", "hdf5"]


def test_complete_retry_is_network_and_credential_free(monkeypatch, tmp_path):
    store, calls, _hdf5_bytes, first = _capture(monkeypatch, tmp_path)

    def forbidden(*_args, **_kwargs):
        pytest.fail("complete retry mag credential noch netwerk gebruiken")

    monkeypatch.setattr(native, "_token_from_environment", forbidden)
    monkeypatch.setattr(native, "_fetch_catalog", forbidden)
    monkeypatch.setattr(native, "_fetch_product", forbidden)
    retry = native.capture_latest_meteofrance_hdf5(
        _job(), raw_store=RawArtifactStore(store.root)
    )
    assert retry == first
    assert calls == {"credential": 1, "catalog": 1, "product": 1}


class InjectedFailure(RuntimeError):
    pass


@pytest.mark.parametrize(
    ("checkpoint", "receipt_count"),
    [
        ("meteofrance.after_catalog_store", 1),
        ("meteofrance.after_hdf5_store", 2),
        ("meteofrance.before_capture_commit", 2),
    ],
)
def test_partial_attempts_remain_uncommitted_and_resume(
    monkeypatch, tmp_path, checkpoint, receipt_count
):
    store = RawArtifactStore(tmp_path / "raw")
    calls, _ = _install_responses(monkeypatch)

    def fail(name):
        if name == checkpoint:
            raise InjectedFailure(name)

    with pytest.raises(InjectedFailure, match=checkpoint):
        native.capture_latest_meteofrance_hdf5(
            _job(), raw_store=store, _fault_injector=fail
        )
    assert store.stats()["receipt_count"] == receipt_count
    assert store.stats()["capture_count"] == 0
    result = native.capture_latest_meteofrance_hdf5(
        _job(), raw_store=store
    )
    assert result["capture_state"] == "captured_not_decoded"
    assert calls["catalog"] == 1
    assert calls["product"] == 1


def test_after_commit_fault_has_a_complete_offline_retry(monkeypatch, tmp_path):
    store = RawArtifactStore(tmp_path / "raw")
    _install_responses(monkeypatch)

    def fail(name):
        if name == "meteofrance.after_capture_commit":
            raise InjectedFailure(name)

    with pytest.raises(InjectedFailure):
        native.capture_latest_meteofrance_hdf5(
            _job(), raw_store=store, _fault_injector=fail
        )
    assert store.stats()["capture_count"] == 1
    monkeypatch.setattr(
        native,
        "_token_from_environment",
        lambda **_: pytest.fail("credential"),
    )
    result = native.capture_latest_meteofrance_hdf5(
        _job(), raw_store=store
    )
    assert result["capture_state"] == "captured_not_decoded"


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


def _assert_meteofrance_secrets_absent(
    root: Path, *diagnostics: object
) -> None:
    token = TOKEN.encode("utf-8")
    secrets = (token, b"Bearer " + token, b"Authorization: Bearer")
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
            "meteofrance.after_catalog_store",
            1,
            0,
            {"credential": 1, "catalog": 0, "product": 1},
            id="after_catalog_store-R1-C0",
        ),
        pytest.param(
            "meteofrance.after_hdf5_store",
            2,
            0,
            {"credential": 0, "catalog": 0, "product": 0},
            id="after_hdf5_store-R2-C0",
        ),
        pytest.param(
            "meteofrance.before_capture_commit",
            2,
            0,
            {"credential": 0, "catalog": 0, "product": 0},
            id="before_capture_commit-R2-C0",
        ),
        pytest.param(
            "meteofrance.after_capture_commit",
            2,
            1,
            {"credential": 0, "catalog": 0, "product": 0},
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
        "meteofrance",
        str(raw_root),
        checkpoint,
    ]
    command_bytes = "\0".join(command).encode("utf-8")
    assert TOKEN.encode("utf-8") not in command_bytes
    environment = os.environ.copy()
    environment.pop("STV3_METEOFRANCE_API_TOKEN", None)
    environment.pop("STV3_METEOFRANCE_APPLICATION_ID", None)
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
    _assert_meteofrance_secrets_absent(
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
        for role in ("catalog_json", "hdf5")
    }
    durable_roles = (
        ("catalog_json",)
        if crashed_receipts == 1
        else ("catalog_json", "hdf5")
    )
    fixture_hdf5 = _hdf5()
    expected_payloads = {
        "catalog_json": OFFICIAL_EXAMPLE,
        "hdf5": fixture_hdf5,
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

    calls, _ = _install_responses(monkeypatch, hdf5=fixture_hdf5)
    recovered = native.capture_latest_meteofrance_hdf5(
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

    replay = native.capture_latest_meteofrance_hdf5(
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
    _assert_meteofrance_secrets_absent(
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


@pytest.mark.parametrize(
    "payload",
    [
        br'{"attribution":"Source : M\u00e9t\u00e9o-France","links":[],"links":[]}',
        br'{"attribution":"Source : M\u00e9t\u00e9o-France","links":[]}',
        OFFICIAL_EXAMPLE.replace(b"maille=500", b"maille=250"),
        OFFICIAL_EXAMPLE.replace(b"application/x-hdf", b"image/png"),
        OFFICIAL_EXAMPLE.replace(b"19:45:00Z", b"19:46:00Z"),
        OFFICIAL_EXAMPLE.replace(
            b"public-api.meteofrance.fr", b"127.0.0.1:8123"
        ),
    ],
)
def test_catalog_contract_fails_closed(payload):
    with pytest.raises(native.MeteoFranceNativeContractError):
        native._inspect_catalog(payload)


def test_catalog_rejects_two_500m_authorities():
    root = json.loads(OFFICIAL_EXAMPLE)
    root["links"].append(dict(root["links"][1]))
    with pytest.raises(
        native.MeteoFranceNativeContractError, match="exact één"
    ):
        native._inspect_catalog(json.dumps(root).encode())


@pytest.mark.parametrize(
    "disposition",
    [
        None,
        'attachment; filename="T_IPRN21_C_LFPW_20240618194500.h5"',
        'attachment; filename="T_IPRN20_C_LFPW_20240618195000.h5"',
        'attachment; filename="../../T_IPRN20_C_LFPW_20240618194500.h5"',
        'attachment; filename="T_IPRN20_C_LFPW_20240618194500.h5.exe"',
    ],
)
def test_product_identity_requires_exact_reference_and_catalog_time(disposition):
    catalog = native._inspect_catalog(OFFICIAL_EXAMPLE)
    response = _response(
        _hdf5(),
        "application/x-hdf",
        VALIDITY + 1,
        disposition=disposition,
    )
    with pytest.raises(native.MeteoFranceNativeContractError):
        native._product_identity(response, catalog=catalog)


def test_container_inspection_is_metadata_only_and_grid_bounded():
    identity = native._inspect_hdf5(_hdf5())
    assert identity.dataset_count == 2
    assert identity.raster_shaped_dataset_count == 2
    assert identity.pixel_values_read is False
    assert (native.GRID_ROWS, native.GRID_COLUMNS) in identity.dataset_shapes

    with pytest.raises(native.MeteoFranceNativeContractError):
        native._inspect_hdf5(b"not-hdf5")
    with pytest.raises(
        native.MeteoFranceNativeContractError, match="3472"
    ):
        native._inspect_hdf5(_hdf5(expected_grid=False))


def test_container_rejects_external_or_soft_links_without_resolving_them():
    stream = BytesIO()
    with h5py.File(stream, "w") as handle:
        handle.create_dataset(
            "rain",
            shape=(native.GRID_ROWS, native.GRID_COLUMNS),
            dtype="uint16",
            chunks=(64, 64),
        )
        handle["unsafe"] = h5py.ExternalLink("missing-local-file.h5", "/data")
    with pytest.raises(
        native.MeteoFranceNativeContractError, match="externe"
    ):
        native._inspect_hdf5(stream.getvalue())


def test_job_payload_cannot_carry_a_secret(monkeypatch, tmp_path):
    monkeypatch.setattr(
        native,
        "_token_from_environment",
        lambda **_: pytest.fail("credential resolution must not run"),
    )
    for payload in (
        {"token": TOKEN},
        {"application_id": "Basic secret"},
        {"areas": []},
    ):
        job = SimpleNamespace(job_id="meteofrance-secret", payload=payload)
        with pytest.raises(
            native.MeteoFranceNativeContractError, match="exact lege"
        ):
            native.capture_latest_meteofrance_hdf5(
                job, raw_store=RawArtifactStore(tmp_path / "raw")
            )


def test_static_environment_token_is_returned_but_never_logged(monkeypatch):
    monkeypatch.setenv("STV3_METEOFRANCE_API_TOKEN", TOKEN)
    monkeypatch.delenv("STV3_METEOFRANCE_APPLICATION_ID", raising=False)
    assert native._token_from_environment(deadline=10**12) == TOKEN


def test_candidate_is_absent_from_live_worker_wiring():
    service = (ROOT / "storm_tracker_worker/service.py").read_text(
        encoding="utf-8"
    )
    registry = (
        ROOT / "storm_tracker_worker/provider_capture_registry.py"
    ).read_text(encoding="utf-8")
    scheduler = (
        ROOT / "storm_tracker_worker/autonomous_scheduler.py"
    ).read_text(encoding="utf-8")
    module_name = "storm_tracker_worker.meteofrance_native"
    handler_name = "capture_latest_meteofrance_hdf5"
    assert module_name not in service
    assert module_name not in scheduler
    assert handler_name not in service
    assert handler_name not in scheduler
    # The closed migration registry retains one string-only audit reference;
    # it neither imports nor dispatches the candidate handler.
    assert registry.count(module_name) == 1
    assert registry.count(handler_name) == 1
