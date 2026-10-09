"""Closed exact-raw shadow contracts for AEMET and Meteo-France."""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
import hashlib
from io import BytesIO
import json
from pathlib import Path
import time

import h5py
import pytest

from storm_tracker_worker import acquisition_activation as activation
from storm_tracker_worker import acquisition_capture_recovery as recovery
from storm_tracker_worker import acquisition_handler_child as child
from storm_tracker_worker import aemet_native
from storm_tracker_worker import desired_state
from storm_tracker_worker import meteofrance_native
from storm_tracker_worker import worker_acquisition_plan as acquisition
from storm_tracker_worker.raw_artifacts import RawArtifactLimits, RawArtifactStore
from storm_tracker_worker.raw_capacity_ledger import (
    ProviderCapacityLimit,
    RawCapacityLedger,
    RawCapacityProfile,
)


ROOT = Path(__file__).resolve().parents[1]
NOW = 1_800_000_000.0
AEMET_KEY = "aemet-worker-secret-0123456789"
AEMET_DATA_URL = (
    "https://opendata.aemet.es/opendata/sh/dataSelectionToken001"
)
AEMET_METADATA_URL = (
    "https://opendata.aemet.es/opendata/sh/metaSelectionToken001"
)
AEMET_ENVELOPE = json.dumps({
    "descripcion": "exito",
    "estado": 200,
    "datos": AEMET_DATA_URL,
    "metadatos": AEMET_METADATA_URL,
}, separators=(",", ":")).encode("utf-8")
AEMET_DATA = b"II*\x00AEMET-SHADOW-GEOTIFF"
AEMET_METADATA = b'{"format":"GeoTIFF","scale":"ESCALA"}'
METEOFRANCE_TOKEN = "meteofrance-worker-token-0123456789"
METEOFRANCE_CATALOG = (
    ROOT / "tests/fixtures/meteofrance_dpradar_official_catalog_example.json"
).read_bytes()
METEOFRANCE_VALIDITY = datetime(
    2024, 6, 18, 19, 45, tzinfo=timezone.utc
).timestamp()
METEOFRANCE_FILENAME = "T_IPRN20_C_LFPW_20240618194500.h5"


def _target(target_id: str, latitude: float, longitude: float, country: str):
    return {
        "available": True,
        "country_code": country,
        "display_name": target_id,
        "kind": "home",
        "latitude": latitude,
        "location_accuracy_m": None,
        "longitude": longitude,
        "observation_radius_km": 250.0,
        "position_observed_at": NOW - 30.0,
        "target_id": target_id,
    }


def _jobs() -> dict[str, acquisition.AcquisitionJob]:
    state = desired_state.parse_desired_state({
        "authority_epoch": 1,
        "contract": desired_state.CONTRACT,
        "generation": 1,
        "issued_at": NOW - 60.0,
        "previous_state_sha256": None,
        "producer_id": "aemet-meteofrance-shadow-test",
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": False,
            "region_sharing_distance_km": 50.0,
        },
        "targets": [
            _target("zone.madrid", 40.4168, -3.7038, "ES"),
            _target("zone.paris", 48.8566, 2.3522, "FR"),
        ],
    })
    plan = acquisition.build_worker_acquisition_plan(state)
    return {
        job.workload_id: job
        for job in acquisition.build_worker_acquisition_slot_jobs(
            plan, as_of=NOW
        )
    }


def _limits() -> RawArtifactLimits:
    return RawArtifactLimits(
        max_object_bytes=101 * 1024 * 1024,
        max_total_bytes=192 * 1024 * 1024,
        max_object_entries=16,
        max_staging_entries=4,
        max_receipt_entries=32,
        max_capture_entries=16,
        max_capture_members=4,
        max_catalog_bytes=8 * 1024 * 1024,
    )


def _limits_mapping() -> dict[str, int]:
    limits = _limits()
    return {
        field: getattr(limits, field)
        for field in (
            "max_capture_entries",
            "max_capture_members",
            "max_catalog_bytes",
            "max_object_bytes",
            "max_object_entries",
            "max_receipt_entries",
            "max_staging_entries",
            "max_total_bytes",
        )
    }


def _request(root: Path, handler_id: str, job_id: str) -> dict[str, object]:
    spec = child._HANDLERS[handler_id]  # noqa: SLF001 - closed table proof
    # Production creates and fully audits the raw store in the parent before
    # any fresh capture child is admitted.
    if not root.exists():
        RawArtifactStore(root.resolve(), limits=_limits()).initialize()
    return {
        "acquisition_job_id": job_id,
        "contract": child.CONTRACT,
        "handler_id": handler_id,
        "handler_payload": {},
        "raw_store_limits": _limits_mapping(),
        "raw_store_root": str(root.resolve()),
        "workload_id": spec.workload_id,
    }


def _aemet_response(payload: bytes, content_type: str, received_at: float):
    return aemet_native.AemetHttpResponse(
        payload=payload,
        content_type=content_type,
        received_at=received_at,
        content_length=len(payload),
        etag='"aemet-test-etag"',
        last_modified="Sun, 02 Aug 2026 12:00:00 GMT",
    )


def _install_aemet(monkeypatch) -> None:
    monkeypatch.setenv("STV3_AEMET_API_KEY", AEMET_KEY)

    def envelope(key: str, *, deadline: float):
        assert key == AEMET_KEY and deadline > time.monotonic()
        return _aemet_response(AEMET_ENVELOPE, "application/json", NOW)

    def download(url: str, role: str, *, deadline: float):
        assert deadline > time.monotonic()
        if role == "opendata_data_response":
            assert url == AEMET_DATA_URL
            return _aemet_response(AEMET_DATA, "image/tiff", NOW + 1)
        assert role == "opendata_metadata_response"
        assert url == AEMET_METADATA_URL
        return _aemet_response(AEMET_METADATA, "application/json", NOW + 2)

    monkeypatch.setattr(aemet_native, "_fetch_envelope", envelope)
    monkeypatch.setattr(aemet_native, "_fetch_download", download)


def _meteofrance_hdf5() -> bytes:
    stream = BytesIO()
    with h5py.File(stream, "w") as handle:
        handle.create_dataset(
            "fields/accumulation",
            shape=(meteofrance_native.GRID_ROWS, meteofrance_native.GRID_COLUMNS),
            dtype="uint16",
            chunks=(64, 64),
            fillvalue=meteofrance_native.DECLARED_MISSING_CODE,
        )
    return stream.getvalue()


def _mf_response(
    payload: bytes,
    content_type: str,
    received_at: float,
    *,
    disposition: str | None = None,
):
    return meteofrance_native.MeteoFranceHttpResponse(
        payload=payload,
        content_type=content_type,
        received_at=received_at,
        content_length=len(payload),
        content_disposition=disposition,
        etag='"meteofrance-test-etag"',
        last_modified="Tue, 18 Jun 2024 19:46:00 GMT",
    )


def _install_meteofrance(monkeypatch) -> None:
    hdf5 = _meteofrance_hdf5()
    monkeypatch.setenv("STV3_METEOFRANCE_API_TOKEN", METEOFRANCE_TOKEN)
    monkeypatch.delenv("STV3_METEOFRANCE_APPLICATION_ID", raising=False)

    def catalog(token: str, *, deadline: float):
        assert token == METEOFRANCE_TOKEN and deadline > time.monotonic()
        return _mf_response(
            METEOFRANCE_CATALOG, "application/json", METEOFRANCE_VALIDITY + 1
        )

    def product(token: str, *, deadline: float):
        assert token == METEOFRANCE_TOKEN and deadline > time.monotonic()
        return _mf_response(
            hdf5,
            "application/x-hdf",
            METEOFRANCE_VALIDITY + 2,
            disposition=f'attachment; filename="{METEOFRANCE_FILENAME}"',
        )

    monkeypatch.setattr(meteofrance_native, "_fetch_catalog", catalog)
    monkeypatch.setattr(meteofrance_native, "_fetch_product", product)


def _install_provider(monkeypatch, workload_id: str) -> tuple[str, str]:
    if workload_id == child.AEMET_WORKLOAD_ID:
        _install_aemet(monkeypatch)
        return child.AEMET_HANDLER_ID, AEMET_KEY
    _install_meteofrance(monkeypatch)
    return child.METEOFRANCE_HANDLER_ID, METEOFRANCE_TOKEN


def test_activation_binds_exact_catalog_budget_roles_scope_and_secrets():
    jobs = _jobs()
    expected = {
        child.AEMET_WORKLOAD_ID: (
            child.AEMET_HANDLER_ID,
            101 * 1024 * 1024,
            3,
            75,
            "coverage:aemet_es",
            ("worker_secret.aemet_api_key",),
        ),
        child.METEOFRANCE_WORKLOAD_ID: (
            child.METEOFRANCE_HANDLER_ID,
            48 * 1024 * 1024,
            2,
            90,
            "coverage:meteofrance_fr",
            ("worker_secret.meteofrance_oauth",),
        ),
    }
    resolver = activation.MappingSecretResolver({
        "worker_secret.aemet_api_key": "validated-worker-local-presence",
        "worker_secret.meteofrance_oauth": "validated-worker-local-presence",
    })
    for workload_id, contract in expected.items():
        handler_id, maximum, artifacts, wall, scope, secrets = contract
        rule = activation.ACTIVATION_RULES[workload_id]
        spec = child._HANDLERS[handler_id]  # noqa: SLF001
        binding = recovery.RECOVERY_BINDINGS[handler_id]
        job = jobs[workload_id]
        assert rule.status is activation.ActivationStatus.ACTIVE_SHADOW_RAW
        assert rule.reason_code == "exact_multi_role_shadow_capture_audited"
        assert (
            rule.maximum_transport_bytes,
            rule.maximum_artifacts,
            rule.maximum_wall_seconds,
            rule.expected_scope_id,
            rule.secret_requirement_ids,
        ) == (maximum, artifacts, wall, scope, secrets)
        assert rule.required_roles == tuple(role.role for role in spec.roles)
        assert binding.required_roles == rule.required_roles
        assert binding.maximum_transport_bytes == maximum
        assert activation.assess_acquisition_job(
            job, resolver=resolver
        ).executable
        missing = activation.assess_acquisition_job(
            job, resolver=activation.NullSecretResolver()
        )
        assert not missing.executable
        assert missing.reason_code == "secret_requirement_unavailable"


def test_environment_resolver_requires_valid_aemet_or_either_meteofrance_path():
    marker = "worker_environment_secret_present"
    assert activation.EnvironmentSecretResolver({
        "STV3_AEMET_API_KEY": "a" * 16,
    }).resolve("worker_secret.aemet_api_key") == marker
    for invalid in ("a" * 15, " a" * 8, "a" * 15 + "\n", "é" * 16):
        assert activation.EnvironmentSecretResolver({
            "STV3_AEMET_API_KEY": invalid,
        }).resolve("worker_secret.aemet_api_key") is None

    for environment in (
        {"STV3_METEOFRANCE_API_TOKEN": "t" * 16},
        {"STV3_METEOFRANCE_APPLICATION_ID": "c" * 16},
        {"STV3_METEOFRANCE_APPLICATION_ID": "Basic " + "c" * 16},
        {
            "STV3_METEOFRANCE_API_TOKEN": "short",
            "STV3_METEOFRANCE_APPLICATION_ID": "c" * 16,
        },
    ):
        assert activation.EnvironmentSecretResolver(environment).resolve(
            "worker_secret.meteofrance_oauth"
        ) == marker
    for environment in (
        {},
        {"STV3_METEOFRANCE_API_TOKEN": "short"},
        {"STV3_METEOFRANCE_APPLICATION_ID": "Basic short"},
        {"STV3_METEOFRANCE_API_TOKEN": "t" * 15 + "\n"},
    ):
        assert activation.EnvironmentSecretResolver(environment).resolve(
            "worker_secret.meteofrance_oauth"
        ) is None


@pytest.mark.parametrize(
    "workload_id",
    (child.AEMET_WORKLOAD_ID, child.METEOFRANCE_WORKLOAD_ID),
)
def test_native_child_is_exact_raw_secret_free_and_crash_recoverable(
    monkeypatch, tmp_path: Path, workload_id: str
):
    handler_id, secret = _install_provider(monkeypatch, workload_id)
    job = _jobs()[workload_id]
    raw_root = (tmp_path / workload_id).resolve()
    request = _request(raw_root, handler_id, job.job_id)
    assert secret not in repr(request)

    profile = RawCapacityProfile(
        global_max_bytes=192 * 1024 * 1024,
        global_max_entries=16,
        providers=(ProviderCapacityLimit(
            activation.ACTIVATION_RULES[workload_id].provider_id,
            192 * 1024 * 1024,
            16,
        ),),
        max_reservations=16,
        max_catalog_bytes=8 * 1024 * 1024,
        sqlite_busy_timeout_seconds=2.0,
    )
    ledger = RawCapacityLedger(
        (tmp_path / f"{handler_id}.sqlite3").resolve(),
        profile=profile,
        clock=lambda: NOW,
    )
    rule = activation.ACTIVATION_RULES[workload_id]
    reservation_id = "acqcap:" + hashlib.sha256(
        job.job_id.encode("ascii")
    ).hexdigest()
    ledger.reserve(
        reservation_id=reservation_id,
        provider=rule.provider_id,
        job_identity=job.job_id,
        requested_bytes=int(rule.maximum_transport_bytes),
        requested_entries=int(rule.maximum_artifacts),
    )

    result = child.execute_shadow_capture(request)
    assert result["status"] == "captured_shadow_raw"
    assert result["operational_admission_eligible"] is False
    assert result["actual_entries"] == rule.maximum_artifacts
    assert result["roles"] == list(rule.required_roles)
    assert secret not in repr(result)
    assert "payload" not in repr(result).lower()
    assert "upstream" not in repr(result).lower()
    for path in raw_root.rglob("*"):
        if path.is_file():
            assert secret.encode("ascii") not in path.read_bytes()

    # This is the parent-crash window: raw capture is complete while the
    # capacity reservation remains RESERVED. Startup recovery owns the CAS.
    store = RawArtifactStore(raw_root, limits=_limits())
    store.initialize()
    recovered = recovery.CaptureCapacityReconciler(
        raw_store=store,
        capacity_ledger=ledger,
    ).audit_existing()
    assert recovered.recovered_commit_count == 1
    assert recovered.already_committed_count == 0
    committed = ledger.load(reservation_id)
    assert committed is not None and committed.status == "COMMITTED"
    assert committed.actual_entries == rule.maximum_artifacts

    replay_store = RawArtifactStore(raw_root, limits=_limits())
    replay_store.initialize()
    replay = recovery.CaptureCapacityReconciler(
        raw_store=replay_store,
        capacity_ledger=RawCapacityLedger(
            ledger.path, profile=profile, clock=lambda: NOW + 1
        ),
    ).audit_existing()
    assert replay.recovered_commit_count == 0
    assert replay.already_committed_count == 1


@pytest.mark.parametrize(
    "workload_id",
    (child.AEMET_WORKLOAD_ID, child.METEOFRANCE_WORKLOAD_ID),
)
def test_native_recovery_rejects_role_provenance_and_capture_identity_tamper(
    monkeypatch, tmp_path: Path, workload_id: str
):
    handler_id, _secret = _install_provider(monkeypatch, workload_id)
    job = _jobs()[workload_id]
    raw_root = (tmp_path / workload_id).resolve()
    child.execute_shadow_capture(_request(raw_root, handler_id, job.job_id))
    binding = recovery.RECOVERY_BINDINGS[handler_id]
    store = RawArtifactStore(raw_root, limits=_limits())
    store.initialize()
    captures = store.accounting_captures()
    assert len(captures) == 1
    capture = captures[0]
    receipts = {
        member.role: store.load_receipt(member.receipt_id)
        for member in capture.members
    }
    assert all(receipt is not None for receipt in receipts.values())
    receipts = {role: receipt for role, receipt in receipts.items() if receipt}

    first_role = binding.required_roles[0]
    first = receipts[first_role]
    tampered_provenance = dict(first.provenance)
    tampered_provenance["collector_version"] = "forged-version"
    with pytest.raises(recovery.CaptureRecoveryIntegrityError):
        recovery._validate_receipt(  # noqa: SLF001 - adversarial boundary proof
            replace(
                first,
                _provenance_json=json.dumps(
                    tampered_provenance, separators=(",", ":"), sort_keys=True
                ),
            ),
            binding,
            binding.member(first_role),
        )

    metadata = {
        role: recovery._validate_receipt(  # noqa: SLF001
            receipt, binding, binding.member(role)
        )[1]
        for role, receipt in receipts.items()
    }
    with pytest.raises(recovery.CaptureRecoveryIntegrityError):
        recovery._validate_capture(  # noqa: SLF001
            replace(capture, upstream_capture_id="forged-upstream"),
            binding=binding,
            receipts_by_role=receipts,
            metadata_by_role=metadata,
        )


def test_batch_never_enters_service_main_or_meteorological_admission():
    service = (ROOT / "storm_tracker_worker/service.py").read_text(
        encoding="utf-8"
    )
    scheduler = (ROOT / "storm_tracker_worker/autonomous_scheduler.py").read_text(
        encoding="utf-8"
    )
    for module_name, handler_name in (
        (
            "storm_tracker_worker.aemet_native",
            "capture_latest_aemet_national_radar",
        ),
        (
            "storm_tracker_worker.meteofrance_native",
            "capture_latest_meteofrance_hdf5",
        ),
    ):
        assert module_name not in service
        assert module_name not in scheduler
        assert handler_name not in service
        assert handler_name not in scheduler
    matrix = activation.activation_matrix_snapshot()
    encoded = json.dumps(matrix, sort_keys=True)
    assert AEMET_KEY not in encoded
    assert METEOFRANCE_TOKEN not in encoded
    assert matrix["active_shadow_raw"] == 11
