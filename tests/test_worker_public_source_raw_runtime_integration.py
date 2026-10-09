"""Closed runtime proofs for the five anonymous public raw sources.

No test in this module performs live network I/O.  The transport double emits
successful response bytes, including an empty body and malformed JSON, so the
shared child/recovery path proves transport evidence without decoding it.
"""
from __future__ import annotations

import ast
from dataclasses import replace
import hashlib
import io
import json
from pathlib import Path
import time

import pytest

from storm_tracker_worker import acquisition_activation as activation
from storm_tracker_worker import acquisition_capture_recovery as recovery
from storm_tracker_worker import acquisition_handler_child as child
from storm_tracker_worker import acquisition_location_shadow as shadow
from storm_tracker_worker import acquisition_query_authority as authority
from storm_tracker_worker import desired_state as desired
from storm_tracker_worker import public_source_raw_capture as public_raw
from storm_tracker_worker import worker_acquisition_plan as acquisition
from storm_tracker_worker.raw_artifacts import RawArtifactLimits, RawArtifactStore


SLOT = 1_800_000_000.0
NOW = SLOT + 10.0
PUBLIC_WORKLOADS = (
    public_raw.OPENMETEO_WORKLOAD_ID,
    public_raw.OPENMETEO_ICON2I_WORKLOAD_ID,
    public_raw.GEOSPHERE_WORKLOAD_ID,
    public_raw.ITALIAMETEO_CATALOG_WORKLOAD_ID,
    public_raw.METEOLUX_WORKLOAD_ID,
)
HANDLERS = {
    workload_id: public_raw.HANDLER_SPECS_BY_WORKLOAD[workload_id].handler_id
    for workload_id in PUBLIC_WORKLOADS
}


def _canonical(value: dict) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _state() -> desired.DesiredState:
    return desired.parse_desired_state({
        "authority_epoch": 31,
        "contract": desired.CONTRACT,
        "generation": 4,
        "issued_at": NOW,
        "previous_state_sha256": "5" * 64,
        "producer_id": "public-source-raw-runtime-test",
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": False,
            "region_sharing_distance_km": 150.0,
        },
        "targets": [{
            "available": True,
            "country_code": "BE",
            "display_name": "must-never-leave-parent",
            "kind": "home",
            "latitude": 50.85,
            "location_accuracy_m": None,
            "longitude": 4.60,
            "observation_radius_km": 10.0,
            "position_observed_at": SLOT,
            "target_id": "zone.private_home",
        }],
    })


def _limits() -> RawArtifactLimits:
    return RawArtifactLimits(
        max_object_bytes=8 * 1024 * 1024,
        max_total_bytes=64 * 1024 * 1024,
        max_object_entries=64,
        max_staging_entries=8,
        max_receipt_entries=128,
        max_capture_entries=64,
        max_capture_members=32,
        max_catalog_bytes=16 * 1024 * 1024,
    )


def _all_source_jobs() -> tuple[
    desired.DesiredState,
    acquisition.AcquisitionPlan,
    dict[str, acquisition.AcquisitionJob],
]:
    state = _state()
    default = acquisition.build_worker_acquisition_plan(state)
    plan = acquisition.build_worker_acquisition_plan(
        state,
        collector_scope=acquisition.COLLECTOR_SCOPE_ALL_SOURCES,
    )
    jobs = {
        item.workload_id: item
        for item in acquisition.build_worker_acquisition_slot_jobs(
            plan, as_of=SLOT
        )
        if item.workload_id in PUBLIC_WORKLOADS
    }
    assert not set(PUBLIC_WORKLOADS).intersection(
        item.workload_id for item in default.intents
    )
    assert set(jobs) == set(PUBLIC_WORKLOADS)
    return state, plan, jobs


def _request(
    tmp_path: Path,
    workload_id: str,
) -> tuple[dict, acquisition.AcquisitionJob]:
    state, plan, jobs = _all_source_jobs()
    job = jobs[workload_id]
    raw_root = (tmp_path / workload_id).resolve()
    if not raw_root.exists():
        RawArtifactStore(raw_root, limits=_limits()).initialize()
    request = shadow.build_runtime_guarded_shadow_child_request(
        state,
        plan,
        job,
        evaluated_at=NOW,
        resolver=activation.MappingSecretResolver({}),
        raw_store_root=raw_root,
        raw_store_limits=_limits(),
    )
    return request, job


class _Response:
    def __init__(self, url: str, payload: bytes) -> None:
        self.status = 200
        self.headers = {
            "Content-Length": str(len(payload)),
            "Content-Type": "application/json; charset=utf-8",
        }
        self._url = url
        self._stream = io.BytesIO(payload)

    def geturl(self) -> str:
        return self._url

    def read(self, size: int) -> bytes:
        return self._stream.read(size)

    def close(self) -> None:
        self._stream.close()


def _install_transport(
    monkeypatch: pytest.MonkeyPatch,
    *,
    payload: bytes,
) -> list[str]:
    original = public_raw.capture_public_source_handler
    urls: list[str] = []

    def opener(request, *, timeout):
        assert 0.0 < timeout <= public_raw.HTTP_IO_TIMEOUT_SECONDS
        urls.append(request.full_url)
        return _Response(request.full_url, payload)

    def capture(handler_id, job, *, raw_store, **kwargs):
        assert not kwargs
        return original(
            handler_id,
            job,
            raw_store=raw_store,
            deadline_at=time.monotonic() + 5.0,
            opener=opener,
        )

    monkeypatch.setattr(
        public_raw,
        "capture_public_source_handler",
        capture,
    )
    return urls


def test_public_sources_are_raw_service_only_and_recovery_closed() -> None:
    recovery.validate_closed_recovery_bindings(
        recovery.RECOVERY_BINDINGS,
        activation.ACTIVATION_RULES,
    )
    rows = {
        item["workload_id"]: item
        for item in acquisition.acquisition_catalog_snapshot()["workloads"]
    }
    for workload_id in PUBLIC_WORKLOADS:
        native = public_raw.HANDLER_SPECS_BY_WORKLOAD[workload_id]
        rule = activation.ACTIVATION_RULES[workload_id]
        spec = child._HANDLERS[native.handler_id]  # noqa: SLF001
        binding = recovery.RECOVERY_BINDINGS[native.handler_id]

        assert rows[workload_id]["status"] == (
            acquisition.CapabilityStatus.CONTRACT_READY_UNWIRED.value
        )
        assert rows[workload_id]["durable_production_retention_eligible"] is False
        assert rule.executable and rule.raw_capture_service_only
        assert workload_id in activation.RAW_CAPTURE_WORKLOAD_IDS
        assert workload_id not in activation.DEFAULT_SHADOW_WORKLOAD_IDS
        assert rule.process_target == activation.SHADOW_CAPTURE_TARGET
        assert rule.required_roles == binding.required_roles == ("response_bytes",)
        assert rule.maximum_transport_bytes == native.maximum_transport_bytes
        assert rule.maximum_transport_bytes == binding.maximum_transport_bytes
        assert spec.workload_id == binding.workload_id == workload_id
        assert spec.native_provider == binding.raw_provider == native.provider
        assert spec.ledger_provider == binding.ledger_provider == native.provider
        assert spec.capture_product == binding.capture_product == native.capture_product
        assert spec.safe_native_identity
        assert binding.job_identity_kind == "safe_native_alias_v1"
        assert child._recovery_binding(spec) == binding  # noqa: SLF001


def test_live_request_projection_is_bounded_and_contains_no_target_identity(
    tmp_path: Path,
) -> None:
    state, plan, jobs = _all_source_jobs()
    expected_region_points = {
        public_raw.OPENMETEO_ICON2I_WORKLOAD_ID: (41.9028, 12.4964),
        public_raw.GEOSPHERE_WORKLOAD_ID: (48.2082, 16.3738),
        public_raw.METEOLUX_WORKLOAD_ID: (50.85, 4.60),
    }
    for workload_id, job in jobs.items():
        request = shadow.build_runtime_guarded_shadow_child_request(
            state,
            plan,
            job,
            evaluated_at=NOW,
            resolver=activation.MappingSecretResolver({}),
            raw_store_root=(tmp_path / workload_id).resolve(),
            raw_store_limits=_limits(),
        )
        payload = request["handler_payload"]
        guard = request["execution_guard"]
        assert request["handler_id"] == HANDLERS[workload_id]
        assert guard["payload_sha256"] == hashlib.sha256(
            _canonical(payload)
        ).hexdigest()
        assert guard["authority_sha256"] is None if workload_id == (
            public_raw.ITALIAMETEO_CATALOG_WORKLOAD_ID
        ) else len(guard["authority_sha256"]) == 64
        if workload_id == public_raw.OPENMETEO_WORKLOAD_ID:
            assert payload["contract"] == authority.OPENMETEO_BATCH_CONTRACT
            assert len(payload["queries"]) == 1
            assert payload["queries"][0]["latitude"] == 50.85
            assert payload["queries"][0]["longitude"] == 4.60
        elif workload_id == public_raw.ITALIAMETEO_CATALOG_WORKLOAD_ID:
            assert payload == {}
        else:
            assert payload["contract"] == (
                authority.PUBLIC_SOURCE_REGION_QUERY_CONTRACT
            )
            assert (payload["latitude"], payload["longitude"]) == (
                expected_region_points[workload_id]
            )
        encoded = json.dumps(request, sort_keys=True)
        assert "zone.private_home" not in encoded
        assert "must-never-leave-parent" not in encoded


@pytest.mark.parametrize("workload_id", PUBLIC_WORKLOADS)
def test_child_stores_success_bytes_before_parse_and_recovery_revalidates(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    workload_id: str,
) -> None:
    payload = (
        b""
        if workload_id == public_raw.ITALIAMETEO_CATALOG_WORKLOAD_ID
        else b"{malformed-success"
    )
    urls = _install_transport(monkeypatch, payload=payload)
    request, _job = _request(tmp_path, workload_id)
    evidence = child.execute_shadow_capture(
        request,
        _clock=lambda: NOW,
    )

    assert len(urls) == 1 and urls[0].startswith("https://")
    assert evidence["status"] == "captured_shadow_raw"
    assert evidence["actual_entries"] == 1
    assert evidence["actual_bytes"] == len(payload)
    assert evidence["operational_admission_eligible"] is False

    store = RawArtifactStore(Path(request["raw_store_root"]), limits=_limits())
    store.initialize()
    captures = store.accounting_captures()
    assert len(captures) == 1
    capture = captures[0]
    bundle = store.load_capture_bundle_bytes(capture.capture_id)
    assert bundle is not None
    durable_capture, receipts, payloads = bundle
    assert payloads == {"response_bytes": payload}
    receipt = receipts["response_bytes"]
    assert receipt.content_type == "application/octet-stream"
    assert receipt.provenance["parse_attempted"] is False
    assert receipt.provenance["semantic_status"] == "unverified"
    assert receipt.provenance["weather_state"] == "unknown"
    assert receipt.provenance["provider_rights_claim"] is False
    assert receipt.provenance["retention_right_claim"] is False

    binding = recovery.RECOVERY_BINDINGS[HANDLERS[workload_id]]
    native_job_id, transport_sha256 = recovery._validate_receipt(  # noqa: SLF001
        receipt,
        binding,
        binding.member("response_bytes"),
    )
    assert native_job_id == authority.safe_native_job_id(
        request["acquisition_job_id"]
    )
    assert transport_sha256 == receipt.provenance["transport_sha256"]
    assert recovery._validate_capture(  # noqa: SLF001
        durable_capture,
        binding=binding,
        receipts_by_role=receipts,
        metadata_by_role={"response_bytes": transport_sha256},
    ) == native_job_id

    tampered = dict(receipt.provenance)
    tampered["semantic_status"] = "verified"
    with pytest.raises(recovery.CaptureRecoveryIntegrityError):
        recovery._validate_receipt(  # noqa: SLF001
            replace(
                receipt,
                _provenance_json=json.dumps(
                    tampered,
                    separators=(",", ":"),
                    sort_keys=True,
                ),
            ),
            binding,
            binding.member("response_bytes"),
        )


def test_public_raw_modules_never_enter_ha_mqtt_or_decoder_imports() -> None:
    root = Path(__file__).resolve().parents[1]
    public_text = (root / "storm_tracker_worker" / (
        "public_source_raw_capture.py"
    )).read_text(encoding="utf-8")
    child_text = (root / "storm_tracker_worker" / (
        "acquisition_handler_child.py"
    )).read_text(encoding="utf-8")
    imported_modules: set[str] = set()
    for source in (public_text, child_text):
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported_modules.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module is not None:
                imported_modules.add(node.module)
    assert not any(
        name == "homeassistant" or name.startswith("homeassistant.")
        for name in imported_modules
    )
    assert not any("mqtt" in name.lower() for name in imported_modules)
    assert "decode_public_source" not in public_text + child_text
