"""Closed child contracts for the first credential-free multi-role batch."""
from __future__ import annotations

from dataclasses import replace
import re

import pytest

from storm_tracker_worker import acquisition_activation as activation
from storm_tracker_worker import acquisition_capture_recovery as recovery
from storm_tracker_worker import acquisition_handler_child as child
from storm_tracker_worker import desired_state
from storm_tracker_worker import worker_acquisition_plan as acquisition
from storm_tracker_worker.raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RawArtifactLimits,
    RawArtifactStore,
)


NOW = 1_800_000_000.0
JOB_ID = "acquisition:" + "a" * 64
SECRET_SENTINEL = "must-not-escape-child-output-77a4"
SHA256 = re.compile(r"^[0-9a-f]{64}$")
NEW_HANDLERS = (
    child.DPC_HANDLER_ID,
    child.NOAA_ERR_HANDLER_ID,
    child.GOES18_HANDLER_ID,
    child.GOES19_HANDLER_ID,
    child.METOFFICE_HANDLER_ID,
    child.HSAF_H40B_HANDLER_ID,
)
SINGLE_ROLE_HANDLERS = (
    child.METAR_HANDLER_ID,
    child.DWD_HANDLER_ID,
    child.KNMI_HANDLER_ID,
)


def _limits() -> RawArtifactLimits:
    return RawArtifactLimits(
        max_object_bytes=64 * 1024 * 1024,
        max_total_bytes=128 * 1024 * 1024,
        max_object_entries=64,
        max_staging_entries=8,
        max_receipt_entries=128,
        max_capture_entries=64,
        max_capture_members=8,
        max_catalog_bytes=8 * 1024 * 1024,
    )


def _limits_mapping() -> dict[str, int]:
    limits = _limits()
    return {
        name: getattr(limits, name)
        for name in (
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


def _request(tmp_path, handler_id: str) -> dict:
    spec = child._HANDLERS[handler_id]  # noqa: SLF001 - closed-contract test
    raw_root = (tmp_path / handler_id).resolve()
    if not raw_root.exists():
        RawArtifactStore(raw_root, limits=_limits()).initialize()
    return {
        "acquisition_job_id": JOB_ID,
        "contract": child.CONTRACT,
        "handler_id": handler_id,
        "handler_payload": {},
        "raw_store_limits": _limits_mapping(),
        "raw_store_root": str(raw_root),
        "workload_id": spec.workload_id,
    }


def _materializing_native(state: dict, *, mutation: str | None = None):
    def fake(
        spec,
        *,
        job_id,
        store,
        handler_payload=None,
        execution_deadline_at=None,
    ):
        del handler_payload, execution_deadline_at
        receipts = {}
        for index, role in enumerate(reversed(spec.roles), start=1):
            payload = f"{spec.handler_id}|{role.role}|{index}".encode("ascii")
            receipt = store.store(
                provider=spec.native_provider,
                product=(
                    "wrong-product"
                    if mutation == "receipt_product" and index == 1
                    else role.raw_product
                ),
                fetch_id=f"{job_id}{role.fetch_suffix}",
                content_type=sorted(role.allowed_content_types)[0],
                received_at=NOW + index,
                upstream_id=(
                    f"authority:{role.role}:token={SECRET_SENTINEL}"
                ),
                provenance={
                    "audit_marker": SECRET_SENTINEL,
                    "capture_stage": NATIVE_CAPTURE_STAGE,
                    "collector": "multi_role_child_test",
                    "collector_version": "multi-role-child-test-v1",
                    "representation": NATIVE_REPRESENTATION,
                    "role": role.role,
                },
                payload=payload,
            )
            receipts[role.role] = receipt
        capture = store.commit_capture(
            provider=spec.native_provider,
            product=(
                "wrong-capture-product"
                if mutation == "capture_product"
                else spec.capture_product
            ),
            fetch_id=f"{job_id}{spec.capture_fetch_suffix}",
            upstream_capture_id=(
                f"authority:capture:token={SECRET_SENTINEL}"
            ),
            product_timestamp=NOW,
            members={role: receipt.receipt_id for role, receipt in receipts.items()},
        )
        sources = {}
        for role in reversed(spec.roles):
            receipt = receipts[role.role]
            sources[role.role] = {
                "role": role.role,
                f"{spec.source_hash_prefix}receipt_id": receipt.receipt_id,
                f"{spec.source_hash_prefix}content_sha256": (
                    "f" * 64
                    if mutation == "source_content_hash" and role is spec.roles[0]
                    else receipt.content_sha256
                ),
                f"{spec.source_hash_prefix}receipt_sha256": receipt.receipt_sha256,
                "size_bytes": receipt.size_bytes,
            }
        if mutation == "missing_role":
            sources.pop(spec.roles[0].role)
        result = {
            "capture_state": "captured_not_decoded",
            "decode_status": spec.decode_status,
            "operational_admission_eligible": False,
            "raw_provenance": {
                "raw_capture_id": capture.capture_id,
                "raw_capture_manifest_sha256": capture.manifest_sha256,
                "raw_capture_receipt_sha256": capture.capture_receipt_sha256,
                "raw_member_count": len(spec.roles),
                spec.source_field: sources,
            },
            "weather_state": "unknown",
        }
        if spec.result_provider is not None:
            result["provider"] = spec.result_provider
        elif mutation == "unexpected_provider":
            result["provider"] = spec.native_provider
        state.update(
            capture=capture,
            receipts=receipts,
            spec=spec,
            store=store,
        )
        return result

    return fake


@pytest.mark.parametrize("handler_id", NEW_HANDLERS)
def test_multi_role_child_returns_exact_shared_aggregate_only(
    monkeypatch, tmp_path, handler_id
):
    state = {}
    monkeypatch.setattr(
        child,
        "_run_native",
        _materializing_native(state),
    )
    result = child.execute_shadow_capture(_request(tmp_path, handler_id))
    spec = state["spec"]
    binding = child._recovery_binding(spec)  # noqa: SLF001 - parity proof
    expected = recovery.recovery_evidence_digest(
        binding=binding,
        receipts_by_role=state["receipts"],
        capture=state["capture"],
    )

    assert result["contract"] == child.RESULT_CONTRACT
    assert result["handler_id"] == handler_id
    assert result["workload_id"] == spec.workload_id
    assert result["roles"] == [role.role for role in spec.roles]
    assert result["actual_entries"] == len(spec.roles) == expected.actual_entries
    assert result["actual_bytes"] == expected.actual_bytes
    assert result["content_sha256"] == expected.content_sha256
    assert result["receipt_sha256"] == expected.receipt_sha256
    assert result["capture_sha256"] == expected.capture_sha256
    assert result["operational_admission_eligible"] is False
    assert result["status"] == "captured_shadow_raw"
    assert all(
        SHA256.fullmatch(result[name])
        for name in (
            "capture_sha256",
            "content_sha256",
            "evidence_sha256",
            "receipt_sha256",
        )
    )
    assert SECRET_SENTINEL not in repr(result)
    assert "payload" not in repr(result).lower()
    assert "upstream" not in repr(result).lower()


@pytest.mark.parametrize(
    ("handler_id", "mutation"),
    (
        (child.DPC_HANDLER_ID, "capture_product"),
        (child.DPC_HANDLER_ID, "receipt_product"),
        (child.NOAA_ERR_HANDLER_ID, "missing_role"),
        (child.GOES18_HANDLER_ID, "source_content_hash"),
        (child.METOFFICE_HANDLER_ID, "unexpected_provider"),
        (child.HSAF_H40B_HANDLER_ID, "missing_role"),
    ),
)
def test_multi_role_child_fails_closed_on_durable_or_result_drift(
    monkeypatch, tmp_path, handler_id, mutation
):
    monkeypatch.setattr(
        child,
        "_run_native",
        _materializing_native({}, mutation=mutation),
    )
    with pytest.raises(ValueError):
        child.execute_shadow_capture(_request(tmp_path, handler_id))


@pytest.mark.parametrize("handler_id", SINGLE_ROLE_HANDLERS)
@pytest.mark.parametrize("mutation", ("capture_product", "receipt_product"))
def test_single_role_child_also_rejects_durable_product_drift(
    monkeypatch,
    tmp_path,
    handler_id,
    mutation,
):
    monkeypatch.setattr(
        child,
        "_run_native",
        _materializing_native({}, mutation=mutation),
    )
    with pytest.raises(ValueError):
        child.execute_shadow_capture(_request(tmp_path, handler_id))


def test_unregistered_child_spec_cannot_create_recovery_evidence():
    unregistered = replace(
        child._HANDLERS[child.DPC_HANDLER_ID],  # noqa: SLF001
        handler_id="unregistered_multi_role_handler",
    )
    with pytest.raises(ValueError, match="bindings differ"):
        child._recovery_binding(unregistered)  # noqa: SLF001


def _target(target_id, latitude, longitude, country):
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


def _jobs_by_workload() -> dict[str, acquisition.AcquisitionJob]:
    snapshot = desired_state.parse_desired_state({
        "authority_epoch": 1,
        "contract": desired_state.CONTRACT,
        "generation": 1,
        "issued_at": NOW - 60.0,
        "previous_state_sha256": None,
        "producer_id": "multi-role-activation-test",
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": True,
            "region_sharing_distance_km": 50.0,
        },
        "targets": [
            _target("zone.italy", 41.9, 12.5, "IT"),
            _target("zone.uk", 51.5, -0.1, "GB"),
            _target("zone.goes_west", 34.0, -118.0, "US"),
            _target("zone.goes_east", 40.7, -74.0, "US"),
        ],
    })
    plan = acquisition.build_worker_acquisition_plan(snapshot)
    return {
        job.workload_id: job
        for job in acquisition.build_worker_acquisition_slot_jobs(
            plan,
            as_of=NOW,
        )
    }


def test_active_handlers_bind_exact_scope_budget_roles_and_satellite():
    jobs = _jobs_by_workload()
    for handler_id in NEW_HANDLERS:
        spec = child._HANDLERS[handler_id]  # noqa: SLF001
        rule = activation.ACTIVATION_RULES[spec.workload_id]
        job = jobs[spec.workload_id]
        assert rule.status is activation.ActivationStatus.ACTIVE_SHADOW_RAW
        assert rule.reason_code == "exact_multi_role_shadow_capture_audited"
        assert rule.handler_id == handler_id
        assert rule.process_target == activation.SHADOW_CAPTURE_TARGET
        assert rule.required_roles == tuple(role.role for role in spec.roles)
        assert rule.maximum_transport_bytes == spec.maximum_transport_bytes
        assert rule.maximum_artifacts == len(spec.roles)
        assert activation._bind_handler_payload(job, rule) == {}  # noqa: SLF001

    goes18 = jobs[child.GOES18_WORKLOAD_ID]
    rule18 = activation.ACTIVATION_RULES[child.GOES18_WORKLOAD_ID]
    with pytest.raises(activation.ActivationError):
        activation._bind_handler_payload(  # noqa: SLF001
            replace(goes18, satellite_id="G19"),
            rule18,
        )


def test_goes_dispatch_is_closed_to_the_handler_fixed_satellite(monkeypatch, tmp_path):
    from storm_tracker_worker import goes_rrqpe_native

    calls = []

    def fake(_job, *, satellite, raw_store):
        calls.append((satellite, isinstance(raw_store, RawArtifactStore)))
        return {}

    monkeypatch.setattr(goes_rrqpe_native, "capture_latest_goes_rrqpe", fake)
    store = RawArtifactStore((tmp_path / "raw").resolve(), limits=_limits())
    store.initialize()
    child._run_native(  # noqa: SLF001
        child._HANDLERS[child.GOES18_HANDLER_ID],
        job_id=JOB_ID,
        store=store,
    )
    child._run_native(  # noqa: SLF001
        child._HANDLERS[child.GOES19_HANDLER_ID],
        job_id=JOB_ID,
        store=store,
    )
    assert calls == [(18, True), (19, True)]
