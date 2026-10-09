"""Adversarial proofs for generic multi-role capture recovery."""
from __future__ import annotations

from dataclasses import replace
import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest

from storm_tracker_worker import acquisition_activation as activation
from storm_tracker_worker import acquisition_capture_recovery as recovery
from storm_tracker_worker import desired_state
from storm_tracker_worker import worker_acquisition_plan as acquisition
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


def _job():
    state = desired_state.parse_desired_state({
        "authority_epoch": 1,
        "contract": desired_state.CONTRACT,
        "generation": 1,
        "issued_at": NOW,
        "previous_state_sha256": None,
        "producer_id": "multi-role-recovery-test",
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": False,
            "region_sharing_distance_km": 150.0,
        },
        "targets": [{
            "available": True,
            "country_code": "IT",
            "display_name": "Rome",
            "kind": "person",
            "latitude": 41.9028,
            "location_accuracy_m": None,
            "longitude": 12.4964,
            "observation_radius_km": 250.0,
            "position_observed_at": NOW,
            "target_id": "person.rome",
        }],
    })
    plan = acquisition.build_worker_acquisition_plan(state)
    jobs = acquisition.build_worker_acquisition_slot_jobs(plan, as_of=NOW)
    return next(item for item in jobs if item.workload_id == "radar.dpc.sri")


def _closed_contract():
    blocked = activation.ACTIVATION_RULES["radar.dpc.sri"]
    rule = replace(
        blocked,
        status=activation.ActivationStatus.ACTIVE_SHADOW_RAW,
        reason_code="multi_role_recovery_test_only",
    )
    binding = recovery.RawRecoveryBinding(
        handler_id=str(rule.handler_id),
        workload_id=rule.workload_id,
        ledger_provider=rule.provider_id,
        raw_provider="test_dpc_raw",
        capture_product="test_dpc_multi_capture",
        capture_suffix=":capture",
        members=(
            recovery.RawRecoveryMemberBinding(
                role="catalog_json",
                raw_product="test_dpc_catalog",
                receipt_suffix=":catalog_json",
                maximum_transport_bytes=16 * 1024 * 1024,
                allowed_content_types=frozenset({"application/json"}),
                provenance_kind="closed_role_set_v1",
            ),
            recovery.RawRecoveryMemberBinding(
                role="geotiff",
                raw_product="test_dpc_geotiff",
                receipt_suffix=":geotiff",
                maximum_transport_bytes=16 * 1024 * 1024,
                allowed_content_types=frozenset({"image/tiff"}),
                provenance_kind="closed_role_set_v1",
            ),
        ),
        maximum_transport_bytes=32 * 1024 * 1024,
        capture_provenance_kind="closed_role_set_v1",
    )
    bindings = {binding.handler_id: binding}
    rules = {rule.workload_id: rule}
    recovery.validate_closed_recovery_bindings(bindings, rules)
    return binding, rule, bindings, rules


def _stores(tmp_path: Path):
    limits = RawArtifactLimits(
        max_object_bytes=32 * 1024 * 1024,
        max_total_bytes=64 * 1024 * 1024,
        max_object_entries=16,
        max_staging_entries=4,
        max_receipt_entries=32,
        max_capture_entries=16,
        max_capture_members=4,
        max_catalog_bytes=8 * 1024 * 1024,
    )
    store = RawArtifactStore((tmp_path / "raw").resolve(), limits=limits)
    store.initialize()
    profile = RawCapacityProfile(
        global_max_bytes=64 * 1024 * 1024,
        global_max_entries=16,
        providers=(ProviderCapacityLimit("dpc", 64 * 1024 * 1024, 16),),
        max_reservations=16,
        max_catalog_bytes=8 * 1024 * 1024,
        sqlite_busy_timeout_seconds=2.0,
    )
    ledger = RawCapacityLedger(
        (tmp_path / "capacity.sqlite3").resolve(),
        profile=profile,
        clock=lambda: NOW,
    )
    return store, ledger


def _reserve(ledger, binding, job_id):
    reservation_id = "acqcap:" + hashlib.sha256(job_id.encode()).hexdigest()
    ledger.reserve(
        reservation_id=reservation_id,
        provider=binding.ledger_provider,
        job_identity=job_id,
        requested_bytes=binding.maximum_transport_bytes,
        requested_entries=binding.maximum_artifacts,
    )
    return reservation_id


def _store_member(store, binding, member, job_id, payload):
    content_sha = hashlib.sha256(payload).hexdigest()
    content_type = next(iter(member.allowed_content_types))
    return store.store(
        provider=binding.raw_provider,
        product=member.raw_product,
        fetch_id=f"{job_id}{member.receipt_suffix}",
        content_type=content_type,
        received_at=NOW,
        upstream_id=(
            f"{member.raw_product}|role={member.role}"
            f"|content_sha256={content_sha}"
        ),
        provenance={
            "representation": NATIVE_REPRESENTATION,
            "capture_stage": NATIVE_CAPTURE_STAGE,
            "collector": "closed_role_set_capture",
            "collector_version": recovery.ROLE_SET_DIGEST_CONTRACT,
            "meteorological_interpretation_verified_after_decode": False,
            "raw_product": member.raw_product,
            "role": member.role,
        },
        payload=payload,
    )


def _materialize(store, binding, job_id, *, swapped=False, omit_last=False, sizes=None):
    sizes = sizes or (17, 23)
    receipts = {
        member.role: _store_member(
            store,
            binding,
            member,
            job_id,
            bytes([65 + index]) * sizes[index],
        )
        for index, member in enumerate(binding.members)
    }
    members = {role: receipt.receipt_id for role, receipt in receipts.items()}
    if swapped:
        roles = tuple(members)
        members = {roles[0]: members[roles[1]], roles[1]: members[roles[0]]}
    if omit_last:
        members.pop(binding.required_roles[-1])
    digest = recovery.recovery_evidence_digest(
        binding=binding,
        receipts_by_role=receipts,
        capture=SimpleNamespace(capture_receipt_sha256="0" * 64),
    )
    capture = store.commit_capture(
        provider=binding.raw_provider,
        product=binding.capture_product,
        fetch_id=f"{job_id}{binding.capture_suffix}",
        upstream_capture_id=(
            f"{binding.capture_product}"
            f"|role_set_content_sha256={digest.content_sha256}"
        ),
        product_timestamp=None,
        members=members,
    )
    return receipts, capture, digest


def _reconciler(store, ledger, bindings, rules):
    return recovery.CaptureCapacityReconciler(
        raw_store=store,
        capacity_ledger=ledger,
        recovery_bindings=bindings,
        activation_rules=rules,
    )


def test_multi_role_digest_is_order_independent_and_restart_idempotent(tmp_path):
    binding, rule, bindings, rules = _closed_contract()
    job = _job()
    store, ledger = _stores(tmp_path)
    reservation_id = _reserve(ledger, binding, job.job_id)
    receipts, capture, expected = _materialize(store, binding, job.job_id)

    reversed_receipts = dict(reversed(tuple(receipts.items())))
    assert recovery.recovery_evidence_digest(
        binding=binding,
        receipts_by_role=reversed_receipts,
        capture=capture,
    ).content_sha256 == expected.content_sha256

    first = _reconciler(store, ledger, bindings, rules).reconcile_job(
        job=job,
        rule=rule,
        reservation_id=reservation_id,
    )
    assert first is not None and not first.already_committed
    assert first.actual_entries == 2
    assert first.actual_bytes == 40
    assert first.content_sha256 == expected.content_sha256
    assert first.content_sha256 not in {
        receipt.content_sha256 for receipt in receipts.values()
    }

    reopened = RawArtifactStore(store.root, limits=store.limits)
    reopened.initialize()
    replay = _reconciler(
        reopened,
        RawCapacityLedger(ledger.path, profile=ledger.profile, clock=lambda: NOW + 1),
        bindings,
        rules,
    ).audit_existing()
    assert replay.recovered_commit_count == 0
    assert replay.already_committed_count == 1
    assert ledger.stats()["committed_entries"] == 2


@pytest.mark.parametrize("tamper", ["swapped", "missing"])
def test_multi_role_member_tamper_fails_before_capacity_commit(tmp_path, tamper):
    binding, _rule, bindings, rules = _closed_contract()
    job = _job()
    store, ledger = _stores(tmp_path)
    reservation_id = _reserve(ledger, binding, job.job_id)
    _materialize(
        store,
        binding,
        job.job_id,
        swapped=tamper == "swapped",
        omit_last=tamper == "missing",
    )
    with pytest.raises(recovery.CaptureRecoveryIntegrityError):
        _reconciler(store, ledger, bindings, rules).audit_existing()
    assert ledger.load(reservation_id).status == "RESERVED"


def test_multi_role_aggregate_cap_must_cover_every_declared_member_cap():
    binding, _rule, _bindings, rules = _closed_contract()
    binding = replace(binding, maximum_transport_bytes=20)
    rule = replace(
        rules["radar.dpc.sri"],
        maximum_transport_bytes=20,
    )
    binding = replace(
        binding,
        members=tuple(
            replace(member, maximum_transport_bytes=16) for member in binding.members
        ),
    )
    bindings = {binding.handler_id: binding}
    rules = {rule.workload_id: rule}
    # The declared member envelopes sum to 32 bytes while the aggregate is 20;
    # this can never become an executable closed binding.
    with pytest.raises(RuntimeError, match="aggregate transport"):
        recovery.validate_closed_recovery_bindings(bindings, rules)


@pytest.mark.parametrize(
    "drift",
    ("provider", "product", "content_type", "role_cap", "job_identity"),
)
def test_digest_api_revalidates_each_closed_member(drift, tmp_path):
    binding, _rule, _bindings, _rules = _closed_contract()
    job = _job()
    store, _ledger = _stores(tmp_path)
    receipts, _capture, _digest = _materialize(store, binding, job.job_id)
    role = binding.required_roles[0]
    receipt = receipts[role]
    member = binding.member(role)
    replacements = {
        "provider": {"provider": "wrong-provider"},
        "product": {"product": "wrong-product"},
        "content_type": {"content_type": "application/octet-stream"},
        "role_cap": {"size_bytes": member.maximum_transport_bytes + 1},
        "job_identity": {
            "fetch_id": f"acquisition:{'f' * 64}{member.receipt_suffix}",
        },
    }
    tampered = dict(receipts)
    tampered[role] = replace(receipt, **replacements[drift])
    with pytest.raises(recovery.CaptureRecoveryIntegrityError):
        recovery.recovery_evidence_digest(
            binding=binding,
            receipts_by_role=tampered,
            capture=SimpleNamespace(capture_receipt_sha256="0" * 64),
        )


def test_committed_multi_role_digest_tamper_is_rejected_on_restart(tmp_path):
    binding, _rule, bindings, rules = _closed_contract()
    job = _job()
    store, ledger = _stores(tmp_path)
    reservation_id = _reserve(ledger, binding, job.job_id)
    _receipts, capture, digest = _materialize(store, binding, job.job_id)
    ledger.commit(
        reservation_id=reservation_id,
        actual_bytes=digest.actual_bytes,
        actual_entries=digest.actual_entries,
        content_sha256="f" * 64,
        receipt_sha256=digest.receipt_sha256,
        capture_sha256=capture.capture_receipt_sha256,
    )
    with pytest.raises(
        recovery.CaptureRecoveryIntegrityError,
        match="differs from durable raw evidence",
    ):
        _reconciler(store, ledger, bindings, rules).audit_existing()


def test_closed_binding_validator_rejects_role_and_suffix_drift():
    binding, _rule, _bindings, rules = _closed_contract()
    duplicate = replace(
        binding.members[1],
        role=binding.members[0].role,
        receipt_suffix=binding.members[0].receipt_suffix,
    )
    broken = replace(binding, members=(binding.members[0], duplicate))
    with pytest.raises(RuntimeError):
        recovery.validate_closed_recovery_bindings(
            {broken.handler_id: broken},
            rules,
        )
