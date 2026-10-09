"""Fail-closed proofs for the never-live science-to-fact bridge."""
from __future__ import annotations

import ast
from contextlib import closing
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import sqlite3
import time

import pytest

from storm_tracker_worker.desired_state import DesiredStateStore
from storm_tracker_worker.operational_fact_store import OperationalFactRevisionStore
from storm_tracker_worker.science_operational_fact_bridge import (
    MAX_AUTHORITY_BYTES,
    MAX_REFERENCE_HASHES,
    DurableScienceShadowFactAuthority,
    ScienceFactProvenanceBinding,
    ScienceOperationalFactBridge,
    ScienceOperationalFactBridgeError,
    ScienceOperationalFactRequest,
    ScienceTargetFootprint,
    SyntheticScienceTestFileAuthority,
    SyntheticScienceTestReceipt,
    encode_synthetic_science_test_receipt,
    science_receipt_provenance_sha256,
)
from storm_tracker_worker.science_shadow_orchestrator import ScienceShadowLimits
from tests.helpers.science_shadow_process_factory import build_test_orchestrator


NOW = 1_800_900_000.0
MODULE = Path(__file__).parents[1] / "storm_tracker_worker/science_operational_fact_bridge.py"


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _run(number: int) -> str:
    return "run-" + format(number, "064x")


def _activate_desired(
    tmp_path: Path,
    *,
    synthetic: bool,
    name: str = "desired.sqlite3",
):
    producer = "synthetic-test:ha" if synthetic else "ha:science-bridge-test"
    target_id = "synthetic-test:target" if synthetic else "zone.home"
    store = DesiredStateStore((tmp_path / name).resolve())
    claim = store.claim_producer(producer, "claim:science-bridge", claimed_at=NOW - 120.0)
    value = {
        "authority_epoch": claim["authority_epoch"],
        "contract": "stv3-worker-desired-state-v1",
        "generation": 1,
        "issued_at": NOW - 90.0,
        "previous_state_sha256": None,
        "producer_id": producer,
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": False,
            "region_sharing_distance_km": 100.0,
        },
        "targets": [{
            "available": True,
            "country_code": "BE",
            "display_name": "Synthetic target" if synthetic else "Thuis",
            "kind": "test" if synthetic else "home",
            "latitude": 51.04,
            "location_accuracy_m": 10.0,
            "longitude": 4.48,
            "observation_radius_km": 250.0,
            "position_observed_at": NOW - 60.0,
            "target_id": target_id,
        }],
    }
    store.apply(value, received_at=NOW - 80.0)
    desired = store.load_active()
    assert desired is not None
    return store, desired


def _request(
    desired,
    *,
    receipt_sha256: str = "0" * 64,
    provider: str = "synthetic-test:radar",
    provenance_sha256: str | None = None,
    source_receipts: tuple[str, ...] | None = None,
    certificates: tuple[str, ...] = (_sha("synthetic-certificate"),),
    attempt_id: str = "attempt-science-fact",
    science_request_sha256: str | None = None,
    runtime_profile_sha256: str | None = None,
    canonical_run_id: str | None = None,
):
    target = desired.targets[0]
    footprint = ScienceTargetFootprint.from_target(target)
    return ScienceOperationalFactRequest(
        science_receipt_sha256=receipt_sha256,
        science_attempt_id=attempt_id,
        science_request_sha256=science_request_sha256 or _sha("science-request"),
        canonical_run_id=canonical_run_id or _run(1),
        engine_id="synthetic-test:science-engine",
        runtime_profile_sha256=runtime_profile_sha256 or _sha("runtime-profile"),
        process_authority_sha256=_sha("process-authority"),
        process_receipt_sha256=_sha("process-receipt"),
        desired_state_sha256=desired.state_sha256,
        authority_epoch=desired.authority_epoch,
        snapshot_revision=1,
        footprint=footprint,
        provenance=ScienceFactProvenanceBinding(
            science_provenance_sha256=provenance_sha256 or _sha("provenance"),
            profile_sha256=runtime_profile_sha256 or _sha("runtime-profile"),
            source_receipt_sha256s=source_receipts or (_sha("upstream-source"),),
            certificate_sha256s=certificates,
        ),
        product_valid_at=NOW - 60.0,
        received_at=NOW - 30.0,
        evaluated_at=NOW,
        valid_until=NOW + 120.0,
        rain_rate_mm_h=1.25,
        provider_id=provider,
        provider_display_name="Synthetic radar" if provider.startswith("synthetic") else provider.upper(),
        region_id=(
            "synthetic-test:region" if provider.startswith("synthetic") else "region:home"
        ),
        region_display_name="Synthetic region" if provider.startswith("synthetic") else "Regio thuis",
    )


def _synthetic_bundle(tmp_path: Path, *, fact_fault=None, name: str = "authority.json"):
    desired_store, desired = _activate_desired(tmp_path, synthetic=True)
    preliminary = _request(desired)
    receipt = SyntheticScienceTestReceipt.create(
        authority_id="synthetic-test:authority",
        request=preliminary,
        certificate_sha256s=preliminary.provenance.certificate_sha256s,
        decided_at=NOW - 1.0,
    )
    request = replace(preliminary, science_receipt_sha256=receipt.receipt_sha256)
    body = encode_synthetic_science_test_receipt(receipt)
    authority_path = (tmp_path / name).resolve()
    authority_path.write_bytes(body)
    authority = SyntheticScienceTestFileAuthority(
        authority_path,
        expected_file_sha256=hashlib.sha256(body).hexdigest(),
    )
    fact_store = OperationalFactRevisionStore(
        (tmp_path / f"facts-{name}.sqlite3").resolve(),
        clock=lambda: NOW + 1.0,
        fault_injector=fact_fault,
    )
    bridge = ScienceOperationalFactBridge(
        desired_state_store=desired_store,
        fact_revision_store=fact_store,
        clock=lambda: NOW + 1.0,
    )
    return desired_store, desired, fact_store, bridge, request, authority, authority_path


def _real_shadow_receipt(tmp_path: Path):
    limits = ScienceShadowLimits(max_runtime_seconds=30.0, max_receipts=32)
    receipt_path = (tmp_path / "real-science.sqlite3").resolve()
    document = {
        "authorized_paths": [{
            "access": "transactional_file",
            "content_sha256": None,
            "label": "science_receipts",
            "path": str(receipt_path),
        }],
        "engine_id": "science-process-test-engine",
        "factory_config": {"mode": "normal"},
        "receipt_store_label": "science_receipts",
        "science_limits": limits.to_mapping(),
    }
    orchestrator = build_test_orchestrator(document)
    run_id = _run(9)
    result = orchestrator.process(
        attempt_id="attempt-real-shadow",
        run_id=run_id,
        deadline_at=time.monotonic() + 10.0,
    )
    assert result.receipt.outcome.value == "advanced"
    return orchestrator.receipt_store, result.receipt


def test_synthetic_test_authority_stages_exactly_one_revision_and_binding(tmp_path):
    desired_store, desired, facts, bridge, request, authority, _path = _synthetic_bundle(
        tmp_path
    )

    first = bridge.stage(request, authority=authority)
    second = bridge.stage(request, authority=authority)

    assert first.status == "staged"
    assert first.created_ready_fact is True
    assert first.binding is not None
    assert first.binding.pipeline_item_id == "science-fact:" + request.bridge_request_sha256
    assert first.binding.process_receipt_sha256 == request.process_receipt_sha256
    assert first.binding.desired_state_sha256 == desired.state_sha256
    assert first.binding.authority_epoch == desired.authority_epoch
    assert first.binding.snapshot_revision == 1
    assert second.status == "duplicate"
    assert second.created_ready_fact is False
    assert second.binding == first.binding
    assert facts.audit()["ready"] == 1
    assert facts.audit()["retained_rows"] == 1
    record = facts.next_ready(desired)
    assert record is not None
    assert record.revision.snapshot["targets"]["items"][0][
        "current_precipitation"
    ]["state"] == "raining"
    assert record.revision.snapshot["targets"]["items"][0]["next_threat"][
        "state"
    ] == "unknown"
    assert len(record.revision.admission_receipts) == 3
    assert all(
        item.source_receipt_sha256s == (request.science_receipt_sha256,)
        and item.process_receipt_sha256 == request.process_receipt_sha256
        for item in record.revision.admission_receipts
    )
    assert desired_store.load_active() == desired


@pytest.mark.parametrize("provider", ("dwd", "kmi"))
def test_current_real_dwd_and_kmi_shadow_contracts_create_zero_ready_facts(
    tmp_path,
    provider,
):
    desired_store, desired = _activate_desired(
        tmp_path,
        synthetic=False,
        name=f"desired-{provider}.sqlite3",
    )
    science_store, receipt = _real_shadow_receipt(tmp_path / provider)
    provenance_sha = science_receipt_provenance_sha256(receipt)
    request = _request(
        desired,
        receipt_sha256=receipt.receipt_sha256,
        provider=provider,
        provenance_sha256=provenance_sha,
        source_receipts=(receipt.receipt_sha256,),
        certificates=(),
        attempt_id=receipt.attempt_id,
        science_request_sha256=receipt.request_sha256,
        runtime_profile_sha256=receipt.runtime_profile_sha256,
        canonical_run_id=receipt.canonical_run_id,
    )
    request = replace(request, engine_id=receipt.engine_id)
    facts = OperationalFactRevisionStore(
        (tmp_path / f"facts-{provider}.sqlite3").resolve(),
        clock=lambda: NOW + 1.0,
    )
    bridge = ScienceOperationalFactBridge(
        desired_state_store=desired_store,
        fact_revision_store=facts,
        clock=lambda: NOW + 1.0,
    )

    result = bridge.stage(
        request,
        authority=DurableScienceShadowFactAuthority(science_store),
    )

    assert result.status == "withheld"
    assert result.binding is None
    assert result.created_ready_fact is False
    assert "science_receipt_shadow_only" in result.reason_codes
    assert "meteorological_admission_not_granted" in result.reason_codes
    assert "durable_process_admission_certificate_missing" in result.reason_codes
    assert facts.audit()["ready"] == 0
    assert facts.audit()["retained_rows"] == 0


def test_random_receipt_sha_missing_row_and_tampered_file_fail_closed(tmp_path):
    _desired_store, _desired, facts, bridge, request, authority, path = _synthetic_bundle(
        tmp_path
    )
    random_request = replace(request, science_receipt_sha256=_sha("random-receipt"))
    random_result = bridge.stage(random_request, authority=authority)
    assert random_result.status == "rejected"
    assert random_result.reason_codes == ("receipt_authority_rejected",)

    original = path.read_bytes()
    value = json.loads(original)
    value["process_receipt_sha256"] = _sha("tampered-process")
    path.write_bytes(json.dumps(value, sort_keys=True, separators=(",", ":")).encode())
    tampered = bridge.stage(request, authority=authority)
    assert tampered.status == "rejected"
    assert facts.audit()["retained_rows"] == 0

    missing = SyntheticScienceTestFileAuthority(
        (tmp_path / "missing-authority.json").resolve(),
        expected_file_sha256=_sha("missing-file"),
    )
    missing_result = bridge.stage(request, authority=missing)
    assert missing_result.status == "rejected"
    assert facts.audit()["retained_rows"] == 0


def test_real_durable_science_random_missing_and_tampered_rows_fail_closed(tmp_path):
    desired_store, desired = _activate_desired(tmp_path, synthetic=False)
    science_store, receipt = _real_shadow_receipt(tmp_path / "real-row")
    request = _request(
        desired,
        receipt_sha256=receipt.receipt_sha256,
        provider="dwd",
        provenance_sha256=science_receipt_provenance_sha256(receipt),
        source_receipts=(receipt.receipt_sha256,),
        certificates=(),
        attempt_id=receipt.attempt_id,
        science_request_sha256=receipt.request_sha256,
        runtime_profile_sha256=receipt.runtime_profile_sha256,
        canonical_run_id=receipt.canonical_run_id,
    )
    request = replace(request, engine_id=receipt.engine_id)
    facts = OperationalFactRevisionStore(
        (tmp_path / "real-row-facts.sqlite3").resolve(),
        clock=lambda: NOW + 1.0,
    )
    bridge = ScienceOperationalFactBridge(
        desired_state_store=desired_store,
        fact_revision_store=facts,
        clock=lambda: NOW + 1.0,
    )
    authority = DurableScienceShadowFactAuthority(science_store)

    random_sha = bridge.stage(
        replace(request, science_receipt_sha256=_sha("random-real-receipt")),
        authority=authority,
    )
    missing_row = bridge.stage(
        replace(
            request,
            science_attempt_id="attempt-missing",
            science_request_sha256=_sha("missing-request"),
        ),
        authority=authority,
    )
    assert random_sha.status == missing_row.status == "rejected"
    assert facts.audit()["retained_rows"] == 0

    with closing(sqlite3.connect(science_store.path)) as database, database:
        database.execute(
            "UPDATE science_shadow_receipts SET receipt_json=? WHERE attempt_id=?",
            (b"{}", receipt.attempt_id),
        )
        database.commit()
    tampered = bridge.stage(request, authority=authority)
    assert tampered.status == "rejected"
    assert tampered.reason_codes == ("receipt_authority_rejected",)
    assert facts.audit()["retained_rows"] == 0


def test_stale_desired_target_process_profile_provenance_and_expiry_are_rejected(tmp_path):
    desired_store, desired, facts, bridge, request, original_authority, _path = _synthetic_bundle(
        tmp_path
    )

    # Process/profile/provenance substitutions do not match the already sealed
    # authority and are rejected before the fact store is touched.
    for changed in (
        replace(request, process_receipt_sha256=_sha("other-process")),
        replace(request, runtime_profile_sha256=_sha("other-profile")),
        replace(
            request,
            provenance=replace(
                request.provenance,
                science_provenance_sha256=_sha("other-provenance"),
            ),
        ),
    ):
        result = bridge.stage(changed, authority=original_authority)
        assert result.status == "rejected"
        assert result.reason_codes == ("receipt_authority_rejected",)

    # For stale desired state and changed target geometry the synthetic receipt
    # itself is exact, so the subsequent desired/footprint gate is exercised.
    cases = (
        replace(request, desired_state_sha256=_sha("stale-desired")),
        replace(
            request,
            footprint=replace(request.footprint, latitude=request.footprint.latitude + 0.1),
        ),
    )
    for index, changed in enumerate(cases):
        receipt = SyntheticScienceTestReceipt.create(
            authority_id="synthetic-test:authority",
            request=changed,
            certificate_sha256s=changed.provenance.certificate_sha256s,
            decided_at=NOW - 1.0,
        )
        changed = replace(changed, science_receipt_sha256=receipt.receipt_sha256)
        body = encode_synthetic_science_test_receipt(receipt)
        path = (tmp_path / f"case-{index}.json").resolve()
        path.write_bytes(body)
        authority = SyntheticScienceTestFileAuthority(
            path,
            expected_file_sha256=hashlib.sha256(body).hexdigest(),
        )
        result = bridge.stage(changed, authority=authority)
        assert result.status == "rejected"

    # Exact authority but an evaluation clock outside the bounded validity.
    _ds, _d, expiry_facts, _unused, current, current_authority, _p = _synthetic_bundle(
        tmp_path / "expired"
    )
    expired_bridge = ScienceOperationalFactBridge(
        desired_state_store=_ds,
        fact_revision_store=expiry_facts,
        clock=lambda: current.valid_until,
    )
    expired = expired_bridge.stage(current, authority=current_authority)
    assert expired.status == "rejected"
    assert expired.reason_codes == ("fact_not_current",)
    assert facts.audit()["retained_rows"] == 0
    assert desired_store.load_active() == desired


@pytest.mark.parametrize("fault_point", ("stage.after_insert", "stage.after_state"))
def test_fact_store_crash_before_commit_leaves_no_partial_ready_fact(tmp_path, fault_point):
    def fault(point):
        if point == fault_point:
            raise RuntimeError("simulated bridge crash")

    desired_store, desired, facts, bridge, request, authority, _path = _synthetic_bundle(
        tmp_path,
        fact_fault=fault,
    )
    fact_path = facts.path
    with pytest.raises(RuntimeError, match="simulated bridge crash"):
        bridge.stage(request, authority=authority)
    reopened = OperationalFactRevisionStore(fact_path, clock=lambda: NOW + 1.0)
    assert reopened.audit()["retained_rows"] == 0

    retry = ScienceOperationalFactBridge(
        desired_state_store=desired_store,
        fact_revision_store=reopened,
        clock=lambda: NOW + 1.0,
    ).stage(request, authority=authority)
    assert retry.status == "staged"
    assert reopened.next_ready(desired) is not None


def test_crash_after_commit_restarts_as_exact_duplicate(tmp_path):
    def fault(point):
        if point == "stage.after_commit":
            raise RuntimeError("simulated post-commit crash")

    desired_store, _desired, facts, bridge, request, authority, _path = _synthetic_bundle(
        tmp_path,
        fact_fault=fault,
    )
    with pytest.raises(RuntimeError, match="post-commit"):
        bridge.stage(request, authority=authority)
    reopened = OperationalFactRevisionStore(facts.path, clock=lambda: NOW + 1.0)
    assert reopened.audit()["ready"] == 1
    duplicate = ScienceOperationalFactBridge(
        desired_state_store=desired_store,
        fact_revision_store=reopened,
        clock=lambda: NOW + 1.0,
    ).stage(request, authority=authority)
    assert duplicate.status == "duplicate"
    assert reopened.audit()["retained_rows"] == 1


def test_payload_item_and_file_capacity_bounds_fail_closed(tmp_path):
    with pytest.raises(ScienceOperationalFactBridgeError, match="item bound"):
        ScienceFactProvenanceBinding(
            science_provenance_sha256=_sha("provenance"),
            profile_sha256=_sha("profile"),
            source_receipt_sha256s=tuple(
                sorted(_sha(f"source:{index}") for index in range(MAX_REFERENCE_HASHES + 1))
            ),
            certificate_sha256s=(),
        )

    _ds, _desired, facts, bridge, request, _authority, _path = _synthetic_bundle(
        tmp_path / "oversized"
    )
    huge = (tmp_path / "huge-authority.json").resolve()
    body = b"x" * (MAX_AUTHORITY_BYTES + 1)
    huge.write_bytes(body)
    result = bridge.stage(
        request,
        authority=SyntheticScienceTestFileAuthority(
            huge,
            expected_file_sha256=hashlib.sha256(body).hexdigest(),
        ),
    )
    assert result.status == "rejected"
    assert facts.audit()["retained_rows"] == 0


def test_result_diagnostics_are_bounded_private_and_bridge_has_no_live_imports(tmp_path):
    _ds, _desired, _facts, bridge, request, authority, path = _synthetic_bundle(tmp_path)
    result = bridge.stage(request, authority=authority)
    encoded = json.dumps(result.to_mapping(), sort_keys=True)
    assert len(encoded.encode()) < 4_096
    assert str(path) not in encoded
    assert str(request.footprint.latitude) not in encoded
    assert "token" not in encoded.lower()
    assert "password" not in encoded.lower()

    tree = ast.parse(MODULE.read_text(encoding="utf-8"))
    imports = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    imports.update(
        node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
    )
    assert not any(
        forbidden in name
        for name in imports
        for forbidden in ("service", "mqtt", "homeassistant", "legacy")
    )
