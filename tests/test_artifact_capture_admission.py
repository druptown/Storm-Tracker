"""Fail-closed tests for the shadow raw-capture admission bridge."""
from __future__ import annotations

import ast
from dataclasses import replace
import hashlib
import json
from pathlib import Path

import pytest

from storm_tracker_worker.tracking_core.engine import (
    artifact_capture_admission as admission,
)
from storm_tracker_worker.tracking_core.engine import (
    artifact_retention_planner as planner,
)
from storm_tracker_worker.tracking_core.engine.provider_artifact_contracts import (
    CONTRACTS,
    RedactionError,
    derive_record_id,
)
from storm_tracker_worker.tracking_core.engine.raw_artifact_store import (
    RawArtifactStore,
)


ROOT = Path(__file__).resolve().parents[1]
HA_MODULE = (
    ROOT
    / "custom_components"
    / "storm_tracker_v3"
    / "engine"
    / "artifact_capture_admission.py"
)
WORKER_MODULE = (
    ROOT
    / "storm_tracker_worker"
    / "tracking_core"
    / "engine"
    / "artifact_capture_admission.py"
)
DOC = ROOT / "docs" / "ARTIFACT_CAPTURE_ADMISSION.md"


def _attribution(provider: str) -> str:
    contract = CONTRACTS.get(provider)
    return (
        contract.attribution.value
        if contract is not None and contract.attribution.value
        else "UNKNOWN"
    )


def _policy(
    *providers: str,
    global_quota: int = 20_000,
    provider_quota: int = 10_000,
    reserve: int = 1_000,
) -> planner.RetentionPolicy:
    return planner.RetentionPolicy.from_mappings(
        global_quota_bytes=global_quota,
        reserve_free_bytes=reserve,
        per_provider_quota_bytes={
            provider: provider_quota for provider in providers
        },
        attribution_by_provider={
            provider: _attribution(provider) for provider in providers
        },
    )


def _capacity(
    *,
    free: int = 30_000,
    growth: int = 500,
) -> planner.CapacitySnapshot:
    return planner.CapacitySnapshot(
        free_bytes=free,
        projected_daily_growth_bytes=growth,
        measured_at=1_800_000_100.0,
    )


def _single_request(
    *,
    provider: str = "noaa_goes_rrqpe",
    receipt: str = "goes/object/20270115T120000Z",
    revision: str = "etag-a",
    payload: bytes = b"\x89HDF\r\nexact-upstream-netcdf",
    media_type: str = "application/x-netcdf",
    availability: admission.CaptureAvailability = (
        admission.CaptureAvailability.UPSTREAM_EXACT_SINGLE
    ),
    order: admission.CaptureOrder = admission.CaptureOrder.BEFORE_PARSE,
    received_at: float = 1_800_000_060.0,
) -> admission.CaptureRequest:
    member = admission.CapturedMember(
        member_id="primary",
        role="authoritative-body",
        media_type=media_type,
        payload=payload,
    )
    return admission.CaptureRequest(
        provider=provider,
        receipt_id=receipt,
        revision_id=revision,
        received_at=received_at,
        nominal_at=1_800_000_000.0,
        availability=availability,
        capture_order=order,
        expected_member_ids=("primary",),
        members=(member,),
    )


def _admit(
    request: admission.CaptureRequest,
    *,
    inventory=(),
    policy: planner.RetentionPolicy | None = None,
    capacity: planner.CapacitySnapshot | None = None,
) -> admission.CaptureDecision:
    return admission.admit_artifact_capture(
        request,
        inventory=inventory,
        policy=policy or _policy(request.provider),
        capacity=capacity or _capacity(),
    )


def _inventory_evidence(
    request: admission.CaptureRequest,
    decision: admission.CaptureDecision,
    **overrides,
) -> planner.ArtifactEvidence:
    values = {
        "key": planner.ArtifactKey(
            request.provider,
            request.receipt_id,
            request.revision_id,
        ),
        "size_bytes": decision.byte_length,
        "received_at": request.received_at,
        "nominal_at": request.nominal_at,
        "content_sha256": decision.payload_sha256,
        "attribution": _attribution(request.provider),
        "provenance": tuple(sorted((
            ("availability", request.availability.value),
            ("capture_order", request.capture_order.value),
            ("member_manifest_sha256", decision.payload_sha256),
            (
                "provider_contract_sha256",
                decision.provider_contract_sha256,
            ),
        ))),
    }
    values.update(overrides)
    return planner.ArtifactEvidence(**values)


def test_exact_before_parse_store_decision_and_receipt_metadata():
    request = _single_request(
        payload=b"Bearer this-is-payload-content-not-receipt-metadata",
    )

    decision = _admit(request)
    metadata = decision.to_receipt_metadata()
    rendered = json.dumps(metadata, sort_keys=True)

    assert decision.processing_allowed is True
    assert decision.retention_decision == "STORE"
    assert (
        decision.disposition
        is admission.RawWriteDisposition.STORE_AUTHORIZED
    )
    assert decision.durable_raw_write_authorized is True
    assert decision.payload_sha256 == hashlib.sha256(
        request.members[0].payload
    ).hexdigest()
    assert decision.byte_length == len(request.members[0].payload)
    assert decision.product == CONTRACTS[request.provider].product
    assert decision.received_at == request.received_at
    assert decision.nominal_at == request.nominal_at
    assert len(decision.contract_registry_sha256) == 64
    assert len(decision.provider_contract_sha256) == 64
    assert len(decision.retention_plan_sha256) == 64
    assert len(decision.decision_sha256) == 64
    assert len(decision.write_plan) == 1
    assert "Bearer this-is-payload-content" not in rendered
    assert "payload" not in metadata["members"][0]


def test_authorized_single_write_is_raw_store_idempotent_and_revision_safe(
    tmp_path,
):
    store = RawArtifactStore(tmp_path / "raw.sqlite3")
    store.initialize()
    first_request = _single_request()
    first_decision = _admit(first_request)

    first = admission.execute_authorized_store(
        first_request,
        first_decision,
        store,
    )
    redelivery = admission.execute_authorized_store(
        first_request,
        first_decision,
        store,
    )

    assert redelivery == first
    assert store.stats()["receipt_count"] == 1
    assert store.load_payload(first.raw_store_receipt_ids[0]) == (
        first_request.members[0].payload
    )

    revision_request = _single_request(
        revision="etag-b",
        payload=b"\x89HDF\r\nsecond-source-revision",
    )
    revision_decision = _admit(revision_request)
    second = admission.execute_authorized_store(
        revision_request,
        revision_decision,
        store,
    )
    assert second.raw_store_receipt_ids != first.raw_store_receipt_ids
    assert store.stats()["receipt_count"] == 2
    assert store.stats()["blob_count"] == 2


def test_after_parse_never_writes_but_processing_stays_allowed():
    request = _single_request(order=admission.CaptureOrder.AFTER_PARSE)
    decision = _admit(request)

    assert decision.processing_allowed is True
    assert decision.retention_decision == "STORE"
    assert decision.durable_raw_write_authorized is False
    assert decision.disposition is admission.RawWriteDisposition.RAW_CAPTURE_BLOCKED
    assert "EXACT_BYTES_NOT_CAPTURED_BEFORE_PARSE" in decision.reasons
    assert decision.write_plan == ()

    class StoreMustNotBeCalled:
        def __init__(self):
            self.calls = 0

        def store(self, **_):
            self.calls += 1
            raise AssertionError

    store = StoreMustNotBeCalled()
    with pytest.raises(
        admission.CaptureExecutionError,
        match="not authorized",
    ):
        admission.execute_authorized_store(request, decision, store)
    assert store.calls == 0


@pytest.mark.parametrize(
    ("provider", "availability", "media_type", "reason", "retention"),
    (
        (
            "netatmo",
            admission.CaptureAvailability.DECODED_BODY_ONLY,
            "application/json",
            "DECODED_BODY_IS_NOT_UPSTREAM_RAW",
            "HOLD",
        ),
        (
            "noaa_enterprise_rainrate",
            admission.CaptureAvailability.WORKER_DERIVED_ONLY,
            "application/x-netcdf",
            "WORKER_DERIVED_ARTIFACT_IS_NOT_UPSTREAM_RAW",
            "STORE",
        ),
        (
            "future_provider",
            admission.CaptureAvailability.UNKNOWN,
            "application/octet-stream",
            "UNKNOWN_CAPTURE_AVAILABILITY",
            "HOLD",
        ),
    ),
)
def test_nonraw_and_unknown_status_fail_closed_for_storage_not_processing(
    provider,
    availability,
    media_type,
    reason,
    retention,
):
    request = _single_request(
        provider=provider,
        receipt=f"{provider}/record",
        media_type=media_type,
        availability=availability,
    )

    decision = _admit(request)

    assert decision.processing_allowed is True
    assert decision.retention_decision == retention
    assert decision.durable_raw_write_authorized is False
    assert reason in decision.reasons
    assert decision.payload_sha256 == hashlib.sha256(
        request.members[0].payload
    ).hexdigest()


def test_contract_review_hold_is_distinct_from_capture_availability():
    request = _single_request(
        provider="kmi",
        receipt="animation/frame/20270115T1200",
        media_type="image/png",
        payload=b"\x89PNG\r\n\x1a\nexact-kmi-image",
    )

    decision = _admit(request)

    assert decision.artifact_set_complete is True
    assert decision.retention_decision == "HOLD"
    assert decision.disposition is admission.RawWriteDisposition.HOLD
    assert decision.durable_raw_write_authorized is False
    assert "CONTRACT_REVIEW_HOLD" in decision.reasons
    assert "LICENSE_UNKNOWN" in decision.reasons
    assert decision.processing_allowed is True


def _rainviewer_request(
    *,
    reversed_members: bool = False,
    missing_tile: bool = False,
) -> admission.CaptureRequest:
    manifest = admission.CapturedMember(
        "manifest",
        "weather-maps-manifest",
        "application/json",
        b'{"radar":{"past":[]}}',
    )
    tile = admission.CapturedMember(
        "tile-z7-x65-y42",
        "radar-tile",
        "image/png",
        b"\x89PNG\r\n\x1a\nexact-tile",
    )
    members = (manifest,) if missing_tile else (manifest, tile)
    if reversed_members:
        members = tuple(reversed(members))
    return admission.CaptureRequest(
        provider="rainviewer",
        receipt_id="past/frame/1800000000",
        revision_id="manifest-revision-a",
        received_at=1_800_000_060.0,
        nominal_at=1_800_000_000.0,
        availability=admission.CaptureAvailability.UPSTREAM_EXACT_MULTI,
        capture_order=admission.CaptureOrder.BEFORE_PARSE,
        expected_member_ids=("tile-z7-x65-y42", "manifest"),
        members=members,
    )


def _authorized_rainviewer_decision(
    request: admission.CaptureRequest,
) -> admission.CaptureDecision:
    held = _admit(request)
    return replace(
        held,
        retention_decision="STORE",
        disposition=admission.RawWriteDisposition.STORE_AUTHORIZED,
        durable_raw_write_authorized=True,
        write_plan=tuple(
            admission.RawStoreWritePlan(
                member_id=member.member_id,
                upstream_record_id=derive_record_id(
                    request.provider,
                    source_record_id=(
                        f"{request.receipt_id}/revision/"
                        f"{request.revision_id}/member/{member.member_id}"
                    ),
                    nominal_timestamp=request.nominal_at,
                    payload_digest=member.payload_sha256,
                ),
                payload_sha256=member.payload_sha256,
                byte_length=member.byte_length,
                media_type=member.media_type,
                parser_version=CONTRACTS["rainviewer"].parser_version,
            )
            for member in held.members
        ),
    )


def test_multi_artifact_order_is_canonical_and_missing_member_fails_closed():
    first = _admit(_rainviewer_request())
    reversed_decision = _admit(_rainviewer_request(reversed_members=True))

    assert first.decision_sha256 == reversed_decision.decision_sha256
    assert first.payload_sha256 == reversed_decision.payload_sha256
    assert first.payload_hash_scope == (
        "canonical_complete_member_manifest_not_concatenated_bytes"
    )
    assert tuple(member.member_id for member in first.members) == (
        "manifest",
        "tile-z7-x65-y42",
    )
    assert first.atomic_set_required is True
    assert first.artifact_set_complete is True
    assert first.retention_decision == "HOLD"

    incomplete = _admit(_rainviewer_request(missing_tile=True))
    assert incomplete.processing_allowed is True
    assert incomplete.atomic_set_required is True
    assert incomplete.artifact_set_complete is False
    assert incomplete.missing_member_ids == ("tile-z7-x65-y42",)
    assert "ARTIFACT_SET_MEMBERS_MISSING" in incomplete.reasons
    assert incomplete.durable_raw_write_authorized is False


def test_multi_artifact_execution_refuses_before_any_partial_store_call():
    request = _rainviewer_request()
    forged_store_state = _authorized_rainviewer_decision(request)

    class CountingStore:
        def __init__(self):
            self.calls = 0

        def store(self, **_):
            self.calls += 1
            raise AssertionError

    store = CountingStore()
    with pytest.raises(admission.AtomicArtifactSetUnsupported):
        admission.execute_authorized_store(
            request,
            forged_store_state,
            store,
        )
    assert store.calls == 0


def test_multi_artifact_execution_is_atomic_idempotent_and_replayable(
    tmp_path,
):
    request = _rainviewer_request()
    decision = _authorized_rainviewer_decision(request)
    store = RawArtifactStore(tmp_path / "raw.sqlite3")
    store.initialize()

    first = admission.execute_authorized_store(request, decision, store)
    redelivery = admission.execute_authorized_store(
        request,
        decision,
        store,
    )

    assert redelivery == first
    assert first.raw_store_artifact_set_id is not None
    assert len(first.raw_store_manifest_sha256 or "") == 64
    assert len(first.raw_store_receipt_ids) == 2
    assert first.payload_sha256_values == tuple(
        member.payload_sha256 for member in decision.members
    )
    assert first.byte_length == decision.byte_length
    assert store.stats()["artifact_set_count"] == 1
    assert store.stats()["receipt_count"] == 2
    stored_set = store.load_set_receipt(
        first.raw_store_artifact_set_id
    )
    assert stored_set is not None
    assert stored_set.provenance_sha256 == decision.decision_sha256
    assert stored_set.manifest_sha256 == first.raw_store_manifest_sha256
    assert store.load_set_payloads(first.raw_store_artifact_set_id) == (
        ("manifest", request.members[0].payload),
        ("tile-z7-x65-y42", request.members[1].payload),
    )
    assert store.integrity_check()["healthy"] is True


def _archived_artifact(size: int = 200) -> planner.ArtifactEvidence:
    digest = "a" * 64
    return planner.ArtifactEvidence(
        key=planner.ArtifactKey("dwd_radolan", "old", "revision-1"),
        size_bytes=size,
        received_at=1_700_000_000.0,
        nominal_at=1_699_999_900.0,
        content_sha256=digest,
        attribution=_attribution("dwd_radolan"),
        provenance=(("source_record", "dwd-old"),),
        archive_state=planner.ArchiveState.ARCHIVE_VERIFIED,
        archive_checksum_sha256=digest,
        archive_checksum_verified=True,
        archive_receipt_id="archive-receipt",
    )


def test_capacity_store_archive_required_and_reject_remain_explicit():
    request = _single_request(payload=b"x" * 100)
    store = _admit(request)
    assert store.retention_decision == "STORE"
    assert store.disposition is admission.RawWriteDisposition.STORE_AUTHORIZED

    archived = _archived_artifact(200)
    archive_required = _admit(
        request,
        inventory=(archived,),
        policy=_policy(
            "noaa_goes_rrqpe",
            "dwd_radolan",
            global_quota=250,
            provider_quota=1_000,
        ),
    )
    assert archive_required.retention_decision == "ARCHIVE_REQUIRED"
    assert (
        archive_required.disposition
        is admission.RawWriteDisposition.ARCHIVE_REQUIRED
    )
    assert archive_required.durable_raw_write_authorized is False
    assert archive_required.processing_allowed is True

    rejected = _admit(
        request,
        policy=_policy(
            "noaa_goes_rrqpe",
            global_quota=50,
            provider_quota=50,
        ),
    )
    assert rejected.retention_decision == "REJECT_CAPACITY"
    assert (
        rejected.disposition
        is admission.RawWriteDisposition.REJECT_CAPACITY
    )
    assert rejected.processing_allowed is True


def test_plan_hash_commits_caller_policy_even_when_both_decisions_store():
    request = _single_request(payload=b"x" * 100)

    first = _admit(
        request,
        policy=_policy(
            "noaa_goes_rrqpe",
            global_quota=10_000,
            provider_quota=5_000,
        ),
    )
    second = _admit(
        request,
        policy=_policy(
            "noaa_goes_rrqpe",
            global_quota=20_000,
            provider_quota=10_000,
        ),
    )

    assert first.retention_decision == second.retention_decision == "STORE"
    assert first.payload_sha256 == second.payload_sha256
    assert first.retention_plan_sha256 != second.retention_plan_sha256
    assert first.decision_sha256 != second.decision_sha256


def test_inventory_redelivery_and_revision_conflict_are_separate():
    original_request = _single_request()
    original = _admit(original_request)
    existing = _inventory_evidence(original_request, original)
    redelivery_request = _single_request(received_at=1_800_000_120.0)

    redelivery = _admit(redelivery_request, inventory=(existing,))

    assert redelivery.retention_decision == "IDEMPOTENT_ALREADY_PRESENT"
    assert (
        redelivery.disposition
        is admission.RawWriteDisposition.IDEMPOTENT_ALREADY_PRESENT
    )
    assert redelivery.durable_raw_write_authorized is False
    assert redelivery.processing_allowed is True

    conflict_request = _single_request(payload=b"different-same-revision")
    conflict = _admit(conflict_request, inventory=(existing,))
    assert conflict.retention_decision == "REVISION_CONFLICT"
    assert (
        conflict.disposition
        is admission.RawWriteDisposition.REVISION_CONFLICT
    )
    assert "REVISION_IDENTITY_REUSED_FOR_DIFFERENT_CONTENT" in conflict.reasons
    assert conflict.processing_allowed is True


@pytest.mark.parametrize(
    "factory",
    (
        lambda: _single_request(
            receipt="https://example.invalid/frame?token=secret",
        ),
        lambda: admission.CapturedMember(
            "tile?access_token=secret",
            "tile",
            "image/png",
            b"bytes",
        ),
    ),
)
def test_record_identities_reject_urls_and_secret_material(factory):
    with pytest.raises((RedactionError, ValueError)):
        factory()


def test_store_failure_returns_no_false_execution_receipt():
    request = _single_request()
    decision = _admit(request)

    class FailingStore:
        calls = 0

        def store(self, **_):
            self.calls += 1
            raise RuntimeError("simulated crash before store receipt")

    store = FailingStore()
    with pytest.raises(RuntimeError, match="simulated crash"):
        admission.execute_authorized_store(request, decision, store)
    assert store.calls == 1
    assert decision.durable_raw_write_authorized is True
    assert decision.processing_allowed is True


@pytest.mark.parametrize(
    ("name", "mutate"),
    (
        (
            "availability",
            lambda request: replace(
                request,
                availability=admission.CaptureAvailability.DECODED_BODY_ONLY,
            ),
        ),
        (
            "capture_order",
            lambda request: replace(
                request,
                capture_order=admission.CaptureOrder.AFTER_PARSE,
            ),
        ),
        (
            "expected_member_ids",
            lambda request: replace(
                request,
                expected_member_ids=("primary", "missing-secondary"),
            ),
        ),
    ),
)
def test_executor_binds_every_capture_boundary_fact_before_write(
    tmp_path,
    name,
    mutate,
):
    request = _single_request()
    decision = _admit(request)
    changed_request = mutate(request)
    store = RawArtifactStore(tmp_path / f"{name}.sqlite3")
    store.initialize()

    with pytest.raises(
        admission.CaptureExecutionError,
        match="request does not match capture decision",
    ):
        admission.execute_authorized_store(
            changed_request,
            decision,
            store,
        )

    assert store.stats()["receipt_count"] == 0


@pytest.mark.parametrize(
    "mutate",
    (
        lambda decision: replace(
            decision,
            schema_version="artifact-capture-admission-future",
        ),
        lambda decision: replace(
            decision,
            provider_contract_sha256="f" * 64,
        ),
    ),
)
def test_executor_rejects_stale_or_forged_contract_decision(
    tmp_path,
    mutate,
):
    request = _single_request()
    decision = mutate(_admit(request))
    store = RawArtifactStore(tmp_path / "raw.sqlite3")
    store.initialize()

    with pytest.raises(admission.CaptureExecutionError):
        admission.execute_authorized_store(request, decision, store)

    assert store.stats()["receipt_count"] == 0


def test_executor_rederives_write_plan_before_write(tmp_path):
    request = _single_request()
    decision = _admit(request)
    forged_write = replace(
        decision.write_plan[0],
        upstream_record_id="forged-but-secret-free-record-id",
        parser_version="forged-parser",
    )
    forged_decision = replace(decision, write_plan=(forged_write,))
    store = RawArtifactStore(tmp_path / "raw.sqlite3")
    store.initialize()

    with pytest.raises(
        admission.CaptureExecutionError,
        match="write plan does not match",
    ):
        admission.execute_authorized_store(request, forged_decision, store)

    assert store.stats()["receipt_count"] == 0


def test_ha_worker_mirror_and_no_live_imports_or_provider_transports():
    assert HA_MODULE.read_bytes() == WORKER_MODULE.read_bytes()
    assert hashlib.sha256(HA_MODULE.read_bytes()).hexdigest() == hashlib.sha256(
        WORKER_MODULE.read_bytes()
    ).hexdigest()

    tree = ast.parse(WORKER_MODULE.read_text(encoding="utf-8"))
    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        node.module or ""
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    }
    assert not any("providers." in name for name in imported)
    assert not any("raw_artifact_store" in name for name in imported)

    imports = []
    for root in (
        ROOT / "custom_components" / "storm_tracker_v3",
        ROOT / "storm_tracker_worker",
    ):
        for path in root.rglob("*.py"):
            if path.name == "artifact_capture_admission.py":
                continue
            parsed = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(parsed):
                if isinstance(node, ast.ImportFrom) and (
                    "artifact_capture_admission" in (node.module or "")
                ):
                    imports.append(path)
                if isinstance(node, ast.Import) and any(
                    "artifact_capture_admission" in alias.name
                    for alias in node.names
                ):
                    imports.append(path)
    assert imports == []
    assert admission.SHADOW_ONLY is True


def test_documentation_keeps_processing_and_raw_retention_separate():
    text = DOC.read_text(encoding="utf-8")
    assert "processing_allowed=True" in text
    assert "provider_overlay_not_upstream_raw" in text
    assert "geen live wiring" in text
    assert "atomische multi-artifact" in text
