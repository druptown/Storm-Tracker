"""Fail-closed proofs for the certified shadow-to-fact composition."""
from __future__ import annotations

import ast
from dataclasses import dataclass, replace
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

from storm_tracker_worker.acquisition_capture_recovery import (
    RECOVERY_BINDING_CATALOG,
    RecoveredCaptureEvidence,
)
from storm_tracker_worker.canonical_evidence_store import (
    CanonicalEvidenceStore,
    DecoderMemberBinding,
    DecoderRun,
    DecoderRunReceipt,
    NonOperationalCandidateArtifactRef,
)
from storm_tracker_worker.certified_shadow_fact_pipeline import (
    CERTIFIED_SHADOW_FACT_PIPELINE_ALGORITHM,
    CertifiedRawRecoveryReference,
    CertifiedShadowFactPipeline,
    CertifiedShadowFactPipelineCapacityError,
    CertifiedShadowFactPipelineError,
    CertifiedShadowFactPipelineReceipt,
    CertifiedShadowFactPipelineStore,
)
from storm_tracker_worker.desired_state import DesiredStateStore
from storm_tracker_worker.hard_decoder_isolation import _canonical_json_bytes
from storm_tracker_worker.operational_fact_store import OperationalFactRevisionStore
from storm_tracker_worker.science_operational_fact_bridge import (
    DurableScienceShadowFactAuthority,
    ScienceFactProvenanceBinding,
    ScienceOperationalFactBridge,
    ScienceOperationalFactRequest,
    ScienceTargetFootprint,
    SyntheticScienceTestFileAuthority,
    SyntheticScienceTestReceipt,
    encode_synthetic_science_test_receipt,
    science_receipt_provenance_sha256,
)
from storm_tracker_worker.science_shadow_orchestrator import (
    ScienceShadowLimits,
    ScienceShadowReceiptStore,
)
from storm_tracker_worker.science_shadow_process_isolation import (
    SCIENCE_SHADOW_PROCESS_AUTHORITY_CONTRACT,
    ScienceShadowFactoryContract,
    ScienceShadowIsolatedRunner,
    ScienceShadowIsolationLimits,
    ScienceShadowProcessResult,
    _receipt_metadata,
)
from tests.helpers.science_shadow_process_factory import (
    build_test_orchestrator,
    runtime_profile_for_authority,
)


NOW = 1_800_950_000.0
ROOT = Path(__file__).parents[1]
MODULE = ROOT / "storm_tracker_worker/certified_shadow_fact_pipeline.py"
CRASH_HELPER = (
    ROOT / "tests/helpers/certified_shadow_fact_pipeline_crash_helper.py"
)
FACTORY_TARGET = (
    "tests.helpers.science_shadow_process_factory:build_test_orchestrator"
)


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _run(number: int) -> str:
    return "run-" + format(number, "064x")


def _desired(tmp_path: Path, *, synthetic: bool):
    producer = "synthetic-test:ha" if synthetic else "ha:certified-pipeline-test"
    target_id = "synthetic-test:target" if synthetic else "zone.home"
    store = DesiredStateStore((tmp_path / "desired.sqlite3").resolve())
    claim = store.claim_producer(
        producer,
        "claim:certified-pipeline",
        claimed_at=NOW - 120.0,
    )
    store.apply(
        {
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
            "targets": [
                {
                    "available": True,
                    "country_code": "BE",
                    "display_name": "Private Home Target",
                    "kind": "test" if synthetic else "home",
                    "latitude": 51.04,
                    "location_accuracy_m": 10.0,
                    "longitude": 4.48,
                    "observation_radius_km": 250.0,
                    "position_observed_at": NOW - 60.0,
                    "target_id": target_id,
                }
            ],
        },
        received_at=NOW - 80.0,
    )
    desired = store.load_active()
    assert desired is not None
    return store, desired


def _science_limits() -> ScienceShadowLimits:
    return ScienceShadowLimits(
        max_runtime_seconds=30.0,
        max_receipts=64,
        checkpoint_interval_frames=4,
    )


def _science_receipt(tmp_path: Path, *, run_id: str):
    receipt_path = (tmp_path / "science-preview.sqlite3").resolve()
    limits = _science_limits()
    document = {
        "authorized_paths": [
            {
                "access": "transactional_file",
                "content_sha256": None,
                "label": "science_receipts",
                "path": str(receipt_path),
            }
        ],
        "engine_id": "certified-pipeline-test-engine",
        "factory_config": {"mode": "normal"},
        "receipt_store_label": "science_receipts",
        "science_limits": limits.to_mapping(),
    }
    orchestrator = build_test_orchestrator(document)
    result = orchestrator.process(
        attempt_id="attempt-certified-pipeline",
        run_id=run_id,
        deadline_at=time.monotonic() + 10.0,
    )
    assert result.receipt.outcome.value == "advanced"
    assert result.receipt.provenance is not None
    return orchestrator, result.receipt


def _real_process_boundary(tmp_path: Path, science_receipt):
    """Build an empty, checksummed authority for one genuine isolated run.

    The in-process receipt above is a deterministic preview used only to bind
    the downstream request before execution.  This authority uses a different,
    initially empty store; therefore the pipeline must cross the actual spawned
    ``ScienceShadowIsolatedRunner`` boundary to make that receipt durable.
    """

    isolation_limits = ScienceShadowIsolationLimits(
        wall_seconds=6.0,
        cpu_seconds=4.0,
        rss_bytes=1024 * 1024**2,
        max_authority_bytes=256 * 1024,
        max_request_bytes=32 * 1024,
        max_result_bytes=64 * 1024,
        max_authorized_paths=8,
    )
    science_limits = _science_limits()
    factory = ScienceShadowFactoryContract(
        factory_id="science-process-test-factory-v1",
        target=FACTORY_TARGET,
        opens_only_authorized_paths=True,
        hard_kill_safe=True,
        transactional_receipt_only=True,
        spawns_descendants=False,
    )
    receipt_path = (tmp_path / "science-authority.sqlite3").resolve()
    document = {
        "authorized_paths": [{
            "access": "transactional_file",
            "content_sha256": None,
            "label": "science_receipts",
            "path": str(receipt_path),
        }],
        "contract": SCIENCE_SHADOW_PROCESS_AUTHORITY_CONTRACT,
        "engine_id": science_receipt.engine_id,
        "factory_config": {"mode": "normal"},
        "factory_contract_sha256": factory.fingerprint,
        "factory_id": factory.factory_id,
        "isolation_limits": isolation_limits.to_mapping(),
        "receipt_store_label": "science_receipts",
        "runtime_profile_sha256": "0" * 64,
        "science_limits": science_limits.to_mapping(),
    }
    document["runtime_profile_sha256"] = runtime_profile_for_authority(document)
    assert document["runtime_profile_sha256"] == science_receipt.runtime_profile_sha256
    authority_path = (tmp_path / "science-process-authority.json").resolve()
    authority_body = _canonical_json_bytes(document)
    authority_path.write_bytes(authority_body)
    authority_sha256 = hashlib.sha256(authority_body).hexdigest()
    metadata = _receipt_metadata(
        science_receipt,
        authority_sha256=authority_sha256,
    )
    predicted_result = ScienceShadowProcessResult(
        status="success",
        execution_outcome="succeeded",
        detail_code="child_receipt_verified",
        authority_sha256=authority_sha256,
        durable_disposition="committed",
        receipt_metadata=metadata,
        duplicate=False,
        isolation_enforced=True,
        child_exit_code=0,
        elapsed_seconds=0.0,
    )
    runner = ScienceShadowIsolatedRunner(
        authority_path=authority_path,
        expected_authority_sha256=authority_sha256,
        factory_contract=factory,
        limits=isolation_limits,
    )
    receipt_store = ScienceShadowReceiptStore(
        receipt_path,
        engine_id=document["engine_id"],
        runtime_profile_sha256=document["runtime_profile_sha256"],
        limits=science_limits,
    )
    assert receipt_store.stats()["receipt_count"] == 0
    return runner, receipt_store, predicted_result


def _single_binding():
    return next(
        binding
        for _handler_id, binding in sorted(RECOVERY_BINDING_CATALOG.items())
        if binding.minimum_artifacts == binding.maximum_artifacts == 1
    )


def _raw_and_canonical(science_receipt):
    provenance = science_receipt.provenance
    assert provenance is not None
    raw_content_sha = _sha("certified-raw-content")
    raw_role_receipt_sha = _sha("certified-raw-role-receipt")
    raw = RecoveredCaptureEvidence(
        capture_id=provenance.capture_id,
        actual_bytes=128,
        actual_entries=1,
        content_sha256=raw_content_sha,
        receipt_sha256=raw_role_receipt_sha,
        capture_sha256=provenance.capture_receipt_sha256,
        capacity_commit_sha256=_sha("certified-capacity-commit"),
        already_committed=False,
    )
    run = DecoderRun(
        decoder_job_id="decoder-job-" + "a" * 64,
        attempt_id="decoder-attempt-" + "b" * 64,
        capture_id=provenance.capture_id,
        capture_receipt_sha256=provenance.capture_receipt_sha256,
        members=(
            DecoderMemberBinding(
                member_id="member-" + _sha("member"),
                role="payload",
                receipt_id="receipt-" + _sha("member-receipt-id"),
                content_sha256=raw_content_sha,
                receipt_sha256=raw_role_receipt_sha,
                received_at=NOW - 30.0,
            ),
        ),
        decoder_identity="certified-test-decoder",
        decoder_version="decoder-v1",
        config_sha256=_sha("decoder-config"),
        provider="synthetic-provider",
        product="synthetic-radar",
        product_time=NOW - 60.0,
        received_at=NOW - 30.0,
        evaluated_at=NOW - 20.0,
        quantity="instantaneous_precipitation_rate",
        unit="mm/h",
        native_crs="EPSG:4326",
        native_grid=None,
        coverage=None,
        non_operational_candidate_artifact=NonOperationalCandidateArtifactRef(
            handle="artifact:decoded:certified-pipeline",
            sha256=_sha("canonical-candidate"),
        ),
        outcome="decoded_complete",
        reason_code="decoded_complete",
    )
    canonical = DecoderRunReceipt(
        run_id=science_receipt.canonical_run_id,
        commit_id=provenance.canonical_commit_id,
        revision_scope_id="revision-scope-" + _sha("revision-scope"),
        revision=1,
        previous_receipt_sha256=None,
        revision_sha256=_sha("canonical-revision"),
        receipt_sha256=provenance.canonical_receipt_sha256,
        result_sha256=provenance.canonical_result_sha256,
        capture_manifest_sha256=provenance.capture_manifest_sha256,
        committed_at=NOW - 15.0,
        run=run,
    )
    store = object.__new__(CanonicalEvidenceStore)
    store.load = lambda run_id: canonical if run_id == canonical.run_id else None
    return raw, canonical, store


class _Runner:
    def __init__(self, result: ScienceShadowProcessResult) -> None:
        self.result = result
        self.calls = 0

    def execute(
        self,
        *,
        attempt_id,
        canonical_run_id,
        lineage_hypotheses=(),
        cancel_event=None,
    ):
        del lineage_hypotheses, cancel_event
        self.calls += 1
        assert attempt_id == "attempt-certified-pipeline"
        if self.result.receipt_metadata is not None:
            assert canonical_run_id == self.result.receipt_metadata["canonical_run_id"]
        return self.result


@dataclass
class _Bundle:
    pipeline: CertifiedShadowFactPipeline
    pipeline_store: CertifiedShadowFactPipelineStore
    desired_store: DesiredStateStore
    facts: OperationalFactRevisionStore
    bridge: ScienceOperationalFactBridge
    request: ScienceOperationalFactRequest
    authority: object
    binding: object
    raw: RecoveredCaptureEvidence
    canonical: DecoderRunReceipt
    canonical_store: CanonicalEvidenceStore
    runner: object
    science_store: ScienceShadowReceiptStore | None
    process_result: ScienceShadowProcessResult


def _bundle(
    tmp_path: Path,
    *,
    synthetic: bool,
    pipeline_fault=None,
    monotonic=time.monotonic,
) -> _Bundle:
    tmp_path.mkdir(parents=True, exist_ok=True)
    desired_store, desired = _desired(tmp_path, synthetic=synthetic)
    orchestrator, science_receipt = _science_receipt(
        tmp_path,
        run_id=_run(41 if synthetic else 40),
    )
    raw, canonical, canonical_store = _raw_and_canonical(science_receipt)
    if synthetic:
        process_authority_sha = _sha("certified-process-authority")
        metadata = _receipt_metadata(
            science_receipt,
            authority_sha256=process_authority_sha,
        )
        process_result = ScienceShadowProcessResult(
            status="success",
            execution_outcome="succeeded",
            detail_code="child_receipt_verified",
            authority_sha256=process_authority_sha,
            durable_disposition="committed",
            receipt_metadata=metadata,
            duplicate=False,
            isolation_enforced=True,
            child_exit_code=0,
            elapsed_seconds=0.25,
        )
        runner: object = _Runner(process_result)
        science_store = None
    else:
        runner, science_store, process_result = _real_process_boundary(
            tmp_path,
            science_receipt,
        )
        process_authority_sha = process_result.authority_sha256
        assert process_result.receipt_metadata is not None
        metadata = process_result.receipt_metadata
    provenance = science_receipt.provenance
    assert provenance is not None
    sources = tuple(
        sorted(
            {
                raw.receipt_sha256,
                raw.capture_sha256,
                canonical.receipt_sha256,
                science_receipt.receipt_sha256,
            }
        )
    )
    certificates = {
        provenance.radar_semantics_certificate_sha256,
        provenance.transform_certificate_sha256,
    }
    if synthetic:
        certificates.add(_sha("synthetic-test-admission-certificate"))
    target = desired.targets[0]
    preliminary = ScienceOperationalFactRequest(
        science_receipt_sha256=(
            "0" * 64 if synthetic else science_receipt.receipt_sha256
        ),
        science_attempt_id=science_receipt.attempt_id,
        science_request_sha256=science_receipt.request_sha256,
        canonical_run_id=science_receipt.canonical_run_id,
        engine_id=science_receipt.engine_id,
        runtime_profile_sha256=science_receipt.runtime_profile_sha256,
        process_authority_sha256=process_authority_sha,
        process_receipt_sha256=metadata["metadata_sha256"],
        desired_state_sha256=desired.state_sha256,
        authority_epoch=desired.authority_epoch,
        snapshot_revision=1,
        footprint=ScienceTargetFootprint.from_target(target),
        provenance=ScienceFactProvenanceBinding(
            science_provenance_sha256=science_receipt_provenance_sha256(
                science_receipt
            ),
            profile_sha256=science_receipt.runtime_profile_sha256,
            source_receipt_sha256s=sources,
            certificate_sha256s=tuple(sorted(certificates)),
        ),
        product_valid_at=NOW - 60.0,
        received_at=NOW - 30.0,
        evaluated_at=NOW,
        valid_until=NOW + 120.0,
        rain_rate_mm_h=1.25,
        provider_id=("synthetic-test:radar" if synthetic else "kmi"),
        provider_display_name=("Synthetic radar" if synthetic else "KMI"),
        region_id=("synthetic-test:region" if synthetic else "region:home"),
        region_display_name=("Synthetic region" if synthetic else "Regio thuis"),
    )
    if synthetic:
        synthetic_receipt = SyntheticScienceTestReceipt.create(
            authority_id="synthetic-test:authority",
            request=preliminary,
            certificate_sha256s=preliminary.provenance.certificate_sha256s,
            decided_at=NOW - 1.0,
        )
        request = replace(
            preliminary,
            science_receipt_sha256=synthetic_receipt.receipt_sha256,
        )
        authority_body = encode_synthetic_science_test_receipt(synthetic_receipt)
        authority_path = (tmp_path / "synthetic-authority.json").resolve()
        authority_path.write_bytes(authority_body)
        authority = SyntheticScienceTestFileAuthority(
            authority_path,
            expected_file_sha256=hashlib.sha256(authority_body).hexdigest(),
        )
    else:
        request = preliminary
        assert science_store is not None
        authority = DurableScienceShadowFactAuthority(science_store)
    facts = OperationalFactRevisionStore(
        (tmp_path / "facts.sqlite3").resolve(),
        clock=lambda: NOW + 1.0,
    )
    bridge = ScienceOperationalFactBridge(
        desired_state_store=desired_store,
        fact_revision_store=facts,
        clock=lambda: NOW + 1.0,
    )
    pipeline_store = CertifiedShadowFactPipelineStore(
        (tmp_path / "pipeline.sqlite3").resolve(),
        clock=lambda: NOW + 2.0,
        fault_injector=pipeline_fault,
    )
    pipeline = CertifiedShadowFactPipeline(
        desired_state_store=desired_store,
        canonical_evidence_store=canonical_store,
        science_process_runner=runner,
        fact_bridge=bridge,
        receipt_store=pipeline_store,
        monotonic=monotonic,
    )
    return _Bundle(
        pipeline=pipeline,
        pipeline_store=pipeline_store,
        desired_store=desired_store,
        facts=facts,
        bridge=bridge,
        request=request,
        authority=authority,
        binding=_single_binding(),
        raw=raw,
        canonical=canonical,
        canonical_store=canonical_store,
        runner=runner,
        science_store=science_store,
        process_result=process_result,
    )


def _process(bundle: _Bundle, *, deadline_at=None):
    return bundle.pipeline.process(
        bundle.request,
        recovery_binding=bundle.binding,
        raw_recovery_evidence=bundle.raw,
        authority=bundle.authority,
        deadline_at=(time.monotonic() + 30.0 if deadline_at is None else deadline_at),
    )


def test_real_primary_repeat_and_reopen_cross_the_actual_isolated_boundary(
    tmp_path,
):
    bundle = _bundle(tmp_path / "primary", synthetic=False)
    assert type(bundle.runner) is ScienceShadowIsolatedRunner
    assert bundle.science_store is not None
    assert bundle.science_store.stats()["receipt_count"] == 0

    first = _process(bundle)
    repeated = _process(bundle)

    receipt = first.receipt
    assert first.duplicate is False
    assert repeated.duplicate is True
    assert repeated.receipt == receipt
    assert bundle.science_store.stats()["receipt_count"] == 1
    assert receipt.science_process_invoked is True
    assert receipt.science_process_verified is True
    assert receipt.status == "withheld"
    assert receipt.stage_coverage_complete is True
    assert set(receipt.verified_stages) == {
        "analysis_grid",
        "canonical_decoder_normalizer",
        "cell_detection",
        "cell_tracking",
        "fact_staging_boundary",
        "native_radar_field",
        "raw_recovery",
    }
    assert receipt.meteorological_admission_granted is False
    assert receipt.created_ready_fact is False
    assert receipt.dashboard_projection_generated is False
    assert receipt.publication_authorized is False
    assert receipt.live_service_wiring is False
    assert "meteorological_admission_certificate_authority_unavailable" in (
        receipt.reason_codes
    )
    assert receipt.exact_output_sha256 == receipt.stage_hashes.fingerprint
    assert len(dict(receipt.stage_hashes.items)) >= 20
    assert bundle.facts.audit()["ready"] == 0

    reopened_store = CertifiedShadowFactPipelineStore(bundle.pipeline_store.path)
    reopened = CertifiedShadowFactPipeline(
        desired_state_store=bundle.desired_store,
        canonical_evidence_store=bundle.canonical_store,
        science_process_runner=bundle.runner,
        fact_bridge=bundle.bridge,
        receipt_store=reopened_store,
    )
    reopened_result = reopened.process(
        bundle.request,
        recovery_binding=bundle.binding,
        raw_recovery_evidence=bundle.raw,
        authority=bundle.authority,
        deadline_at=time.monotonic() + 30.0,
    )
    assert reopened_result.duplicate is True
    assert reopened_result.receipt.receipt_sha256 == receipt.receipt_sha256
    assert bundle.science_store.stats()["receipt_count"] == 1
    assert reopened_store.audit() == {
        "algorithm_version": CERTIFIED_SHADOW_FACT_PIPELINE_ALGORITHM,
        "contract": "stv3-certified-shadow-fact-store-v1",
        "database_bytes": reopened_store.audit()["database_bytes"],
        "live_admission_receipts": 0,
        "ready_fact_receipts": 0,
        "receipt_count": 1,
        "synthetic_test_receipts": 0,
    }


def test_forged_structural_executor_is_inert_and_never_process_verified(tmp_path):
    bundle = _bundle(tmp_path, synthetic=False)
    forged = _Runner(bundle.process_result)
    pipeline = CertifiedShadowFactPipeline(
        desired_state_store=bundle.desired_store,
        canonical_evidence_store=bundle.canonical_store,
        science_process_runner=forged,
        fact_bridge=bundle.bridge,
        receipt_store=bundle.pipeline_store,
    )

    result = pipeline.process(
        bundle.request,
        recovery_binding=bundle.binding,
        raw_recovery_evidence=bundle.raw,
        authority=bundle.authority,
        deadline_at=time.monotonic() + 30.0,
    )

    assert result.receipt.status == "inert"
    assert result.receipt.reason_codes == (
        "real_science_process_executor_not_certified",
    )
    assert result.receipt.science_process_invoked is False
    assert result.receipt.science_process_verified is False
    assert result.receipt.created_ready_fact is False
    assert result.receipt.meteorological_admission_granted is False
    assert forged.calls == 0
    assert bundle.science_store is not None
    assert bundle.science_store.stats()["receipt_count"] == 0
    assert bundle.facts.audit()["ready"] == 0


def test_synthetic_authority_only_creates_explicit_test_result(tmp_path):
    bundle = _bundle(tmp_path, synthetic=True)

    result = _process(bundle)

    assert result.receipt.status == "synthetic_test_result"
    assert result.receipt.synthetic_test_only is True
    assert result.receipt.synthetic_test_fact_staged is True
    assert result.receipt.created_ready_fact is False
    assert result.receipt.meteorological_admission_granted is False
    assert result.receipt.operational_snapshot_claim_generated is False
    assert result.receipt.dashboard_projection_generated is False
    assert result.receipt.publication_authorized is False
    assert result.receipt.live_service_wiring is False
    assert bundle.facts.audit()["ready"] == 1
    assert bundle.facts.audit()["retained_rows"] == 1
    assert _process(bundle).duplicate is True
    assert isinstance(bundle.runner, _Runner)
    assert bundle.runner.calls == 1


def test_same_attempt_with_nonempty_lineage_is_not_silently_deduplicated(tmp_path):
    bundle = _bundle(tmp_path, synthetic=True)
    first = _process(bundle)
    assert first.duplicate is False
    assert isinstance(bundle.runner, _Runner)
    assert bundle.runner.calls == 1

    with pytest.raises(
        CertifiedShadowFactPipelineError,
        match="lineage_hypotheses must be empty",
    ):
        bundle.pipeline.process(
            bundle.request,
            recovery_binding=bundle.binding,
            raw_recovery_evidence=bundle.raw,
            authority=bundle.authority,
            lineage_hypotheses=(object(),),
            deadline_at=time.monotonic() + 30.0,
        )

    assert bundle.runner.calls == 1
    assert bundle.pipeline_store.audit()["receipt_count"] == 1
    assert bundle.facts.audit()["ready"] == 1


def test_cross_store_crash_retries_synthetic_stage_idempotently(tmp_path):
    def crash(point: str) -> None:
        if point == "record.before_commit":
            raise RuntimeError("injected pipeline receipt crash")

    bundle = _bundle(tmp_path, synthetic=True, pipeline_fault=crash)
    with pytest.raises(RuntimeError, match="injected pipeline receipt crash"):
        _process(bundle)
    assert bundle.facts.audit()["ready"] == 1

    store = CertifiedShadowFactPipelineStore(bundle.pipeline_store.path)
    retry_pipeline = CertifiedShadowFactPipeline(
        desired_state_store=bundle.desired_store,
        canonical_evidence_store=bundle.canonical_store,
        science_process_runner=bundle.runner,
        fact_bridge=bundle.bridge,
        receipt_store=store,
    )
    retried = retry_pipeline.process(
        bundle.request,
        recovery_binding=bundle.binding,
        raw_recovery_evidence=bundle.raw,
        authority=bundle.authority,
        deadline_at=time.monotonic() + 30.0,
    )
    assert retried.receipt.status == "synthetic_test_result"
    assert retried.receipt.synthetic_test_fact_staged is True
    assert bundle.facts.audit()["ready"] == 1
    assert store.audit()["receipt_count"] == 1


def test_incomplete_canonical_chain_is_durable_inert_and_never_calls_science(
    tmp_path,
):
    bundle = _bundle(tmp_path, synthetic=False)
    incomplete_run = replace(
        bundle.canonical.run,
        outcome="incomplete",
        reason_code="incomplete",
        non_operational_candidate_artifact=None,
    )
    incomplete = replace(bundle.canonical, run=incomplete_run)
    bundle.canonical_store.load = (
        lambda run_id: incomplete if run_id == incomplete.run_id else None
    )

    result = _process(bundle)

    assert result.receipt.status == "inert"
    assert result.receipt.reason_codes == ("canonical_decoder_output_incomplete",)
    assert result.receipt.verified_stages == ("raw_recovery",)
    assert result.receipt.stage_coverage_complete is False
    assert result.receipt.science_process_invoked is False
    assert bundle.science_store is not None
    assert bundle.science_store.stats()["receipt_count"] == 0
    assert bundle.facts.audit()["ready"] == 0
    assert _process(bundle).duplicate is True
    assert bundle.science_store.stats()["receipt_count"] == 0


def test_deadlines_and_process_crashes_are_receipted_without_fact_staging(tmp_path):
    expired = _bundle(
        tmp_path / "expired",
        synthetic=False,
        monotonic=lambda: 10.0,
    )
    before = _process(expired, deadline_at=9.0)
    assert before.receipt.status == "deadline_exceeded"
    assert before.receipt.reason_codes == (
        "pipeline_deadline_expired_before_science",
    )
    assert expired.science_store is not None
    assert expired.science_store.stats()["receipt_count"] == 0

    moments = iter((0.0, 2.0))
    after = _bundle(
        tmp_path / "after",
        synthetic=False,
        monotonic=lambda: next(moments),
    )
    after_result = _process(after, deadline_at=1.0)
    assert after_result.receipt.status == "deadline_exceeded"
    assert after_result.receipt.reason_codes == (
        "pipeline_deadline_expired_after_science",
    )
    assert after_result.receipt.science_process_verified is True
    assert "fact_staging_boundary" in after_result.receipt.missing_stages
    assert after.facts.audit()["ready"] == 0
    assert after.science_store is not None
    assert after.science_store.stats()["receipt_count"] == 1

    crashed = _bundle(tmp_path / "crash", synthetic=True)
    assert isinstance(crashed.runner, _Runner)
    crashed.runner.result = ScienceShadowProcessResult(
        status="crash",
        execution_outcome="child_crash",
        detail_code="science_child_failed",
        authority_sha256=crashed.request.process_authority_sha256,
        durable_disposition="not_committed",
        receipt_metadata=None,
        duplicate=None,
        isolation_enforced=True,
        child_exit_code=93,
        elapsed_seconds=0.5,
    )
    crash_result = _process(crashed)
    assert crash_result.receipt.status == "process_failed"
    assert crash_result.receipt.reason_codes == ("science_process_crash",)
    assert crash_result.receipt.science_process_invoked is True
    assert crash_result.receipt.science_process_verified is False
    assert crashed.facts.audit()["ready"] == 0


def test_receipts_are_private_bounded_and_store_capacity_is_fail_closed(tmp_path):
    bundle = _bundle(tmp_path / "base", synthetic=False)
    receipt = _process(bundle).receipt
    database = bundle.pipeline_store.path.read_bytes()
    assert b"Private Home Target" not in database
    assert b"51.04" not in database
    assert b"authorization" not in database.lower()
    assert len(json.dumps(receipt.to_mapping()).encode("ascii")) < 64 * 1024

    unsafe = replace(bundle.raw, capture_id="capture-api_key-leak")
    with pytest.raises(CertifiedShadowFactPipelineError, match="safe bounded"):
        bundle.pipeline.process(
            bundle.request,
            recovery_binding=bundle.binding,
            raw_recovery_evidence=unsafe,
            authority=bundle.authority,
            deadline_at=time.monotonic() + 30.0,
        )

    small = CertifiedShadowFactPipelineStore(
        (tmp_path / "small.sqlite3").resolve(),
        max_receipt_bytes=1024,
    )
    with pytest.raises(CertifiedShadowFactPipelineCapacityError, match="byte capacity"):
        small.record(receipt)

    one = CertifiedShadowFactPipelineStore(
        (tmp_path / "one.sqlite3").resolve(),
        max_receipts=1,
    )
    one.record(receipt)
    second = CertifiedShadowFactPipelineReceipt.create(
        request_sha256=_sha("second-request"),
        attempt_id="attempt-second-certified-pipeline",
        canonical_run_id=_run(99),
        desired_state_sha256=receipt.desired_state_sha256,
        authority_epoch=receipt.authority_epoch,
        mode="real_shadow",
        status="inert",
        reason_codes=("canonical_decoder_receipt_unavailable",),
        verified_stages=("raw_recovery",),
        stage_hashes={"raw_recovery_reference": _sha("second-raw")},
        science_process_invoked=False,
        science_process_verified=False,
        synthetic_test_fact_staged=False,
    )
    with pytest.raises(CertifiedShadowFactPipelineCapacityError, match="row capacity"):
        one.record(second)


def test_uncatchable_store_crash_is_atomically_empty_then_recoverable(tmp_path):
    bundle = _bundle(tmp_path / "source", synthetic=False)
    receipt = _process(bundle).receipt
    receipt_path = (tmp_path / "receipt.json").resolve()
    receipt_path.write_text(
        json.dumps(receipt.to_mapping(), separators=(",", ":"), sort_keys=True),
        encoding="ascii",
    )
    database_path = (tmp_path / "crash.sqlite3").resolve()
    completed = subprocess.run(
        [
            sys.executable,
            str(CRASH_HELPER),
            str(database_path),
            str(receipt_path),
            "record.before_commit",
        ],
        cwd=ROOT,
        env={
            **os.environ,
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONPATH": str(ROOT)
            + os.pathsep
            + os.environ.get("PYTHONPATH", ""),
        },
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 73, (completed.stdout, completed.stderr)

    reopened = CertifiedShadowFactPipelineStore(database_path)
    assert reopened.audit()["receipt_count"] == 0
    stored, duplicate = reopened.record(receipt)
    assert duplicate is False
    assert stored.receipt_sha256 == receipt.receipt_sha256
    assert reopened.audit()["receipt_count"] == 1


def test_product_module_has_no_dashboard_publication_or_service_wiring():
    source = MODULE.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        (node.module or "")
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    }
    assert not any(
        name.endswith(
            (
                ".service",
                ".operational_snapshot_runtime",
                ".operational_snapshot_publication",
            )
        )
        for name in imports
    )
    assert "mqtt" not in source.lower()
    assert "homeassistant" not in source.lower()
    assert "dashboard_projection_generated" in source
    assert "publication_authorized" in source
    assert "live_service_wiring" in source
    assert "meteorological_admission_granted" in source


def test_raw_recovery_reference_is_catalogued_and_content_addressed(tmp_path):
    bundle = _bundle(tmp_path, synthetic=False)
    first = CertifiedRawRecoveryReference.create(
        binding=bundle.binding,
        evidence=bundle.raw,
    )
    second = CertifiedRawRecoveryReference.from_mapping(first.to_mapping())
    assert first == second
    assert first.reference_sha256 == hashlib.sha256(
        json.dumps(
            first.payload_mapping(),
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    ).hexdigest()
