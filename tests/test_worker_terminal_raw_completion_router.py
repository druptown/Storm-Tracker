from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import pytest

from storm_tracker_worker import acquisition_scheduler_bridge
from storm_tracker_worker import autonomous_scheduler
from storm_tracker_worker import desired_state
from storm_tracker_worker import terminal_raw_completion_router as router_module
from storm_tracker_worker import worker_acquisition_plan
from storm_tracker_worker.acquisition_activation import (
    ACTIVATION_RULES,
    ActivationStatus,
)
from storm_tracker_worker.acquisition_capture_recovery import (
    RECOVERY_BINDINGS,
    recovery_evidence_digest,
)
from storm_tracker_worker.acquisition_evidence_lifecycle import (
    AcquisitionEvidenceLifecycleStore,
)
from storm_tracker_worker.acquisition_execution_store import (
    ExecutionReceiptStore,
)
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
from storm_tracker_worker.terminal_raw_completion_router import (
    DWD_CONSUMER_ID,
    DWD_STREAM_ID,
    DWD_TRANSFER_MODE,
    DWD_WORKLOAD_ID,
    REFERENCE_PAYLOAD_SCHEMA,
    REFERENCE_PROOF_SCHEMA,
    REFERENCE_TRANSFER_MODE,
    TERMINAL_RAW_CONSUMER_CATALOG,
    TerminalRawCompletionCapacityError,
    TerminalRawCompletionIntegrityError,
    TerminalRawCompletionRouter,
    validate_terminal_raw_consumer_catalog,
)


NOW = 1_800_000_000.0
SYNC = lambda _path: None
REPOSITORY = Path(__file__).resolve().parents[1]


class FaultOnce:
    def __init__(self, point: str) -> None:
        self.point = point
        self.hit = False

    def __call__(self, point: str) -> None:
        if point == self.point and not self.hit:
            self.hit = True
            raise RuntimeError(f"fault:{point}")


def _desired(workload_id: str | None = None) -> desired_state.DesiredState:
    in_united_kingdom = workload_id == "radar.metoffice.uk_rate"
    return desired_state.parse_desired_state({
        "authority_epoch": 1,
        "contract": desired_state.CONTRACT,
        "generation": 1,
        "issued_at": NOW - 60.0,
        "previous_state_sha256": None,
        "producer_id": "ha-terminal-raw-router-test",
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": True,
            "region_sharing_distance_km": 150.0,
        },
        "targets": [{
            "available": True,
            "country_code": "GB" if in_united_kingdom else "BE",
            "display_name": "London" if in_united_kingdom else "Home",
            "kind": "home",
            "latitude": 51.5 if in_united_kingdom else 51.04,
            "location_accuracy_m": None,
            "longitude": -0.1 if in_united_kingdom else 4.48,
            "observation_radius_km": 250.0,
            "position_observed_at": NOW - 60.0,
            "target_id": "zone.london" if in_united_kingdom else "zone.home",
        }],
    })


def _raw_limits() -> RawArtifactLimits:
    return RawArtifactLimits(
        max_object_bytes=1024 * 1024,
        max_total_bytes=32 * 1024 * 1024,
        max_object_entries=128,
        max_staging_entries=16,
        max_receipt_entries=128,
        max_capture_entries=64,
        max_capture_members=64,
        max_catalog_bytes=8 * 1024 * 1024,
    )


def _lifecycle(tmp_path: Path) -> AcquisitionEvidenceLifecycleStore:
    return AcquisitionEvidenceLifecycleStore(
        (tmp_path / "lifecycle").resolve(),
        clock=lambda: NOW + 2.0,
        monotonic=lambda: 1.0,
        token_factory=lambda: "a" * 64,
        directory_fsync=SYNC,
    )


def _environment(
    tmp_path: Path,
    workload_id: str,
    *,
    provenance_marker: str | None = None,
    cross_job_receipts: bool = False,
):
    state = _desired(workload_id)
    plan = worker_acquisition_plan.build_worker_acquisition_plan(state)
    jobs = worker_acquisition_plan.build_worker_acquisition_slot_jobs(
        plan, as_of=NOW
    )
    job = next(item for item in jobs if item.workload_id == workload_id)
    raw_receipt_job_id = (
        next(item.job_id for item in jobs if item.job_id != job.job_id)
        if cross_job_receipts
        else job.job_id
    )
    schedule = next(
        item
        for item in acquisition_scheduler_bridge.build_acquisition_schedule_intents(
            plan
        )
        if item.intent_id == job.intent_id
    )
    desired_store = desired_state.DesiredStateStore(
        (tmp_path / "desired.sqlite3").resolve()
    )
    desired_store.claim_producer(
        state.producer_id,
        "terminal-raw-router-claim",
        claimed_at=NOW - 100.0,
    )
    desired_store.apply(state.as_dict(), received_at=NOW - 59.0)
    scheduler = autonomous_scheduler.AutonomousScheduler(
        (tmp_path / "scheduler.sqlite3").resolve(),
        desired_state_store=desired_store,
        plan_builder=lambda _state: (schedule,),
        clock=lambda: NOW,
        lease_seconds=60.0,
    )
    scheduler.reconcile(now=NOW)
    claim = scheduler.claim_next("acquisition-owner", now=NOW)
    assert claim is not None

    rule = ACTIVATION_RULES[workload_id]
    binding = RECOVERY_BINDINGS[str(rule.handler_id)]
    raw = RawArtifactStore(
        (tmp_path / "source-raw").resolve(), limits=_raw_limits()
    )
    raw.initialize()
    receipt_ids: dict[str, str] = {}
    original_payloads: dict[str, bytes] = {}
    for index, member in enumerate(binding.members):
        payload = f"exact-{workload_id}-{member.role}-{index}".encode("ascii")
        provenance = {
            "capture_stage": NATIVE_CAPTURE_STAGE,
            "collector": "terminal_raw_router_test",
            "collector_version": "1",
            "representation": NATIVE_REPRESENTATION,
        }
        if provenance_marker is not None:
            provenance["opaque_marker"] = provenance_marker
        receipt = raw.store(
            provider=binding.raw_provider,
            product=member.raw_product,
            fetch_id=f"{raw_receipt_job_id}{member.receipt_suffix}",
            content_type=sorted(member.allowed_content_types)[0],
            received_at=NOW - 10.0 + index,
            upstream_id=(
                f"test-{member.role}|content_sha256="
                f"{hashlib.sha256(payload).hexdigest()}"
            ),
            provenance=provenance,
            payload=payload,
        )
        receipt_ids[member.role] = receipt.receipt_id
        original_payloads[member.role] = payload
    capture = raw.commit_capture(
        provider=binding.raw_provider,
        product=binding.capture_product,
        fetch_id=f"{job.job_id}{binding.capture_suffix}",
        upstream_capture_id=f"test-{workload_id}-complete",
        product_timestamp=NOW - 15.0,
        members=receipt_ids,
    )
    bundle = raw.load_capture_bundle(capture.capture_id)
    assert bundle is not None
    capture, receipts_by_role = bundle
    evidence = recovery_evidence_digest(
        binding=binding,
        receipts_by_role=receipts_by_role,
        capture=capture,
    )

    ledger = RawCapacityLedger(
        (tmp_path / "capacity.sqlite3").resolve(),
        profile=RawCapacityProfile(
            global_max_bytes=512 * 1024 * 1024,
            global_max_entries=512,
            providers=(ProviderCapacityLimit(
                binding.ledger_provider,
                512 * 1024 * 1024,
                512,
            ),),
            max_reservations=128,
            max_catalog_bytes=8 * 1024 * 1024,
            sqlite_busy_timeout_seconds=2.0,
        ),
        clock=lambda: NOW,
    )
    reservation_id = "reservation-" + hashlib.sha256(
        workload_id.encode("ascii")
    ).hexdigest()
    ledger.reserve(
        reservation_id=reservation_id,
        provider=binding.ledger_provider,
        job_identity=job.job_id,
        requested_bytes=binding.maximum_transport_bytes,
        requested_entries=binding.maximum_artifacts,
    )
    capacity = ledger.commit(
        reservation_id=reservation_id,
        actual_bytes=evidence.actual_bytes,
        actual_entries=evidence.actual_entries,
        content_sha256=evidence.content_sha256,
        receipt_sha256=evidence.receipt_sha256,
        capture_sha256=evidence.capture_sha256,
    )
    executions = ExecutionReceiptStore(
        (tmp_path / "execution.sqlite3").resolve(), clock=lambda: NOW
    )
    terminal = executions.record(
        scheduler_job_id=claim.job_id,
        acquisition_job_id=job.job_id,
        binding_sha256=hashlib.sha256(
            f"execution:{workload_id}".encode("ascii")
        ).hexdigest(),
        attempt_count=claim.attempt_count,
        outcome="completed_shadow_raw",
        reason_code="shadow_raw_capture_complete",
        terminal=True,
        content_sha256=evidence.content_sha256,
        raw_receipt_sha256=evidence.receipt_sha256,
        capture_sha256=evidence.capture_sha256,
        capacity_commit_sha256=capacity.commit_sha256,
    )
    scheduler.complete(claim.job_id, claim.lease_token, now=NOW + 1.0)
    handoff = RawArtifactStore(
        (tmp_path / "dwd-handoff").resolve(), limits=_raw_limits()
    )
    handoff.initialize()
    lifecycle = _lifecycle(tmp_path)
    return {
        "binding": binding,
        "capacity": ledger,
        "capture": capture,
        "desired": desired_store,
        "executions": executions,
        "handoff": handoff,
        "job": job,
        "lifecycle": lifecycle,
        "payloads": original_payloads,
        "raw": raw,
        "receipts": receipts_by_role,
        "scheduler": scheduler,
        "state": state,
        "terminal": terminal,
    }


def _router(env, **changes) -> TerminalRawCompletionRouter:
    values = {
        "lifecycle_store": env["lifecycle"],
        "desired_state_store": env["desired"],
        "scheduler": env["scheduler"],
        "execution_store": env["executions"],
        "capacity_ledger": env["capacity"],
        "source_raw_store": env["raw"],
        "dwd_handoff_raw_store": env["handoff"],
    }
    values.update(changes)
    return TerminalRawCompletionRouter(**values)


def _claim_reference(env, workload_id: str):
    consumer = TERMINAL_RAW_CONSUMER_CATALOG[workload_id]
    lease = env["lifecycle"].claim_next(
        consumer.consumer_id,
        consumer.stream_id,
        "reference-auditor",
        now=NOW + 3.0,
    )
    assert lease is not None
    return lease, json.loads(lease.payload)


COMPLETION_CORE_CRASH_CHILD = (
    Path(__file__).resolve().parent
    / "helpers"
    / "completion_core_os_exit73_child.py"
)
_TERMINAL_ROUTER_CRASH_POINTS = (
    "completion.after_authorities_verified",
    "completion.before_lifecycle_publish",
    "completion.after_lifecycle_publish",
)


def _reopen_environment(tmp_path: Path, workload_id: str):
    """Open every durable router authority without replaying setup writes."""

    state = _desired(workload_id)
    plan = worker_acquisition_plan.build_worker_acquisition_plan(state)
    jobs = worker_acquisition_plan.build_worker_acquisition_slot_jobs(
        plan, as_of=NOW
    )
    job = next(item for item in jobs if item.workload_id == workload_id)
    schedule = next(
        item
        for item in acquisition_scheduler_bridge.build_acquisition_schedule_intents(
            plan
        )
        if item.intent_id == job.intent_id
    )
    desired_store = desired_state.DesiredStateStore(
        (tmp_path / "desired.sqlite3").resolve()
    )
    scheduler = autonomous_scheduler.AutonomousScheduler(
        (tmp_path / "scheduler.sqlite3").resolve(),
        desired_state_store=desired_store,
        plan_builder=lambda _state: (schedule,),
        clock=lambda: NOW,
        lease_seconds=60.0,
    )
    binding = RECOVERY_BINDINGS[str(ACTIVATION_RULES[workload_id].handler_id)]
    raw = RawArtifactStore(
        (tmp_path / "source-raw").resolve(), limits=_raw_limits()
    )
    raw.initialize()
    capacity = RawCapacityLedger(
        (tmp_path / "capacity.sqlite3").resolve(),
        profile=RawCapacityProfile(
            global_max_bytes=512 * 1024 * 1024,
            global_max_entries=512,
            providers=(ProviderCapacityLimit(
                binding.ledger_provider,
                512 * 1024 * 1024,
                512,
            ),),
            max_reservations=128,
            max_catalog_bytes=8 * 1024 * 1024,
            sqlite_busy_timeout_seconds=2.0,
        ),
        clock=lambda: NOW,
    )
    executions = ExecutionReceiptStore(
        (tmp_path / "execution.sqlite3").resolve(), clock=lambda: NOW
    )
    handoff = RawArtifactStore(
        (tmp_path / "dwd-handoff").resolve(), limits=_raw_limits()
    )
    handoff.initialize()
    lifecycle = _lifecycle(tmp_path)
    return {
        "binding": binding,
        "capacity": capacity,
        "desired": desired_store,
        "executions": executions,
        "handoff": handoff,
        "job": job,
        "lifecycle": lifecycle,
        "raw": raw,
        "scheduler": scheduler,
        "state": state,
    }


def run_terminal_router_crash_child(
    root: Path,
    checkpoint: str,
    crash,
) -> None:
    assert checkpoint in _TERMINAL_ROUTER_CRASH_POINTS
    env = _environment(root, "radar.knmi.rtcor")
    _router(env, fault_injector=crash).publish_next()


def _run_terminal_router_crash(
    root: Path,
    checkpoint: str,
) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        [
            sys.executable,
            str(COMPLETION_CORE_CRASH_CHILD),
            "terminal-router",
            str(root),
            checkpoint,
        ],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        check=False,
        timeout=180.0,
    )


def test_consumer_catalog_is_exact_closed_and_preserves_deployed_dwd_semantics():
    validate_terminal_raw_consumer_catalog(TERMINAL_RAW_CONSUMER_CATALOG)
    active = {
        workload_id
        for workload_id, rule in ACTIVATION_RULES.items()
        if rule.status is ActivationStatus.ACTIVE_SHADOW_RAW
    }
    assert set(TERMINAL_RAW_CONSUMER_CATALOG) == active
    assert set(TERMINAL_RAW_CONSUMER_CATALOG) == {
        binding.workload_id for binding in RECOVERY_BINDINGS.values()
    }
    dwd = TERMINAL_RAW_CONSUMER_CATALOG[DWD_WORKLOAD_ID]
    assert dwd.consumer_id == DWD_CONSUMER_ID
    assert dwd.stream_id == DWD_STREAM_ID
    assert dwd.transfer_mode == DWD_TRANSFER_MODE
    assert all(
        binding.transfer_mode == REFERENCE_TRANSFER_MODE
        for workload_id, binding in TERMINAL_RAW_CONSUMER_CATALOG.items()
        if workload_id != DWD_WORKLOAD_ID
    )


def test_raw_capture_service_only_routes_have_inert_manifest_consumers():
    raw_only_workloads = {
        "ground.kmi.aws_10min",
        "ground.netatmo.region",
        "ground.synoptic.region",
        "model.openmeteo.italiameteo_icon2i.region",
        "model.openmeteo.region",
        "radar.kmi.appv4_visual",
        "radar.kmi.public_wcs_unknown_quantity",
        "radar.opera.dbzh",
        "radar.rainviewer.visual",
        "validation.geosphere.location",
        "validation.italiameteo.dpc_sri_historical_catalog",
        "validation.meteolux.location",
    }
    assert raw_only_workloads <= set(TERMINAL_RAW_CONSUMER_CATALOG)
    for workload_id in raw_only_workloads:
        binding = TERMINAL_RAW_CONSUMER_CATALOG[workload_id]
        assert binding.consumer_kind == "raw_manifest_audit"
        assert binding.implementation_id == "terminal_raw_reference_audit_v1"
        assert binding.transfer_mode == REFERENCE_TRANSFER_MODE
        assert binding.as_dict()["meteorological_admission"] is False
        assert binding.as_dict()["runtime_wiring"] == "inert"


@pytest.mark.parametrize(
    "workload_id",
    (
        "radar.knmi.rtcor",
        "satellite.noaa.err",
        "radar.metoffice.uk_rate",
    ),
)
def test_knmi_noaa_and_metoffice_publish_bounded_exact_reference_only(
    tmp_path,
    workload_id,
):
    env = _environment(tmp_path, workload_id)
    result = _router(env).publish_next()
    assert result.status == "published"
    assert result.workload_id == workload_id
    lease, document = _claim_reference(env, workload_id)

    assert document["schema"] == REFERENCE_PAYLOAD_SCHEMA
    assert lease.proof["schema"] == REFERENCE_PROOF_SCHEMA
    assert hashlib.sha256(lease.payload).hexdigest() == lease.proof[
        "reference_content_sha256"
    ]
    assert document["claims"] == {
        "decoded": False,
        "fact_staged": False,
        "meteorological_admission": False,
        "operational_snapshot_published": False,
        "raw_bytes_copied": False,
        "raw_refetch_performed": False,
    }
    role_references = document["capture_reference"]["members"]
    assert {item["role"] for item in role_references} == set(
        env["binding"].required_roles
    )
    assert len(role_references) == env["binding"].maximum_artifacts
    assert document["capture_reference"]["capture_receipt_sha256"] == (
        env["capture"].capture_receipt_sha256
    )
    assert all(
        payload not in lease.payload for payload in env["payloads"].values()
    )
    assert len(lease.payload) < 64 * 1024


def test_three_role_metoffice_reference_is_complete_and_role_sorted(tmp_path):
    env = _environment(tmp_path, "radar.metoffice.uk_rate")
    _router(env).publish_next()
    _lease, document = _claim_reference(env, "radar.metoffice.uk_rate")
    roles = [
        item["role"]
        for item in document["capture_reference"]["members"]
    ]
    assert roles == sorted(env["binding"].required_roles)
    assert roles == ["listing_current", "listing_previous", "odim_hdf5"]


def test_deployed_dwd_route_uses_legacy_raw_payload_and_exact_consumer(
    tmp_path,
    monkeypatch,
):
    env = _environment(tmp_path, DWD_WORKLOAD_ID)
    expected_payload = b"existing-dwd-consumer-exact-raw-tar"
    calls = []

    def compatible_legacy_publisher(**kwargs):
        calls.append(kwargs)
        return kwargs["lifecycle_store"].publish(
            acquisition_job_id=kwargs["acquisition_job_id"],
            consumer_id=kwargs["consumer_id"],
            stream_id=DWD_STREAM_ID,
            desired_state_sha256=kwargs["desired_state"].state_sha256,
            authority_sha256="f" * 64,
            proof={
                "meteorological_claim": False,
                "raw_refetch_performed": False,
                "schema": "stv3-worker-dwd-rv-completion-proof-v1",
            },
            payload=expected_payload,
        )

    monkeypatch.setattr(
        router_module,
        "publish_dwd_completion",
        compatible_legacy_publisher,
    )
    result = _router(env).publish_next()
    assert result.reason_code == "exact_legacy_dwd_completion_published"
    assert len(calls) == 1
    assert calls[0]["consumer_id"] == DWD_CONSUMER_ID
    assert calls[0]["source_capture_id"] == env["capture"].capture_id
    assert calls[0]["source_raw_store"] is env["raw"]
    assert calls[0]["handoff_raw_store"] is env["handoff"]
    lease = env["lifecycle"].claim_next(
        DWD_CONSUMER_ID,
        DWD_STREAM_ID,
        "existing-dwd-decoder",
        now=NOW + 3.0,
    )
    assert lease is not None
    assert lease.payload == expected_payload
    assert lease.proof["schema"] == "stv3-worker-dwd-rv-completion-proof-v1"


def test_crash_after_lifecycle_commit_restarts_without_duplicate_or_refetch(
    tmp_path,
):
    env = _environment(tmp_path, "radar.knmi.rtcor")
    fault = FaultOnce("completion.after_lifecycle_publish")
    with pytest.raises(
        TerminalRawCompletionIntegrityError,
        match="failed closed",
    ):
        _router(env, fault_injector=fault).publish_next()
    assert env["lifecycle"].contains_acquisition_job(env["job"].job_id)

    restarted = _router(env)
    result = restarted.publish_next()
    assert result.status == "idle"
    assert env["lifecycle"].diagnostics()["hot_completions"] == 1
    lease, document = _claim_reference(env, "radar.knmi.rtcor")
    assert document["acquisition_job_id"] == env["job"].job_id
    assert lease.attempt_count == 1


@pytest.mark.parametrize(
    "checkpoint",
    _TERMINAL_ROUTER_CRASH_POINTS,
    ids=(
        "after-authorities-verified",
        "before-lifecycle-publish",
        "after-lifecycle-publish",
    ),
)
def test_native_os_exit73_router_reopens_replays_without_growth(
    tmp_path: Path,
    checkpoint: str,
) -> None:
    root = (tmp_path / checkpoint.replace(".", "-")).resolve()
    root.mkdir()

    crashed = _run_terminal_router_crash(root, checkpoint)
    assert crashed.returncode == 73
    assert crashed.stdout == b""
    assert crashed.stderr == b""

    env = _reopen_environment(root, "radar.knmi.rtcor")
    job = env["job"]
    binding = env["binding"]
    capture_id = RawArtifactStore._capture_id(  # noqa: SLF001
        provider=binding.raw_provider,
        product=binding.capture_product,
        fetch_id=f"{job.job_id}{binding.capture_suffix}",
    )
    bundle = env["raw"].load_capture_bundle(capture_id)
    assert bundle is not None
    capture, receipts = bundle
    assert tuple(sorted(receipts)) == tuple(sorted(binding.required_roles))
    source_witness = (
        env["raw"].stats(),
        capture,
        tuple(sorted(receipts.items())),
        env["executions"].load_job_receipts(job.job_id),
        env["capacity"].load_for_job(
            provider=binding.ledger_provider,
            job_identity=job.job_id,
        ),
    )
    assert source_witness[0]["capture_count"] == 1
    assert source_witness[0]["receipt_count"] == len(binding.required_roles)
    assert env["raw"].integrity_check()["healthy"] is True
    assert env["executions"].stats()["receipt_count"] == 1
    assert env["executions"].stats()["terminal_count"] == 1
    assert env["capacity"].audit()["committed_count"] == 1

    crashed_lookup = env["lifecycle"].load_acquisition_completion(job.job_id)
    expected_present = checkpoint == "completion.after_lifecycle_publish"
    assert (crashed_lookup is not None) is expected_present
    crashed_lifecycle = env["lifecycle"].diagnostics()
    assert crashed_lifecycle["hot_completions"] == (1 if expected_present else 0)

    recovered = _router(env).publish_next()
    assert recovered.status == ("idle" if expected_present else "published")
    durable = env["lifecycle"].load_acquisition_completion(job.job_id)
    assert durable is not None
    assert durable.producer_receipt.acquisition_job_id == job.job_id
    assert durable.producer_receipt.content_sha256 == hashlib.sha256(
        durable.payload
    ).hexdigest()
    after_recovery = env["lifecycle"].diagnostics()
    assert after_recovery["hot_completions"] == 1

    serialized = json.dumps(
        {"payload": json.loads(durable.payload), "proof": durable.proof},
        sort_keys=True,
    ).casefold()
    assert str(root).casefold() not in serialized
    for forbidden in (
        "access_token",
        "api_key",
        "authorization",
        "lease_token",
        "password",
        "refresh_token",
        "http://",
        "https://",
    ):
        assert forbidden not in serialized

    replay_env = _reopen_environment(root, "radar.knmi.rtcor")
    replayed = _router(replay_env).publish_next()
    assert replayed.status == "idle"
    replay_lookup = replay_env["lifecycle"].load_acquisition_completion(job.job_id)
    assert replay_lookup == durable
    assert replay_env["lifecycle"].diagnostics()["hot_completions"] == 1
    replay_bundle = replay_env["raw"].load_capture_bundle(capture_id)
    assert replay_bundle is not None
    assert (
        replay_env["raw"].stats(),
        replay_bundle[0],
        tuple(sorted(replay_bundle[1].items())),
        replay_env["executions"].load_job_receipts(job.job_id),
        replay_env["capacity"].load_for_job(
            provider=binding.ledger_provider,
            job_identity=job.job_id,
        ),
    ) == source_witness
    assert replay_env["raw"].integrity_check()["healthy"] is True
    assert replay_env["capacity"].audit()["committed_count"] == 1


def test_forged_same_job_completion_cannot_suppress_closed_router_event(
    tmp_path,
):
    env = _environment(tmp_path, "radar.knmi.rtcor")
    expected = TERMINAL_RAW_CONSUMER_CATALOG["radar.knmi.rtcor"]
    env["lifecycle"].publish(
        acquisition_job_id=env["job"].job_id,
        consumer_id=expected.consumer_id,
        stream_id=expected.stream_id,
        desired_state_sha256=env["state"].state_sha256,
        authority_sha256="f" * 64,
        proof={"schema": "forged-same-job-completion-v1"},
        payload=b"forged-reference",
    )

    candidate = _router(env)
    with pytest.raises(
        TerminalRawCompletionIntegrityError,
        match="not the exact expected reference",
    ):
        candidate.publish_next()
    diagnostics = candidate.diagnostics()
    assert diagnostics["cursor"] == 0
    assert diagnostics["counts"]["already_published"] == 0
    lookup = env["lifecycle"].load_acquisition_completion(env["job"].job_id)
    assert lookup is not None and lookup.payload == b"forged-reference"


def test_raw_object_tamper_fails_before_any_completion(tmp_path):
    env = _environment(tmp_path, "satellite.noaa.err")
    receipt = next(iter(env["receipts"].values()))
    Path(receipt.artifact_path).write_bytes(b"tampered")

    with pytest.raises(
        TerminalRawCompletionIntegrityError,
        match="authority verification",
    ):
        _router(env).publish_next()
    assert not env["lifecycle"].contains_acquisition_job(env["job"].job_id)


def test_capture_cannot_mix_role_receipts_from_another_acquisition_job(
    tmp_path,
):
    env = _environment(
        tmp_path,
        "radar.knmi.rtcor",
        cross_job_receipts=True,
    )
    with pytest.raises(
        TerminalRawCompletionIntegrityError,
        match="changed acquisition job identity",
    ):
        _router(env).publish_next()
    assert not env["lifecycle"].contains_acquisition_job(env["job"].job_id)


def test_reference_cap_fails_closed_without_partial_lifecycle_row(tmp_path):
    env = _environment(tmp_path, "radar.metoffice.uk_rate")
    with pytest.raises(
        TerminalRawCompletionCapacityError,
        match="byte cap",
    ):
        _router(env, max_reference_payload_bytes=256).publish_next()
    assert not env["lifecycle"].contains_acquisition_job(env["job"].job_id)
    assert env["lifecycle"].diagnostics()["hot_completions"] == 0


def test_reference_and_proof_exclude_paths_capabilities_and_raw_provenance(
    tmp_path,
):
    private_marker = "private_deadbeefcafebabefeedface"
    env = _environment(
        tmp_path,
        "radar.knmi.rtcor",
        provenance_marker=private_marker,
    )
    _router(env).publish_next()
    lease, _document = _claim_reference(env, "radar.knmi.rtcor")
    serialized = json.dumps(
        {"payload": json.loads(lease.payload), "proof": lease.proof},
        sort_keys=True,
    ).casefold()
    assert private_marker not in serialized
    for forbidden in (
        "artifact_path",
        "authorization",
        "access_token",
        "refresh_token",
        "api_key",
        "password",
        "provenance",
        "upstream_id",
        "fetch_id",
        "http://",
        "https://",
    ):
        assert forbidden not in serialized
    assert str(tmp_path).casefold() not in serialized


def test_two_router_instances_converge_on_one_content_addressed_completion(
    tmp_path,
):
    env = _environment(tmp_path, "radar.knmi.rtcor")
    routers = (_router(env), _router(env))
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = tuple(pool.map(lambda item: item.publish_next(), routers))

    assert {item.status for item in results} <= {"idle", "published"}
    assert any(item.status == "published" for item in results)
    assert env["lifecycle"].diagnostics()["hot_completions"] == 1
    lease, document = _claim_reference(env, "radar.knmi.rtcor")
    assert lease.content_sha256 == hashlib.sha256(lease.payload).hexdigest()
    assert document["capture_reference"]["manifest_sha256"] == (
        env["capture"].manifest_sha256
    )


def test_missing_consumer_binding_is_rejected_before_router_can_scan(tmp_path):
    env = _environment(tmp_path, "radar.knmi.rtcor")
    incomplete = dict(TERMINAL_RAW_CONSUMER_CATALOG)
    del incomplete["radar.knmi.rtcor"]
    with pytest.raises(
        TerminalRawCompletionIntegrityError,
        match="differs from ACTIVE_SHADOW_RAW",
    ):
        _router(env, consumer_catalog=incomplete)


def test_router_is_inert_from_live_service_ha_mqtt_and_fact_publication():
    module_name = "terminal_raw_completion_router"
    for relative in (
        "storm_tracker_worker/service.py",
        "storm_tracker_worker/run.py",
        "storm_tracker_worker/operational_snapshot_runtime.py",
        "storm_tracker_worker/science_shadow_orchestrator.py",
        "custom_components/storm_tracker_v3/__init__.py",
    ):
        path = REPOSITORY / relative
        if path.exists():
            assert module_name not in path.read_text(encoding="utf-8")
    source = (REPOSITORY / "storm_tracker_worker" / f"{module_name}.py").read_text(
        encoding="utf-8"
    )
    assert "import paho" not in source
    assert "urllib" not in source
    assert "requests" not in source
    assert "mqtt" not in source.casefold()
    assert "HomeAssistant" not in source
    assert "operational_fact_store" not in source
