from __future__ import annotations

import ast
import asyncio
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import threading
import time

import pytest

from storm_tracker_worker.acquisition_evidence_lifecycle import (
    AcquisitionEvidenceLifecycleStore,
    CompletionLease,
    ConsumptionReceipt,
    desired_authority_proof,
)
from storm_tracker_worker.desired_state import DesiredState, parse_desired_state
from storm_tracker_worker.operational_fact_store import (
    OperationalFactRevisionStore,
)
from storm_tracker_worker.operational_snapshot_publication import (
    PublicationOutboxStore,
)
from storm_tracker_worker.operational_snapshot_runtime import (
    OperationalSnapshotRuntime,
)
from storm_tracker_worker.operational_snapshot_store import (
    OperationalSnapshotStore,
)
from storm_tracker_worker.tracking_core.operational_snapshot import (
    decode_control_envelope,
)
from storm_tracker_worker.worker_first_runtime import (
    FactPublicationBinding,
    FactPublicationReceipt,
    MAX_DIAGNOSTIC_BYTES,
    PipelineOutcome,
    VerifiedPipelineOutcome,
    WorkerFirstRuntimeCoordinator,
    WorkerFirstRuntimeOwnershipError,
)
from storm_tracker_worker.worker_first_ownership import (
    REQUIRED_STORE_ROLES,
    WorkerFirstFenceLost,
    WorkerFirstOwnershipFence,
    WorkerFirstStoreAuthority,
    build_worker_first_authority_bytes,
    initialize_worker_first_lock_file,
)


NOW = 1_800_300_000.0
MODULE = (
    Path(__file__).resolve().parents[1]
    / "storm_tracker_worker"
    / "worker_first_runtime.py"
)
CONSUMER = "dwd-rv-quantitative-decoder-v1"
STREAM = "radar.dwd.radolan_rv"


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode("ascii")).hexdigest()


_FAKE_STORE_BINDINGS = {
    role: (
        str((MODULE.parent / f".authority-{role}").resolve()),
        _sha(f"identity:{role}"),
    )
    for role in REQUIRED_STORE_ROLES
}


class _FakeOwnershipLease:
    def __init__(
        self,
        stores=None,
        *,
        consumer_id: str = CONSUMER,
        stream_id: str = STREAM,
        log: list[str] | None = None,
    ) -> None:
        self.authority_sha256 = _sha("fake-worker-first-authority")
        self.authority_scope_sha256 = _sha("fake-worker-first-scope")
        self.consumer_id = consumer_id
        self.stream_id = stream_id
        self.stores = dict(stores or _FAKE_STORE_BINDINGS)
        self.log = log
        self.held = True
        self.assert_calls = 0
        self.release_calls = 0
        self.binding_calls = 0

    def assert_held(self) -> None:
        self.assert_calls += 1
        if self.log is not None:
            self.log.append("ownership.assert")
        if not self.held:
            raise RuntimeError("private lock path must not escape")

    def assert_coordinator_binding(
        self,
        *,
        consumer_id: str,
        stream_id: str,
        stores,
    ) -> None:
        self.binding_calls += 1
        self.assert_held()
        if (
            consumer_id != self.consumer_id
            or stream_id != self.stream_id
            or dict(stores) != self.stores
        ):
            raise RuntimeError("private authority mismatch")

    def release(self) -> None:
        self.assert_held()
        self.release_calls += 1
        self.held = False
        if self.log is not None:
            self.log.append("ownership.release")


def _real_ownership(tmp_path):
    lock_path = initialize_worker_first_lock_file(
        (tmp_path / "worker-first.lock").resolve()
    )
    stores = []
    bindings = {}
    for role, kind in REQUIRED_STORE_ROLES.items():
        path = (tmp_path / f"authority-{role}").resolve()
        if kind == "directory":
            path.mkdir()
        else:
            path.touch()
        identity = _sha(f"real-identity:{role}")
        stores.append(WorkerFirstStoreAuthority.create(
            role=role,
            path=path,
            identity_sha256=identity,
        ))
        bindings[role] = (str(path), identity)
    body = build_worker_first_authority_bytes(
        consumer_id=CONSUMER,
        stream_id=STREAM,
        lock_path=lock_path,
        stores=stores,
    )
    authority_path = (tmp_path / "worker-first-authority.json").resolve()
    authority_path.write_bytes(body)
    authority_sha = hashlib.sha256(body).hexdigest()
    fence = WorkerFirstOwnershipFence(
        authority_path,
        expected_authority_sha256=authority_sha,
    )
    return fence, fence.acquire(), bindings, authority_path, authority_sha


def _desired(*, generation: int = 1, previous: str | None = None) -> DesiredState:
    return parse_desired_state({
        "contract": "stv3-worker-desired-state-v1",
        "producer_id": "ha:worker-first-test",
        "authority_epoch": 1,
        "generation": generation,
        "issued_at": NOW + generation,
        "previous_state_sha256": previous,
        "targets": [{
            "target_id": "home",
            "display_name": "Thuis",
            "kind": "home",
            "available": True,
            "latitude": 50.99 + generation / 1000,
            "longitude": 4.47,
            "position_observed_at": NOW,
            "location_accuracy_m": 10.0,
            "observation_radius_km": 250.0,
            "country_code": "BE",
        }],
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": True,
            "region_sharing_distance_km": 100.0,
        },
    })


class _DesiredStore:
    def __init__(
        self,
        active: DesiredState | None,
        *,
        log: list[str] | None = None,
    ) -> None:
        self.active = active
        self.log = log

    def load_active(self) -> DesiredState | None:
        if self.log is not None:
            self.log.append("desired.load")
        return self.active


def _lease(
    desired: DesiredState,
    *,
    event: str = "event-1",
    token: str = "lease-1",
    attempt: int = 1,
    desired_sha: str | None = None,
    authority_sha: str | None = None,
) -> CompletionLease:
    return CompletionLease(
        event_id=event,
        event_sha256=_sha(f"{event}:event"),
        producer_receipt_sha256=_sha(f"{event}:producer"),
        acquisition_job_id=f"job:{event}",
        consumer_id=CONSUMER,
        stream_id=STREAM,
        stream_sequence=1,
        desired_state_sha256=desired_sha or desired.state_sha256,
        authority_sha256=(
            authority_sha
            or desired_authority_proof(desired)["authority_sha256"]
        ),
        content_sha256=_sha(f"{event}:content"),
        size_bytes=3,
        attempt_count=attempt,
        lease_token=token,
        lease_expires_at=NOW + 600,
        proof={"schema": "test-proof-v1"},
        payload=b"raw",
    )


class _Completion:
    def __init__(
        self,
        leases: list[CompletionLease] | None = None,
        *,
        log: list[str] | None = None,
        archive_healthy: bool = True,
    ) -> None:
        self.leases = list(leases or [])
        self.log = log if log is not None else []
        self.archive_healthy = archive_healthy
        self.acknowledged: list[tuple[str, str]] = []
        self.rejected: list[tuple[str, str, str | None]] = []
        self.claim_calls = 0

    def resume_pending_prunes(self) -> int:
        self.log.append("completion.resume")
        return 0

    def audit_archive(self) -> dict:
        self.log.append("completion.audit")
        return {"healthy": self.archive_healthy}

    def diagnostics(self) -> dict:
        return {"ready": len(self.leases), "secret": "must-not-escape"}

    def claim_next(
        self,
        consumer_id: str,
        stream_id: str,
        owner_id: str,
        *,
        lease_seconds: float,
        now: float | None = None,
    ) -> CompletionLease | None:
        assert consumer_id == CONSUMER
        assert stream_id == STREAM
        assert owner_id == "worker-first-runtime"
        assert lease_seconds <= 600
        assert now == NOW
        self.claim_calls += 1
        self.log.append("completion.claim")
        return self.leases.pop(0) if self.leases else None

    def acknowledge(
        self,
        event_id: str,
        lease_token: str,
        *,
        output_sha256: str,
        now: float | None = None,
    ) -> ConsumptionReceipt:
        self.log.append("completion.ack")
        self.acknowledged.append((event_id, output_sha256))
        return ConsumptionReceipt(
            receipt_sha256=_sha(f"{event_id}:{output_sha256}"),
            event_id=event_id,
            event_sha256=_sha(f"{event_id}:event"),
            producer_receipt_sha256=_sha(f"{event_id}:producer"),
            consumer_id=CONSUMER,
            stream_id=STREAM,
            stream_sequence=1,
            output_sha256=output_sha256,
            created_at=float(now if now is not None else NOW),
        )

    def reject_leased(
        self,
        event_id: str,
        lease_token: str,
        *,
        expected_event_sha256: str,
        reason_code: str,
        active_desired_state_sha256: str | None,
        now: float | None = None,
    ) -> ConsumptionReceipt:
        self.log.append("completion.reject")
        self.rejected.append(
            (event_id, reason_code, active_desired_state_sha256)
        )
        output_sha = _sha(
            f"{event_id}:{reason_code}:{active_desired_state_sha256}"
        )
        return ConsumptionReceipt(
            receipt_sha256=_sha(f"{event_id}:{output_sha}:rejection"),
            event_id=event_id,
            event_sha256=expected_event_sha256,
            producer_receipt_sha256=_sha(f"{event_id}:producer"),
            consumer_id=CONSUMER,
            stream_id=STREAM,
            stream_sequence=1,
            output_sha256=output_sha,
            created_at=float(now if now is not None else NOW),
        )


class _ForgedAckCompletion(_Completion):
    def acknowledge(self, *args, **kwargs) -> ConsumptionReceipt:
        receipt = super().acknowledge(*args, **kwargs)
        return replace(
            receipt,
            event_id="event-forged",
            output_sha256=_sha("forged-output"),
        )


class _Facts:
    def __init__(
        self,
        *,
        log: list[str] | None = None,
        healthy: bool = True,
    ) -> None:
        self.log = log if log is not None else []
        self.healthy = healthy
        self.superseded: list[str] = []
        self.supersede_all_reasons: list[str] = []

    def audit(self) -> dict:
        self.log.append("facts.audit")
        return {"status": "ok" if self.healthy else "corrupt"}

    def diagnostics(self) -> dict:
        return {"status": "ok", "payload": "must-not-escape"}

    def supersede_obsolete(self, active_desired_state: DesiredState) -> int:
        self.log.append("facts.supersede")
        self.superseded.append(active_desired_state.state_sha256)
        return 0

    def supersede_all_ready(
        self,
        *,
        reason: str = "desired_state_unavailable",
    ) -> int:
        self.log.append("facts.supersede_all")
        assert reason == "desired_state_unavailable"
        self.supersede_all_reasons.append(reason)
        return 0


def _snapshot_status(*, healthy: bool = True) -> dict:
    return {
        "state": "running" if healthy else "degraded",
        "last_error_code": None if healthy else "transport_failed",
        "publication": {
            "transport_state": "published" if healthy else "pending",
            "dirty": not healthy,
        },
    }


class _Snapshots:
    def __init__(
        self,
        *,
        log: list[str] | None = None,
        start_healthy: bool = True,
        tick_healthy: bool = True,
        fact_healthy: bool = True,
        unconfigured: bool = False,
    ) -> None:
        self.log = log if log is not None else []
        self.start_healthy = start_healthy
        self.tick_healthy = tick_healthy
        self.fact_healthy = fact_healthy
        self.unconfigured = unconfigured
        self.start_calls = 0
        self.stop_calls = 0
        self.fact_calls = 0

    async def async_start(self) -> dict:
        self.log.append("snapshots.start")
        self.start_calls += 1
        if self.unconfigured and self.start_healthy:
            return {
                "state": "unconfigured",
                "last_error_code": None,
                "publication": {"transport_state": None, "dirty": False},
            }
        return _snapshot_status(healthy=self.start_healthy)

    async def async_tick(self) -> dict:
        self.log.append("snapshots.tick")
        if self.unconfigured and self.tick_healthy:
            return {
                "state": "unconfigured",
                "last_error_code": None,
                "publication": {"transport_state": None, "dirty": False},
            }
        return _snapshot_status(healthy=self.tick_healthy)

    async def async_publish_ready_fact(self) -> dict:
        self.log.append("snapshots.fact")
        self.fact_calls += 1
        return _snapshot_status(healthy=self.fact_healthy)

    async def async_publish_exact_fact(
        self,
        binding: FactPublicationBinding,
    ) -> FactPublicationReceipt:
        self.log.append("snapshots.fact")
        self.fact_calls += 1
        if not self.fact_healthy:
            raise RuntimeError("exact fact transport failed")
        return FactPublicationReceipt(
            pipeline_item_id=binding.pipeline_item_id,
            revision_sha256=binding.revision_sha256,
            snapshot_sha256=binding.snapshot_sha256,
            process_receipt_sha256=binding.process_receipt_sha256,
            desired_state_sha256=binding.desired_state_sha256,
            authority_epoch=binding.authority_epoch,
            snapshot_revision=binding.snapshot_revision,
            publication_envelope_sha256=_sha("test-publication-envelope"),
            transport_receipt_sha256=_sha("test-transport-receipt"),
            committed_at=NOW,
        )

    async def async_stop(self) -> dict:
        self.log.append("snapshots.stop")
        self.stop_calls += 1
        return {
            "state": "stopped",
            "last_error_code": None,
            "publication": {"transport_state": "published", "dirty": False},
        }

    def diagnostics(self) -> dict:
        return {
            "state": "running",
            "last_error_code": None,
            "secret": "must-not-escape",
        }


def _outcome(*, fact: bool = False, label: str = "output") -> PipelineOutcome:
    return PipelineOutcome(
        output_sha256=_sha(label),
        disposition="advanced",
        fact_revision_staged=fact,
        process_receipt_sha256=_sha(f"{label}:process"),
    )


class _OutcomeAuthority:
    def __init__(self, *, mutate=None, reject: bool = False) -> None:
        self.mutate = mutate
        self.reject = reject
        self.calls = 0

    def verify(self, *, lease, desired_state, outcome):
        self.calls += 1
        if self.reject:
            raise RuntimeError("durable output receipt ontbreekt")
        fact_binding = None
        if outcome.fact_revision_staged:
            fact_binding = FactPublicationBinding(
                pipeline_item_id=f"completion:{lease.event_id}",
                revision_sha256=_sha(f"{lease.event_id}:fact-revision"),
                snapshot_sha256=_sha(f"{lease.event_id}:snapshot"),
                process_receipt_sha256=outcome.process_receipt_sha256,
                desired_state_sha256=desired_state.state_sha256,
                authority_epoch=desired_state.authority_epoch,
                snapshot_revision=2,
            )
        verified = VerifiedPipelineOutcome(
            event_id=lease.event_id,
            event_sha256=lease.event_sha256,
            producer_receipt_sha256=lease.producer_receipt_sha256,
            desired_state_sha256=desired_state.state_sha256,
            authority_epoch=desired_state.authority_epoch,
            authority_sha256=desired_authority_proof(desired_state)[
                "authority_sha256"
            ],
            consumer_id=lease.consumer_id,
            stream_id=lease.stream_id,
            stream_sequence=lease.stream_sequence,
            output_sha256=outcome.output_sha256,
            output_receipt_sha256=_sha(f"{lease.event_id}:output-receipt"),
            process_receipt_sha256=outcome.process_receipt_sha256,
            disposition=outcome.disposition,
            verifier_profile_sha256=_sha("test-outcome-authority-profile"),
            fact_binding=fact_binding,
        )
        return verified if self.mutate is None else self.mutate(verified)


def _runtime(
    desired_store: _DesiredStore,
    completion: _Completion,
    facts: _Facts,
    snapshots: _Snapshots,
    processor,
    outcome_authority=None,
    **kwargs,
) -> WorkerFirstRuntimeCoordinator:
    store_bindings = kwargs.pop("store_bindings", _FAKE_STORE_BINDINGS)
    ownership_lease = kwargs.pop(
        "ownership_lease",
        _FakeOwnershipLease(store_bindings),
    )
    return WorkerFirstRuntimeCoordinator(
        desired_state_store=desired_store,
        completion_lifecycle=completion,
        fact_revision_lifecycle=facts,
        snapshot_lifecycle=snapshots,
        processor=processor,
        outcome_authority=(
            _OutcomeAuthority()
            if outcome_authority is None
            else outcome_authority
        ),
        ownership_lease=ownership_lease,
        store_bindings=store_bindings,
        consumer_id=CONSUMER,
        stream_id=STREAM,
        clock=lambda: NOW,
        **kwargs,
    )


async def _wait_until(predicate, *, timeout: float = 1.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline
        await asyncio.sleep(0.005)


def _assert_worker_threads_stop(*, timeout: float = 1.0) -> None:
    deadline = time.monotonic() + timeout
    while any(
        thread.name.startswith("stv3-worker-first")
        for thread in threading.enumerate()
    ):
        assert time.monotonic() < deadline
        time.sleep(0.005)


def test_module_is_inert_and_service_binding_is_explicitly_selected():
    tree = ast.parse(MODULE.read_text(encoding="utf-8"))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add(node.module or "")
    forbidden = ("service", "mqtt", "homeassistant", "providers", "tracking")
    assert not any(
        token in module.lower()
        for module in imported
        for token in forbidden
    )
    service_tree = ast.parse(
        (MODULE.parent / "service.py").read_text(encoding="utf-8")
    )
    service_imports: set[str] = set()
    for node in ast.walk(service_tree):
        if isinstance(node, ast.Import):
            service_imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            service_imports.add(node.module or "")
    assert any(
        module.endswith("worker_first_service_activation")
        for module in service_imports
    )
    assert any(
        module.endswith("worker_first_service_composition")
        for module in service_imports
    )

    worker_service = next(
        node
        for node in service_tree.body
        if isinstance(node, ast.ClassDef) and node.name == "WorkerService"
    )
    constructor = next(
        node
        for node in worker_service.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "__init__"
    )
    keyword_defaults = dict(
        zip(
            (argument.arg for argument in constructor.args.kwonlyargs),
            constructor.args.kw_defaults,
            strict=True,
        )
    )
    injected_default = keyword_defaults["worker_first_runtime_factory"]
    assert isinstance(injected_default, ast.Constant)
    assert injected_default.value is None


def test_constructor_requires_exact_held_six_store_authority():
    desired = _desired()
    missing = dict(_FAKE_STORE_BINDINGS)
    missing.pop("pipeline_output_store")
    with pytest.raises(
        WorkerFirstRuntimeOwnershipError,
        match="ownership_store_binding_invalid",
    ):
        _runtime(
            _DesiredStore(desired),
            _Completion(),
            _Facts(),
            _Snapshots(),
            lambda *_: _outcome(),
            store_bindings=missing,
            ownership_lease=_FakeOwnershipLease(missing),
        )

    mismatch = _FakeOwnershipLease()
    with pytest.raises(
        WorkerFirstRuntimeOwnershipError,
        match="ownership_authority_invalid",
    ):
        _runtime(
            _DesiredStore(desired),
            _Completion(),
            _Facts(),
            _Snapshots(),
            lambda *_: _outcome(),
            ownership_lease=mismatch,
            store_bindings={
                **_FAKE_STORE_BINDINGS,
                "pipeline_output_store": (
                    _FAKE_STORE_BINDINGS["pipeline_output_store"][0],
                    _sha("wrong-pipeline-identity"),
                ),
            },
        )


def test_real_cross_process_lease_is_transferred_and_released_after_stop(
    tmp_path,
):
    fence, lease, stores, authority_path, authority_sha = _real_ownership(
        tmp_path
    )
    runtime = _runtime(
        _DesiredStore(_desired()),
        _Completion(),
        _Facts(),
        _Snapshots(),
        lambda *_: _outcome(),
        ownership_lease=lease,
        store_bindings=stores,
    )

    async def scenario():
        started = await runtime.async_start()
        stopped = await runtime.async_stop()
        return started, stopped

    started, stopped = asyncio.run(scenario())
    assert started["ownership"]["fence_held"] is True
    assert started["ownership"]["store_count"] == 6
    assert stopped["ownership"]["fence_held"] is False
    with pytest.raises(WorkerFirstFenceLost, match="ownership_not_held"):
        lease.assert_held()

    successor = WorkerFirstOwnershipFence(
        authority_path,
        expected_authority_sha256=authority_sha,
    ).acquire()
    successor.assert_held()
    successor.release()
    fence.close()


def test_real_os_lease_releases_from_late_processor_completion(tmp_path):
    fence, lease, stores, authority_path, authority_sha = _real_ownership(
        tmp_path
    )
    desired = _desired()
    entered = threading.Event()
    release = threading.Event()

    def processor(*_args):
        entered.set()
        release.wait(2.0)
        return _outcome()

    runtime = _runtime(
        _DesiredStore(desired),
        _Completion([_lease(desired)]),
        _Facts(),
        _Snapshots(),
        processor,
        ownership_lease=lease,
        store_bindings=stores,
        lease_seconds=0.2,
        processing_timeout_seconds=0.02,
        cancel_grace_seconds=0.02,
    )

    async def scenario():
        await runtime.async_start()
        failed = await runtime.async_tick()
        assert failed["last_error_code"] == "pipeline_processor_timeout"
        assert entered.is_set()
        stopped = await runtime.async_stop()
        assert stopped["state"] == "stopped_pending_processor"
        assert stopped["ownership"]["fence_held"] is True
        release.set()
        deadline = time.monotonic() + 1.0
        while runtime.diagnostics()["ownership"]["fence_held"]:
            assert time.monotonic() < deadline
            await asyncio.sleep(0.005)

    asyncio.run(scenario())
    with pytest.raises(WorkerFirstFenceLost, match="ownership_not_held"):
        lease.assert_held()
    successor = WorkerFirstOwnershipFence(
        authority_path,
        expected_authority_sha256=authority_sha,
    ).acquire()
    successor.release()
    fence.close()


def test_fake_lease_fences_every_durable_and_snapshot_boundary():
    desired = _desired()
    log: list[str] = []
    ownership = _FakeOwnershipLease(log=log)
    completion = _Completion([_lease(desired)], log=log)
    facts = _Facts(log=log)
    snapshots = _Snapshots(log=log)

    def processor(*_args):
        log.append("processor.commit")
        return _outcome()

    runtime = _runtime(
        _DesiredStore(desired, log=log),
        completion,
        facts,
        snapshots,
        processor,
        ownership_lease=ownership,
    )

    async def scenario():
        await runtime.async_start()
        await runtime.async_tick()
        await runtime.async_stop()

    asyncio.run(scenario())
    boundaries = {
        "completion.resume",
        "completion.audit",
        "facts.audit",
        "facts.supersede",
        "desired.load",
        "snapshots.start",
        "snapshots.tick",
        "completion.claim",
        "processor.commit",
        "completion.ack",
        "snapshots.stop",
    }
    for marker in boundaries:
        position = log.index(marker)
        assert log[position - 1] == "ownership.assert", marker
        assert log[position + 1] == "ownership.assert", marker
    assert ownership.binding_calls == 1
    assert ownership.release_calls == 1
    assert ownership.held is False
    assert log[-1] == "ownership.release"


@pytest.mark.parametrize("boundary", ["resume", "audit", "fact"])
def test_blocked_start_control_boundaries_stop_bounded_and_release_only_late(
    boundary,
):
    entered = threading.Event()
    release = threading.Event()

    class Completion(_Completion):
        def resume_pending_prunes(self):
            if boundary == "resume":
                entered.set()
                release.wait(2.0)
            return super().resume_pending_prunes()

        def audit_archive(self):
            if boundary == "audit":
                entered.set()
                release.wait(2.0)
            return super().audit_archive()

    class Facts(_Facts):
        def audit(self):
            if boundary == "fact":
                entered.set()
                release.wait(2.0)
            return super().audit()

    ownership = _FakeOwnershipLease()
    runtime = _runtime(
        _DesiredStore(_desired()),
        Completion(),
        Facts(),
        _Snapshots(),
        lambda *_: _outcome(),
        ownership_lease=ownership,
        control_timeout_seconds=1.0,
        stop_timeout_seconds=0.02,
    )

    async def scenario():
        starter = asyncio.create_task(runtime.async_start())
        assert await asyncio.to_thread(entered.wait, 0.5)
        started = time.monotonic()
        stopped = await runtime.async_stop()
        elapsed = time.monotonic() - started
        assert elapsed < 0.15
        assert stopped["ownership"]["fence_held"] is True
        assert stopped["ownership"]["mutable_work_count"] == 1
        assert ownership.release_calls == 0
        release.set()
        with pytest.raises(Exception, match="runtime_stopping"):
            await starter
        await _wait_until(lambda: ownership.release_calls == 1)

    asyncio.run(scenario())
    assert runtime.diagnostics()["ownership"]["fence_held"] is False
    _assert_worker_threads_stop()


@pytest.mark.parametrize(
    "boundary",
    ["fact", "claim", "verify", "ack", "reject"],
)
def test_blocked_tick_control_boundaries_never_overlap_or_release_early(boundary):
    desired = _desired()
    entered = threading.Event()
    release = threading.Event()
    blocked = False

    class Completion(_Completion):
        def claim_next(self, *args, **kwargs):
            nonlocal blocked
            if boundary == "claim" and not blocked:
                blocked = True
                self.claim_calls += 1
                self.log.append("completion.claim")
                entered.set()
                release.wait(2.0)
                return None
            return super().claim_next(*args, **kwargs)

        def acknowledge(self, *args, **kwargs):
            nonlocal blocked
            if boundary == "ack" and not blocked:
                blocked = True
                entered.set()
                release.wait(2.0)
            return super().acknowledge(*args, **kwargs)

        def reject_leased(self, *args, **kwargs):
            nonlocal blocked
            if boundary == "reject" and not blocked:
                blocked = True
                entered.set()
                release.wait(2.0)
            return super().reject_leased(*args, **kwargs)

    class Facts(_Facts):
        armed = False

        def supersede_obsolete(self, active_desired_state):
            nonlocal blocked
            if boundary == "fact" and self.armed and not blocked:
                blocked = True
                entered.set()
                release.wait(2.0)
            return super().supersede_obsolete(active_desired_state)

    class Authority(_OutcomeAuthority):
        def verify(self, **kwargs):
            nonlocal blocked
            if boundary == "verify" and not blocked:
                blocked = True
                entered.set()
                release.wait(2.0)
            return super().verify(**kwargs)

    lease = _lease(
        desired,
        desired_sha=(
            _sha("obsolete-for-reject")
            if boundary == "reject"
            else desired.state_sha256
        ),
    )
    completion = Completion(
        [lease] if boundary in {"verify", "ack", "reject"} else []
    )
    facts = Facts()
    ownership = _FakeOwnershipLease()
    runtime = _runtime(
        _DesiredStore(desired),
        completion,
        facts,
        _Snapshots(),
        lambda *_: _outcome(),
        outcome_authority=Authority(),
        ownership_lease=ownership,
        control_timeout_seconds=1.0,
        stop_timeout_seconds=0.02,
    )

    async def scenario():
        await runtime.async_start()
        facts.armed = True
        ticker = asyncio.create_task(runtime.async_tick())
        assert await asyncio.to_thread(entered.wait, 0.5)
        started = time.monotonic()
        first_stop = await runtime.async_stop()
        elapsed = time.monotonic() - started
        assert elapsed < 0.15
        assert first_stop["ownership"]["fence_held"] is True
        assert ownership.release_calls == 0
        release.set()
        await ticker
        assert ownership.release_calls == 0
        final = await runtime.async_stop()
        return final

    final = asyncio.run(scenario())
    assert final["state"] == "stopped"
    assert final["ownership"]["fence_held"] is False
    assert ownership.release_calls == 1
    if boundary == "ack":
        assert completion.acknowledged == [("event-1", _sha("output"))]
        assert final["counts"]["acknowledged"] == 1
    else:
        assert completion.acknowledged == []
    if boundary == "reject":
        assert len(completion.rejected) == 1
        assert final["counts"]["rejected"] == 1
    _assert_worker_threads_stop()


@pytest.mark.parametrize("boundary", ["ack", "reject"])
def test_timed_out_ack_or_reject_reconciles_exactly_once_when_thread_finishes(
    boundary,
):
    desired = _desired()
    entered = threading.Event()
    release = threading.Event()

    class Completion(_Completion):
        def acknowledge(self, *args, **kwargs):
            if boundary == "ack":
                entered.set()
                release.wait(2.0)
            return super().acknowledge(*args, **kwargs)

        def reject_leased(self, *args, **kwargs):
            if boundary == "reject":
                entered.set()
                release.wait(2.0)
            return super().reject_leased(*args, **kwargs)

    completion = Completion([_lease(
        desired,
        desired_sha=(
            _sha("timed-out-obsolete")
            if boundary == "reject"
            else desired.state_sha256
        ),
    )])
    ownership = _FakeOwnershipLease()
    runtime = _runtime(
        _DesiredStore(desired),
        completion,
        _Facts(),
        _Snapshots(),
        lambda *_: _outcome(),
        ownership_lease=ownership,
        control_timeout_seconds=0.02,
        stop_timeout_seconds=0.05,
    )

    async def scenario():
        await runtime.async_start()
        ticker = asyncio.create_task(runtime.async_tick())
        assert await asyncio.to_thread(entered.wait, 0.5)
        timed_out = await ticker
        assert timed_out["last_error_code"] == "control_operation_timeout"
        pending = await runtime.async_tick()
        assert pending["last_error_code"] == "control_operation_pending"
        release.set()
        await _wait_until(
            lambda: runtime.diagnostics()["ownership"][
                "mutable_work_count"
            ] == 0
        )
        reconciled = runtime.diagnostics()
        assert reconciled["counts"]["terminalized"] == 1
        recovered = await runtime.async_tick()
        assert recovered["state"] == "running"
        return await runtime.async_stop()

    stopped = asyncio.run(scenario())
    assert stopped["counts"]["acknowledged"] == (1 if boundary == "ack" else 0)
    assert stopped["counts"]["rejected"] == (1 if boundary == "reject" else 0)
    assert len(completion.acknowledged) == (1 if boundary == "ack" else 0)
    assert len(completion.rejected) == (1 if boundary == "reject" else 0)
    assert ownership.release_calls == 1
    _assert_worker_threads_stop()


@pytest.mark.parametrize("kind", ["control", "snapshot"])
@pytest.mark.parametrize("external_cancel", [False, True])
def test_timeout_or_external_cancel_allows_retry_only_after_real_quiescence(
    kind,
    external_cancel,
):
    desired = _desired()
    control_entered = threading.Event()
    control_release = threading.Event()
    snapshot_entered = asyncio.Event()
    snapshot_release = asyncio.Event()

    class Completion(_Completion):
        def __init__(self):
            super().__init__()
            self.blocked = False

        def claim_next(self, *args, **kwargs):
            self.claim_calls += 1
            self.log.append("completion.claim")
            if kind == "control" and not self.blocked:
                self.blocked = True
                control_entered.set()
                control_release.wait(2.0)
            return None

    class Snapshots(_Snapshots):
        def __init__(self):
            super().__init__()
            self.armed = False
            self.tick_calls = 0

        async def async_tick(self):
            self.tick_calls += 1
            if kind == "snapshot" and self.armed and self.tick_calls == 1:
                snapshot_entered.set()
                await snapshot_release.wait()
            return await super().async_tick()

    completion = Completion()
    snapshots = Snapshots()
    ownership = _FakeOwnershipLease()
    runtime = _runtime(
        _DesiredStore(desired),
        completion,
        _Facts(),
        snapshots,
        lambda *_: _outcome(),
        ownership_lease=ownership,
        control_timeout_seconds=(1.0 if external_cancel else 0.02),
        stop_timeout_seconds=0.05,
    )

    async def scenario():
        await runtime.async_start()
        snapshots.armed = True
        ticker = asyncio.create_task(runtime.async_tick())
        if kind == "control":
            assert await asyncio.to_thread(control_entered.wait, 0.5)
        else:
            await asyncio.wait_for(snapshot_entered.wait(), timeout=0.5)
        if external_cancel:
            ticker.cancel()
            with pytest.raises(asyncio.CancelledError):
                await ticker
        else:
            timed_out = await ticker
            expected = (
                "control_operation_timeout"
                if kind == "control"
                else "snapshot_operation_timeout"
            )
            assert timed_out["last_error_code"] == expected

        pending = await runtime.async_tick()
        expected_pending = (
            "control_operation_pending"
            if kind == "control"
            else "snapshot_operation_pending"
        )
        assert pending["last_error_code"] == expected_pending
        assert ownership.release_calls == 0

        if kind == "control":
            control_release.set()
            await _wait_until(
                lambda: runtime.diagnostics()["ownership"][
                    "mutable_work_count"
                ] == 0
            )
        else:
            snapshot_release.set()
            await _wait_until(
                lambda: runtime.diagnostics()["ownership"][
                    "snapshot_task_count"
                ] == 0
            )
        recovered = await runtime.async_tick()
        assert recovered["state"] == "running"
        await runtime.async_stop()

    asyncio.run(scenario())
    expected_calls = 2
    assert (
        completion.claim_calls if kind == "control" else snapshots.tick_calls
    ) == expected_calls
    assert ownership.release_calls == 1
    assert runtime.diagnostics()["counts"]["late_completions"] >= 1
    _assert_worker_threads_stop()


@pytest.mark.parametrize("boundary", ["start", "tick", "publish", "stop"])
def test_blocked_snapshot_boundaries_obey_stop_budget_and_hold_owner(boundary):
    desired = _desired()
    entered = asyncio.Event()
    release = asyncio.Event()

    class Snapshots(_Snapshots):
        def __init__(self):
            super().__init__()
            self.armed = boundary == "start"
            self.blocked = False

        async def _block_once(self):
            if self.armed and not self.blocked:
                self.blocked = True
                entered.set()
                await release.wait()

        async def async_start(self):
            if boundary == "start":
                await self._block_once()
            return await super().async_start()

        async def async_tick(self):
            if boundary == "tick":
                await self._block_once()
            return await super().async_tick()

        async def async_publish_exact_fact(self, binding):
            if boundary == "publish":
                await self._block_once()
            return await super().async_publish_exact_fact(binding)

        async def async_stop(self):
            if boundary == "stop":
                await self._block_once()
            return await super().async_stop()

    snapshots = Snapshots()
    ownership = _FakeOwnershipLease()
    completion = _Completion(
        [_lease(desired)] if boundary == "publish" else []
    )
    runtime = _runtime(
        _DesiredStore(desired),
        completion,
        _Facts(),
        snapshots,
        lambda *_: _outcome(fact=boundary == "publish"),
        ownership_lease=ownership,
        control_timeout_seconds=1.0,
        stop_timeout_seconds=0.02,
    )

    async def scenario():
        operation = None
        if boundary == "start":
            operation = asyncio.create_task(runtime.async_start())
        else:
            await runtime.async_start()
            snapshots.armed = True
            if boundary in {"tick", "publish"}:
                operation = asyncio.create_task(runtime.async_tick())
            else:
                operation = asyncio.create_task(runtime.async_stop())

        await asyncio.wait_for(entered.wait(), timeout=0.5)
        started = time.monotonic()
        first_stop = (
            await operation
            if boundary == "stop"
            else await runtime.async_stop()
        )
        elapsed = time.monotonic() - started
        assert elapsed < 0.15
        assert first_stop["ownership"]["fence_held"] is True
        assert ownership.release_calls == 0
        release.set()

        if boundary == "start":
            with pytest.raises(Exception, match="runtime_stopping"):
                await operation
        elif boundary in {"tick", "publish"}:
            await operation

        await _wait_until(
            lambda: runtime.diagnostics()["ownership"][
                "snapshot_task_count"
            ] == 0
        )
        final = await runtime.async_stop()
        return final

    final = asyncio.run(scenario())
    assert final["state"] == "stopped"
    assert final["ownership"]["fence_held"] is False
    assert ownership.release_calls == 1
    assert snapshots.blocked is True
    if boundary == "publish":
        assert snapshots.fact_calls == 1
        assert completion.acknowledged == []
    _assert_worker_threads_stop()


def test_external_stop_cancellation_keeps_owner_until_snapshot_stop_finishes():
    desired = _desired()
    entered = asyncio.Event()
    release = asyncio.Event()

    class Snapshots(_Snapshots):
        async def async_stop(self):
            entered.set()
            await release.wait()
            return await super().async_stop()

    snapshots = Snapshots()
    ownership = _FakeOwnershipLease()
    runtime = _runtime(
        _DesiredStore(desired),
        _Completion(),
        _Facts(),
        snapshots,
        lambda *_: _outcome(),
        ownership_lease=ownership,
        control_timeout_seconds=1.0,
        stop_timeout_seconds=0.5,
    )

    async def scenario():
        await runtime.async_start()
        stopper = asyncio.create_task(runtime.async_stop())
        await asyncio.wait_for(entered.wait(), timeout=0.5)
        stopper.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stopper

        pending = runtime.diagnostics()
        assert pending["ownership"]["fence_held"] is True
        assert pending["ownership"]["snapshot_task_count"] == 1
        assert ownership.release_calls == 0

        release.set()
        await _wait_until(lambda: ownership.release_calls == 1)
        final = runtime.diagnostics()
        assert final["state"] == "stopped"
        assert final["ownership"]["fence_held"] is False
        assert final["ownership"]["snapshot_task_count"] == 0
        await asyncio.sleep(0)
        current = asyncio.current_task()
        assert not [
            task
            for task in asyncio.all_tasks()
            if task is not current and not task.done()
        ]

    asyncio.run(scenario())
    _assert_worker_threads_stop()


@pytest.mark.parametrize("committed_before_stop", [False, True])
def test_processor_stop_before_or_after_durable_commit_never_acks(
    committed_before_stop,
):
    desired = _desired()
    entered = threading.Event()
    durable_outputs: list[str] = []

    def processor(_lease, _desired_state, cancel_event):
        if committed_before_stop:
            durable_outputs.append(_sha("processor-durable-output"))
        entered.set()
        assert cancel_event.wait(1.0)
        return _outcome(label="processor-durable-output")

    completion = _Completion([_lease(desired)])
    ownership = _FakeOwnershipLease()
    runtime = _runtime(
        _DesiredStore(desired),
        completion,
        _Facts(),
        _Snapshots(),
        processor,
        ownership_lease=ownership,
        lease_seconds=2.0,
        processing_timeout_seconds=1.0,
        cancel_grace_seconds=0.1,
        control_timeout_seconds=0.5,
        stop_timeout_seconds=0.2,
    )

    async def scenario():
        await runtime.async_start()
        ticker = asyncio.create_task(runtime.async_tick())
        assert await asyncio.to_thread(entered.wait, 0.5)
        started = time.monotonic()
        stopped = await runtime.async_stop()
        elapsed = time.monotonic() - started
        await ticker
        return stopped, elapsed

    stopped, elapsed = asyncio.run(scenario())
    assert elapsed < 0.4
    assert stopped["state"] == "stopped"
    assert stopped["ownership"]["fence_held"] is False
    assert durable_outputs == (
        [_sha("processor-durable-output")]
        if committed_before_stop
        else []
    )
    assert completion.acknowledged == []
    assert ownership.release_calls == 1
    _assert_worker_threads_stop()


def test_noncooperative_processor_stop_returns_before_processing_timeout():
    desired = _desired()
    entered = threading.Event()
    release = threading.Event()

    def processor(*_args):
        entered.set()
        release.wait(2.0)
        return _outcome(label="late-noncooperative-output")

    ownership = _FakeOwnershipLease()
    completion = _Completion([_lease(desired)])
    runtime = _runtime(
        _DesiredStore(desired),
        completion,
        _Facts(),
        _Snapshots(),
        processor,
        ownership_lease=ownership,
        lease_seconds=2.0,
        processing_timeout_seconds=1.0,
        cancel_grace_seconds=0.1,
        control_timeout_seconds=0.5,
        stop_timeout_seconds=0.02,
    )

    async def scenario():
        await runtime.async_start()
        ticker = asyncio.create_task(runtime.async_tick())
        assert await asyncio.to_thread(entered.wait, 0.5)
        started = time.monotonic()
        first = await runtime.async_stop()
        elapsed = time.monotonic() - started
        assert elapsed < 0.15
        assert first["state"] == "stopped_pending_processor"
        assert first["ownership"]["fence_held"] is True
        assert ownership.release_calls == 0
        release.set()
        await ticker
        assert ownership.release_calls == 0
        final = await runtime.async_stop()
        return final

    final = asyncio.run(scenario())
    assert final["state"] == "stopped"
    assert final["ownership"]["fence_held"] is False
    assert completion.acknowledged == []
    assert ownership.release_calls == 1
    _assert_worker_threads_stop()


def test_start_audits_durable_authorities_in_order_and_diagnostics_are_bounded():
    desired = _desired()
    log: list[str] = []
    completion = _Completion(log=log)
    facts = _Facts(log=log)
    snapshots = _Snapshots(log=log)
    runtime = _runtime(
        _DesiredStore(desired), completion, facts, snapshots,
        lambda *_: _outcome(),
    )

    async def scenario():
        started = await runtime.async_start()
        stopped = await runtime.async_stop()
        return started, stopped

    started, stopped = asyncio.run(scenario())
    assert log[:5] == [
        "completion.resume",
        "completion.audit",
        "facts.audit",
        "facts.supersede",
        "snapshots.start",
    ]
    assert started["state"] == "running"
    encoded = json.dumps(started, sort_keys=True).encode("utf-8")
    assert len(encoded) <= MAX_DIAGNOSTIC_BYTES
    assert b"secret" not in encoded
    assert b"payload" not in encoded
    assert stopped["state"] == "stopped"
    assert not stopped["owns_heavy_lane"]


def test_unconfigured_start_and_tick_terminalize_facts_and_reject_without_ack():
    desired = _desired()
    completion = _Completion([_lease(desired)])
    facts = _Facts()
    snapshots = _Snapshots(unconfigured=True)
    processor_calls = 0

    def processor(*_args):
        nonlocal processor_calls
        processor_calls += 1
        return _outcome(fact=True)

    runtime = _runtime(
        _DesiredStore(None),
        completion,
        facts,
        snapshots,
        processor,
    )

    async def scenario():
        started = await runtime.async_start()
        ticked = await runtime.async_tick()
        stopped = await runtime.async_stop()
        return started, ticked, stopped

    started, ticked, stopped = asyncio.run(scenario())
    assert started["state"] == "unconfigured"
    assert ticked["state"] == "unconfigured"
    assert facts.supersede_all_reasons == [
        "desired_state_unavailable",
        "desired_state_unavailable",
    ]
    assert completion.rejected == [(
        "event-1",
        "active_desired_state_unavailable",
        None,
    )]
    assert completion.acknowledged == []
    assert ticked["counts"]["acknowledged"] == 0
    assert ticked["counts"]["rejected"] == 1
    assert ticked["counts"]["terminalized"] == 1
    assert snapshots.fact_calls == 0
    assert processor_calls == 0
    assert stopped["ownership"]["fence_held"] is False


def test_unconfigured_snapshot_tick_is_validated_fail_closed():
    snapshots = _Snapshots(unconfigured=True)
    runtime = _runtime(
        _DesiredStore(None),
        _Completion(),
        _Facts(),
        snapshots,
        lambda *_: _outcome(),
    )

    async def scenario():
        await runtime.async_start()
        snapshots.unconfigured = False
        result = await runtime.async_tick()
        await runtime.async_stop()
        return result

    result = asyncio.run(scenario())
    assert result["state"] == "degraded"
    assert result["last_error_code"] == "snapshot_runtime_tick_failed"


def test_exactly_one_global_owner_and_failed_start_is_permanently_closed():
    desired = _desired()
    first = _runtime(
        _DesiredStore(desired), _Completion(), _Facts(), _Snapshots(),
        lambda *_: _outcome(label="first"),
    )
    second = _runtime(
        _DesiredStore(desired), _Completion(), _Facts(), _Snapshots(),
        lambda *_: _outcome(label="second"),
    )

    async def scenario():
        await first.async_start()
        with pytest.raises(WorkerFirstRuntimeOwnershipError):
            await second.async_start()
        assert second.diagnostics()["closed"] is True
        await first.async_stop()
        third = _runtime(
            _DesiredStore(desired), _Completion(), _Facts(), _Snapshots(),
            lambda *_: _outcome(label="third"),
        )
        await third.async_start()
        await third.async_stop()

    asyncio.run(scenario())


def test_subclass_cannot_split_the_process_local_owner():
    class SubclassCoordinator(WorkerFirstRuntimeCoordinator):
        pass

    desired = _desired()
    first = _runtime(
        _DesiredStore(desired), _Completion(), _Facts(), _Snapshots(),
        lambda *_: _outcome(label="first"),
    )
    second = SubclassCoordinator(
        desired_state_store=_DesiredStore(desired),
        completion_lifecycle=_Completion(),
        fact_revision_lifecycle=_Facts(),
        snapshot_lifecycle=_Snapshots(),
        processor=lambda *_: _outcome(label="second"),
        outcome_authority=_OutcomeAuthority(),
        ownership_lease=_FakeOwnershipLease(),
        store_bindings=_FAKE_STORE_BINDINGS,
        consumer_id=CONSUMER,
        stream_id=STREAM,
        clock=lambda: NOW,
    )

    async def scenario():
        await first.async_start()
        with pytest.raises(WorkerFirstRuntimeOwnershipError):
            await second.async_start()
        await first.async_stop()

    asyncio.run(scenario())


def test_ready_completion_is_acked_only_after_admitted_fact_snapshot():
    desired = _desired()
    log: list[str] = []
    completion = _Completion([_lease(desired)], log=log)
    facts = _Facts(log=log)
    snapshots = _Snapshots(log=log)

    def processor(lease, active, cancel_event):
        assert lease.payload == b"raw"
        assert active == desired
        assert not cancel_event.is_set()
        log.append("processor.commit")
        return _outcome(fact=True)

    runtime = _runtime(
        _DesiredStore(desired), completion, facts, snapshots, processor,
    )

    async def scenario():
        await runtime.async_start()
        result = await runtime.async_tick()
        await runtime.async_stop()
        return result

    result = asyncio.run(scenario())
    assert result["counts"]["advanced"] == 1
    assert result["counts"]["acknowledged"] == 1
    assert log.index("processor.commit") < log.index("snapshots.fact")
    assert log.index("snapshots.fact") < log.index("completion.ack")
    assert completion.acknowledged == [("event-1", _sha("output"))]


def test_authority_mismatch_is_withheld_without_processor_or_fact_publication():
    desired = _desired()
    bad = _lease(desired, desired_sha=_sha("obsolete"))
    completion = _Completion([bad])
    snapshots = _Snapshots()
    calls = 0

    def processor(*_):
        nonlocal calls
        calls += 1
        return _outcome(fact=True)

    runtime = _runtime(
        _DesiredStore(desired), completion, _Facts(), snapshots, processor,
    )

    async def scenario():
        await runtime.async_start()
        result = await runtime.async_tick()
        await runtime.async_stop()
        return result

    result = asyncio.run(scenario())
    assert calls == 0
    assert snapshots.fact_calls == 0
    assert result["counts"]["authority_withheld"] == 1
    assert result["last_disposition"] == "withheld"
    assert completion.acknowledged == []
    assert completion.rejected == [(
        "event-1",
        "desired_state_superseded_before_processing",
        desired.state_sha256,
    )]


def test_desired_change_after_processing_supersedes_fact_and_acks_withheld():
    first = _desired()
    second = _desired(generation=2, previous=first.state_sha256)
    desired_store = _DesiredStore(first)
    completion = _Completion([_lease(first)])
    facts = _Facts()
    snapshots = _Snapshots()

    def processor(*_):
        desired_store.active = second
        return _outcome(fact=True)

    runtime = _runtime(
        desired_store, completion, facts, snapshots, processor,
    )

    async def scenario():
        await runtime.async_start()
        result = await runtime.async_tick()
        await runtime.async_stop()
        return result

    result = asyncio.run(scenario())
    assert snapshots.fact_calls == 0
    assert result["counts"]["authority_withheld"] == 1
    assert facts.superseded[-1] == second.state_sha256
    assert completion.acknowledged == []
    assert completion.rejected[0][1] == "desired_state_changed_after_processing"


def test_desired_becomes_unavailable_after_processing_supersedes_all_ready():
    desired = _desired()
    desired_store = _DesiredStore(desired)
    completion = _Completion([_lease(desired)])
    facts = _Facts()

    def processor(*_args):
        desired_store.active = None
        return _outcome(fact=True)

    runtime = _runtime(
        desired_store,
        completion,
        facts,
        _Snapshots(),
        processor,
    )

    async def scenario():
        await runtime.async_start()
        result = await runtime.async_tick()
        await runtime.async_stop()
        return result

    result = asyncio.run(scenario())
    assert facts.supersede_all_reasons == ["desired_state_unavailable"]
    assert completion.acknowledged == []
    assert completion.rejected == [(
        "event-1",
        "active_desired_state_unavailable",
        None,
    )]
    assert result["state"] == "unconfigured"


def test_desired_change_during_fact_publication_is_withheld_before_ack():
    first = _desired()
    second = _desired(generation=2, previous=first.state_sha256)
    desired_store = _DesiredStore(first)

    class RacingSnapshots(_Snapshots):
        async def async_publish_exact_fact(self, binding):
            result = await super().async_publish_exact_fact(binding)
            desired_store.active = second
            return result

    completion = _Completion([_lease(first)])
    snapshots = RacingSnapshots()
    runtime = _runtime(
        desired_store,
        completion,
        _Facts(),
        snapshots,
        lambda *_: _outcome(fact=True),
    )

    async def scenario():
        await runtime.async_start()
        result = await runtime.async_tick()
        await runtime.async_stop()
        return result

    result = asyncio.run(scenario())
    assert snapshots.fact_calls == 1
    assert result["last_disposition"] == "withheld"
    assert result["counts"]["authority_withheld"] == 1
    assert completion.acknowledged == []
    assert completion.rejected[0][1] == "desired_state_changed_before_ack"


def test_desired_becomes_unavailable_before_ack_supersedes_all_ready():
    desired = _desired()
    desired_store = _DesiredStore(desired)

    class RacingSnapshots(_Snapshots):
        async def async_publish_exact_fact(self, binding):
            receipt = await super().async_publish_exact_fact(binding)
            desired_store.active = None
            return receipt

    completion = _Completion([_lease(desired)])
    facts = _Facts()
    snapshots = RacingSnapshots()
    runtime = _runtime(
        desired_store,
        completion,
        facts,
        snapshots,
        lambda *_: _outcome(fact=True),
    )

    async def scenario():
        await runtime.async_start()
        result = await runtime.async_tick()
        await runtime.async_stop()
        return result

    result = asyncio.run(scenario())
    assert snapshots.fact_calls == 1
    assert facts.supersede_all_reasons == ["desired_state_unavailable"]
    assert completion.acknowledged == []
    assert completion.rejected == [(
        "event-1",
        "active_desired_state_unavailable",
        None,
    )]
    assert result["state"] == "unconfigured"


def test_forged_acknowledgement_receipt_never_advances_runtime_count():
    desired = _desired()
    completion = _ForgedAckCompletion([_lease(desired)])
    runtime = _runtime(
        _DesiredStore(desired), completion, _Facts(), _Snapshots(),
        lambda *_: _outcome(),
    )

    async def scenario():
        await runtime.async_start()
        result = await runtime.async_tick()
        await runtime.async_stop()
        return result

    result = asyncio.run(scenario())
    assert result["state"] == "degraded"
    assert result["counts"]["acknowledged"] == 0
    assert result["counts"]["advanced"] == 0


@pytest.mark.parametrize("mode", ["missing", "wrong_event", "wrong_output"])
def test_unverified_or_mismatched_durable_output_never_acknowledges(mode):
    desired = _desired()
    completion = _Completion([_lease(desired)])
    if mode == "missing":
        authority = _OutcomeAuthority(reject=True)
    elif mode == "wrong_event":
        authority = _OutcomeAuthority(
            mutate=lambda value: replace(value, event_id="event-other")
        )
    else:
        authority = _OutcomeAuthority(
            mutate=lambda value: replace(
                value,
                output_sha256=_sha("different-output"),
            )
        )
    runtime = _runtime(
        _DesiredStore(desired),
        completion,
        _Facts(),
        _Snapshots(),
        lambda *_: _outcome(),
        outcome_authority=authority,
    )

    async def scenario():
        await runtime.async_start()
        result = await runtime.async_tick()
        await runtime.async_stop()
        return result

    result = asyncio.run(scenario())
    assert authority.calls == 1
    assert completion.acknowledged == []
    assert result["state"] == "degraded"
    assert result["counts"]["acknowledged"] == 0


@pytest.mark.parametrize("mode", ["missing", "wrong_binding"])
def test_missing_or_wrong_exact_fact_publication_never_acknowledges(mode):
    desired = _desired()
    completion = _Completion([_lease(desired)])

    if mode == "missing":
        snapshots = _Snapshots(fact_healthy=False)
    else:
        class WrongBindingSnapshots(_Snapshots):
            async def async_publish_exact_fact(self, binding):
                receipt = await super().async_publish_exact_fact(binding)
                return replace(receipt, pipeline_item_id="completion:event-other")

        snapshots = WrongBindingSnapshots()
    runtime = _runtime(
        _DesiredStore(desired),
        completion,
        _Facts(),
        snapshots,
        lambda *_: _outcome(fact=True),
    )

    async def scenario():
        await runtime.async_start()
        result = await runtime.async_tick()
        await runtime.async_stop()
        return result

    result = asyncio.run(scenario())
    assert snapshots.fact_calls == 1
    assert completion.acknowledged == []
    assert result["state"] == "degraded"
    assert result["counts"]["acknowledged"] == 0


def test_processor_failure_leaves_completion_unacked_for_exact_retry():
    desired = _desired()
    first_lease = _lease(desired)
    retry_lease = replace(
        first_lease,
        lease_token="lease-2",
        attempt_count=2,
        lease_expires_at=NOW + 1200,
    )
    completion = _Completion([first_lease, retry_lease])
    attempts: list[str] = []

    def processor(lease, *_):
        attempts.append(lease.event_id)
        if len(attempts) == 1:
            raise RuntimeError("raw payload must not escape")
        return _outcome(label="retry-output")

    runtime = _runtime(
        _DesiredStore(desired), completion, _Facts(), _Snapshots(), processor,
    )

    async def scenario():
        await runtime.async_start()
        failed = await runtime.async_tick()
        assert not completion.acknowledged
        recovered = await runtime.async_tick()
        await runtime.async_stop()
        return failed, recovered

    failed, recovered = asyncio.run(scenario())
    assert failed["state"] == "degraded"
    assert failed["last_error_code"] == "pipeline_transition_failed"
    assert recovered["state"] == "running"
    assert attempts == ["event-1", "event-1"]
    assert completion.acknowledged == [("event-1", _sha("retry-output"))]


def test_noncooperative_processor_never_allows_overlapping_owner():
    desired = _desired()
    completion = _Completion([_lease(desired)])
    release = threading.Event()
    entered = threading.Event()

    def processor(*_):
        entered.set()
        release.wait(2.0)
        return _outcome()

    runtime = _runtime(
        _DesiredStore(desired), completion, _Facts(), _Snapshots(), processor,
        lease_seconds=0.2,
        processing_timeout_seconds=0.02,
        cancel_grace_seconds=0.02,
    )
    ownership = runtime._ownership

    async def scenario():
        await runtime.async_start()
        failed = await runtime.async_tick()
        assert entered.is_set()
        second_tick = await runtime.async_tick()
        assert second_tick["last_error_code"] == "processor_unresponsive"
        assert completion.claim_calls == 1
        assert runtime._cancel_event.is_set()
        started = time.monotonic()
        stopped = await runtime.async_stop()
        elapsed = time.monotonic() - started
        assert elapsed < 0.2
        assert failed["last_error_code"] == "pipeline_processor_timeout"
        assert stopped["state"] == "stopped_pending_processor"
        assert stopped["owns_heavy_lane"] is True
        assert stopped["ownership"]["fence_held"] is True
        assert ownership.release_calls == 0

        contender = _runtime(
            _DesiredStore(desired), _Completion(), _Facts(), _Snapshots(),
            lambda *_: _outcome(label="contender"),
        )
        with pytest.raises(WorkerFirstRuntimeOwnershipError):
            await contender.async_start()

        release.set()
        deadline = time.monotonic() + 1.0
        while runtime.diagnostics()["owns_heavy_lane"]:
            assert time.monotonic() < deadline
            await asyncio.sleep(0.005)
        assert runtime.diagnostics()["state"] == "stopped"
        assert runtime.diagnostics()["ownership"]["fence_held"] is False
        assert ownership.release_calls == 1

        successor = _runtime(
            _DesiredStore(desired), _Completion(), _Facts(), _Snapshots(),
            lambda *_: _outcome(label="successor"),
        )
        await successor.async_start()
        await successor.async_stop()

    asyncio.run(scenario())


def test_cancelled_mutable_store_work_retains_authority_until_future_finishes():
    desired = _desired()
    entered = threading.Event()
    release = threading.Event()

    class BlockingCompletion(_Completion):
        def audit_archive(self):
            self.log.append("completion.audit")
            entered.set()
            release.wait(2.0)
            return {"healthy": True}

    ownership = _FakeOwnershipLease()
    runtime = _runtime(
        _DesiredStore(desired),
        BlockingCompletion(),
        _Facts(),
        _Snapshots(),
        lambda *_: _outcome(),
        ownership_lease=ownership,
    )

    async def scenario():
        starter = asyncio.create_task(runtime.async_start())
        assert await asyncio.to_thread(entered.wait, 0.5)
        starter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await starter
        pending = runtime.diagnostics()
        assert pending["owns_heavy_lane"] is True
        assert pending["ownership"]["fence_held"] is True
        assert pending["ownership"]["mutable_work_count"] == 1
        assert ownership.release_calls == 0

        contender = _runtime(
            _DesiredStore(desired),
            _Completion(),
            _Facts(),
            _Snapshots(),
            lambda *_: _outcome(label="mutable-contender"),
        )
        with pytest.raises(WorkerFirstRuntimeOwnershipError):
            await contender.async_start()

        release.set()
        deadline = time.monotonic() + 1.0
        while runtime.diagnostics()["ownership"]["fence_held"]:
            assert time.monotonic() < deadline
            await asyncio.sleep(0.005)

    asyncio.run(scenario())
    assert ownership.release_calls == 1
    assert runtime.diagnostics()["ownership"]["mutable_work_count"] == 0


def test_snapshot_stop_failure_retains_authority_until_verified_retry():
    desired = _desired()

    class FlakyStopSnapshots(_Snapshots):
        def __init__(self):
            super().__init__()
            self.stop_attempts = 0

        async def async_stop(self):
            self.stop_attempts += 1
            if self.stop_attempts == 1:
                return {
                    "state": "degraded",
                    "last_error_code": "private transport failure",
                    "publication": {
                        "transport_state": "pending",
                        "dirty": True,
                    },
                }
            return await super().async_stop()

    snapshots = FlakyStopSnapshots()
    ownership = _FakeOwnershipLease()
    runtime = _runtime(
        _DesiredStore(desired),
        _Completion(),
        _Facts(),
        snapshots,
        lambda *_: _outcome(),
        ownership_lease=ownership,
    )

    async def scenario():
        await runtime.async_start()
        first = await runtime.async_stop()
        assert first["state"] == "stopped_pending_snapshot"
        assert first["ownership"]["fence_held"] is True
        assert ownership.release_calls == 0
        second = await runtime.async_stop()
        return second

    second = asyncio.run(scenario())
    assert second["state"] == "stopped"
    assert second["ownership"]["fence_held"] is False
    assert ownership.release_calls == 1


def test_dependency_diagnostics_and_start_exception_are_fixed_and_private():
    desired = _desired()

    class PrivateSnapshots(_Snapshots):
        def diagnostics(self) -> dict:
            return {
                "state": "C:/private/location.sqlite3",
                "last_error_code": "token=never-export-this",
            }

    private = _runtime(
        _DesiredStore(desired), _Completion(), _Facts(), PrivateSnapshots(),
        lambda *_: _outcome(),
    )
    diagnostics = private.diagnostics()
    encoded = json.dumps(diagnostics, sort_keys=True)
    assert diagnostics["snapshot"] == {
        "state": "unknown",
        "last_error_code": "snapshot_runtime_error",
    }
    assert "private" not in encoded
    assert "never-export" not in encoded
    asyncio.run(private.async_stop())

    class ExplodingCompletion(_Completion):
        def audit_archive(self):
            raise RuntimeError("C:/secret/token=do-not-export")

    failing = _runtime(
        _DesiredStore(desired), ExplodingCompletion(), _Facts(), _Snapshots(),
        lambda *_: _outcome(),
    )
    with pytest.raises(Exception) as caught:
        asyncio.run(failing.async_start())
    assert str(caught.value) == "runtime_start_failed"
    assert failing.diagnostics()["closed"] is True


def test_concurrent_ticks_are_serialized_through_one_heavy_lane():
    desired = _desired()
    completion = _Completion([
        _lease(desired, event="event-1", token="lease-1"),
        replace(
            _lease(desired, event="event-2", token="lease-2"),
            stream_sequence=2,
        ),
    ])
    first_entered = threading.Event()
    release_first = threading.Event()
    lock = threading.Lock()
    active = 0
    maximum = 0
    calls = 0

    def processor(lease, *_):
        nonlocal active, maximum, calls
        with lock:
            calls += 1
            active += 1
            maximum = max(maximum, active)
            current = calls
        try:
            if current == 1:
                first_entered.set()
                assert release_first.wait(1.0)
            return _outcome(label=f"output-{lease.event_id}")
        finally:
            with lock:
                active -= 1

    runtime = _runtime(
        _DesiredStore(desired), completion, _Facts(), _Snapshots(), processor,
    )

    async def scenario():
        await runtime.async_start()
        first = asyncio.create_task(runtime.async_tick())
        assert await asyncio.to_thread(first_entered.wait, 0.5)
        second = asyncio.create_task(runtime.async_tick())
        await asyncio.sleep(0)
        assert completion.claim_calls == 1
        release_first.set()
        await asyncio.gather(first, second)
        await runtime.async_stop()

    asyncio.run(scenario())
    assert maximum == 1
    assert calls == 2
    assert len(completion.acknowledged) == 2


def test_failed_snapshot_start_stops_partial_runtime_and_releases_owner():
    desired = _desired()
    snapshots = _Snapshots(start_healthy=False)
    runtime = _runtime(
        _DesiredStore(desired), _Completion(), _Facts(), snapshots,
        lambda *_: _outcome(),
    )

    async def scenario():
        with pytest.raises(Exception, match="snapshot_runtime_start_failed"):
            await runtime.async_start()
        status = runtime.diagnostics()
        assert status["closed"] is True
        assert status["owns_heavy_lane"] is False
        assert snapshots.start_calls == 1
        assert snapshots.stop_calls == 1

        successor = _runtime(
            _DesiredStore(desired), _Completion(), _Facts(), _Snapshots(),
            lambda *_: _outcome(label="successor"),
        )
        await successor.async_start()
        await successor.async_stop()

    asyncio.run(scenario())


def test_real_durable_stores_complete_raw_output_to_cursor_without_fact_claim(
    tmp_path,
):
    desired = _desired()
    desired_store = _DesiredStore(desired)
    completion = AcquisitionEvidenceLifecycleStore(
        (tmp_path / "completion").resolve(),
        clock=lambda: NOW,
        token_factory=lambda: "a" * 64,
        directory_fsync=lambda _path: None,
    )
    authority = desired_authority_proof(desired)
    producer = completion.publish(
        acquisition_job_id="dwd-job-1",
        consumer_id=CONSUMER,
        stream_id=STREAM,
        desired_state_sha256=desired.state_sha256,
        authority_sha256=authority["authority_sha256"],
        proof={"schema": "dwd-quantitative-handoff-test-v1"},
        payload=b"exact-dwd-raw",
    )
    facts = OperationalFactRevisionStore(
        tmp_path / "facts.sqlite3",
        clock=lambda: NOW,
    )
    snapshot_store = OperationalSnapshotStore(
        tmp_path / "snapshots.sqlite3",
        clock=lambda: NOW,
    )
    publication_store = PublicationOutboxStore(
        tmp_path / "publication.sqlite3",
        clock=lambda: NOW,
    )
    published: list[bytes] = []
    snapshots = OperationalSnapshotRuntime(
        publisher=published.append,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        desired_state_store=desired_store,
        snapshot_store=snapshot_store,
        outbox_store=publication_store,
        fact_revision_store=facts,
        clock=lambda: NOW,
    )

    def processor(lease, active, cancel_event):
        assert lease.event_id == producer.event_id
        assert lease.payload == b"exact-dwd-raw"
        assert active == desired
        assert not cancel_event.is_set()
        return PipelineOutcome(
            output_sha256=_sha("real-durable-output"),
            disposition="withheld",
            fact_revision_staged=False,
            process_receipt_sha256=_sha("real-process-receipt"),
        )

    runtime = _runtime(
        desired_store,
        completion,
        facts,
        snapshots,
        processor,
        lease_seconds=60.0,
        processing_timeout_seconds=30.0,
        cancel_grace_seconds=5.0,
    )

    async def scenario():
        await runtime.async_start()
        result = await runtime.async_tick()
        await runtime.async_stop()
        return result

    result = asyncio.run(scenario())
    assert result["counts"]["acknowledged"] == 1
    assert result["counts"]["fact_triggers"] == 0
    assert len(published) == 1
    assert decode_control_envelope(published[0])["snapshot"]["revision"] == 1
    assert snapshot_store.head()["revision"] == 1
    assert publication_store.load().state == "published"
    assert facts.audit() == {
        "contract": "stv3-operational-fact-store-v1",
        "snapshot_committed": 0,
        "ready": 0,
        "retained_rows": 0,
        "status": "ok",
        "superseded": 0,
    }
    assert completion.claim_next(
        CONSUMER,
        STREAM,
        "postcondition-owner",
        lease_seconds=60.0,
        now=NOW + 1.0,
    ) is None
