from __future__ import annotations

import asyncio
from contextlib import closing
import hashlib
import importlib.util
import sqlite3
import sys
from pathlib import Path
from typing import Callable

import pytest

from storm_tracker_worker.acquisition_evidence_lifecycle import (
    AcquisitionEvidenceLifecycleStore,
    desired_authority_proof,
)
from storm_tracker_worker.desired_state import DesiredState, DesiredStateStore
from storm_tracker_worker.operational_fact_revision import (
    AdmissionDisposition,
    CommittedOperationalFactRevision,
    FactAdmissionReceipt,
    OperationalFactKind,
)
from storm_tracker_worker.operational_fact_store import (
    OperationalFactRevisionStore,
)
from storm_tracker_worker.operational_snapshot_builder import (
    build_unknown_operational_snapshot,
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
    canonical_json_bytes,
    decode_control_envelope,
)
from storm_tracker_worker.worker_first_ownership import REQUIRED_STORE_ROLES
from storm_tracker_worker.worker_first_runtime import (
    FactPublicationBinding,
    PipelineOutcome,
    VerifiedPipelineOutcome,
    WorkerFirstRuntimeCoordinator,
)


NOW = 1_800_500_000.0
CONSUMER = "dwd-rv-quantitative-decoder-v1"
STREAM = "radar.dwd.radolan_rv"


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode("ascii")).hexdigest()


def _desired_document(
    *,
    authority_epoch: int,
    generation: int,
    previous: str | None,
) -> dict:
    return {
        "contract": "stv3-worker-desired-state-v1",
        "producer_id": "ha:linearizability-test",
        "authority_epoch": authority_epoch,
        "generation": generation,
        "issued_at": NOW + generation,
        "previous_state_sha256": previous,
        "targets": [{
            "target_id": "home",
            "display_name": "Thuis",
            "kind": "home",
            "available": True,
            "latitude": 50.99 + generation / 1000.0,
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
    }


def _configured_desired(path: Path) -> DesiredStateStore:
    store = DesiredStateStore(path)
    claim = store.claim_producer(
        "ha:linearizability-test",
        "linearizability-test-claim",
        claimed_at=NOW,
    )
    store.apply(
        _desired_document(
            authority_epoch=claim["authority_epoch"],
            generation=1,
            previous=None,
        ),
        received_at=NOW + 1.0,
    )
    return store


def _apply_d2(store: DesiredStateStore) -> DesiredState:
    active = store.load_active()
    assert active is not None
    return store.apply(
        _desired_document(
            authority_epoch=active.authority_epoch,
            generation=active.generation + 1,
            previous=active.state_sha256,
        ),
        received_at=NOW + 10.0,
    )


class _SwitchableDesiredView:
    """Expose a real desired store while permitting an unavailable boundary."""

    def __init__(self, store: DesiredStateStore) -> None:
        self.store = store
        self.available = True

    def load_active(self) -> DesiredState | None:
        return self.store.load_active() if self.available else None


class _AckBoundaryLifecycle:
    """Run exactly one transition inside, not adjacent to, acknowledge()."""

    def __init__(self, lifecycle, transition: Callable[[], None]) -> None:
        self._lifecycle = lifecycle
        self._transition = transition
        self.transition_calls = 0

    def __getattr__(self, name):
        return getattr(self._lifecycle, name)

    def acknowledge(self, *args, **kwargs):
        if self.transition_calls == 0:
            self.transition_calls += 1
            self._transition()
        return self._lifecycle.acknowledge(*args, **kwargs)


class _OwnershipLease:
    """Small ownership double; every lifecycle/store below it is real."""

    def __init__(self, stores) -> None:
        self.authority_sha256 = _sha("linearizability-owner")
        self.authority_scope_sha256 = _sha("linearizability-owner-scope")
        self.consumer_id = CONSUMER
        self.stream_id = STREAM
        self.stores = dict(stores)
        self.held = True

    def assert_held(self) -> None:
        if not self.held:
            raise RuntimeError("ownership lost")

    def assert_coordinator_binding(self, *, consumer_id, stream_id, stores) -> None:
        self.assert_held()
        assert consumer_id == self.consumer_id
        assert stream_id == self.stream_id
        assert dict(stores) == self.stores

    def release(self) -> None:
        self.assert_held()
        self.held = False


def _store_bindings(
    tmp_path: Path,
    *,
    completion: AcquisitionEvidenceLifecycleStore,
    desired: DesiredStateStore,
    facts: OperationalFactRevisionStore,
    snapshots: OperationalSnapshotStore,
    outbox: PublicationOutboxStore,
) -> dict[str, tuple[str, str]]:
    pipeline_output = (tmp_path / "pipeline-output.sqlite3").resolve()
    pipeline_output.touch(exist_ok=True)
    paths = {
        "acquisition_completion_store": completion.root.resolve(),
        "desired_state_store": desired.path.resolve(),
        "operational_fact_store": facts.path.resolve(),
        "operational_snapshot_store": snapshots.path.resolve(),
        "pipeline_output_store": pipeline_output,
        "snapshot_outbox_store": outbox.path.resolve(),
    }
    assert set(paths) == set(REQUIRED_STORE_ROLES)
    return {
        role: (str(path), _sha(f"linearizability-store:{role}"))
        for role, path in paths.items()
    }


def _stage_admitted_fact(
    store: OperationalFactRevisionStore,
    desired: DesiredState,
    *,
    pipeline_item_id: str,
    snapshot_revision: int,
    process_receipt_sha256: str,
) -> FactPublicationBinding:
    snapshot = build_unknown_operational_snapshot(
        desired,
        revision=snapshot_revision,
        evaluated_at=NOW,
        worker_state="online",
        queue_depth=0,
        failed_jobs=0,
    )
    provider = {
        "display_name": "DWD RADOLAN RV",
        "error": None,
        "freshness": "current",
        "product_valid_at": NOW - 60.0,
        "provider_id": "dwd",
        "reason_codes": ["quantitative_observation_admitted"],
        "received_at": NOW - 30.0,
        "state": "operational",
    }
    region = {
        "display_name": "Regio thuis",
        "provider_ids": ["dwd"],
        "region_id": "region:home",
        "state": "current",
        "target_ids": ["home"],
    }
    snapshot["product_valid_at"] = NOW - 60.0
    snapshot["received_at"] = NOW - 30.0
    snapshot["providers"]["items"] = [provider]
    snapshot["regions"]["items"] = [region]
    snapshot["targets"]["items"][0]["region_id"] = "region:home"
    source = _sha("source-receipt")
    certificate = _sha("grid-certificate")
    receipts = tuple(sorted((
        FactAdmissionReceipt.create(
            fact_kind=OperationalFactKind.PROVIDER_CURRENT,
            scope_id="dwd",
            fact_value=provider,
            desired_state_sha256=desired.state_sha256,
            authority_epoch=desired.authority_epoch,
            source_receipt_sha256s=(source,),
            certificate_sha256s=(certificate,),
            process_receipt_sha256=process_receipt_sha256,
            full_scan_complete=False,
            external_truth_validated=False,
            decided_at=NOW,
            valid_until=NOW + 300.0,
            disposition=AdmissionDisposition.ADMITTED,
            reason_codes=("quantitative_observation_admitted",),
        ),
        FactAdmissionReceipt.create(
            fact_kind=OperationalFactKind.REGION_CURRENT,
            scope_id="region:home",
            fact_value=region,
            desired_state_sha256=desired.state_sha256,
            authority_epoch=desired.authority_epoch,
            source_receipt_sha256s=(source,),
            certificate_sha256s=(certificate,),
            process_receipt_sha256=process_receipt_sha256,
            full_scan_complete=False,
            external_truth_validated=False,
            decided_at=NOW,
            valid_until=NOW + 300.0,
            disposition=AdmissionDisposition.ADMITTED,
            reason_codes=("quantitative_observation_admitted",),
        ),
    ), key=lambda item: item.receipt_sha256))
    revision = CommittedOperationalFactRevision.create(
        desired_state_sha256=desired.state_sha256,
        authority_epoch=desired.authority_epoch,
        snapshot=snapshot,
        admission_receipts=receipts,
    )
    store.stage(
        pipeline_item_id,
        desired_state=desired,
        revision=revision,
    )
    return FactPublicationBinding(
        pipeline_item_id=pipeline_item_id,
        revision_sha256=revision.revision_sha256,
        snapshot_sha256=hashlib.sha256(
            canonical_json_bytes(revision.snapshot)
        ).hexdigest(),
        process_receipt_sha256=process_receipt_sha256,
        desired_state_sha256=desired.state_sha256,
        authority_epoch=desired.authority_epoch,
        snapshot_revision=snapshot_revision,
    )


class _PipelineHarness:
    def __init__(
        self,
        facts: OperationalFactRevisionStore,
        snapshots: OperationalSnapshotStore,
        *,
        fact_events: set[str],
    ) -> None:
        self.facts = facts
        self.snapshots = snapshots
        self.fact_events = set(fact_events)
        self.calls: list[str] = []
        self._records: dict[str, tuple[PipelineOutcome, FactPublicationBinding | None]] = {}

    def __call__(self, lease, desired_state, cancel_event) -> PipelineOutcome:
        assert not cancel_event.is_set()
        self.calls.append(lease.event_id)
        process_sha = _sha(f"process:{lease.event_id}")
        binding = None
        if lease.event_id in self.fact_events:
            head = self.snapshots.head()
            assert head is not None
            binding = _stage_admitted_fact(
                self.facts,
                desired_state,
                pipeline_item_id=f"completion:{lease.event_id}",
                snapshot_revision=int(head["revision"]) + 1,
                process_receipt_sha256=process_sha,
            )
        outcome = PipelineOutcome(
            output_sha256=_sha(f"output:{lease.event_id}"),
            disposition="advanced",
            fact_revision_staged=binding is not None,
            process_receipt_sha256=process_sha,
        )
        self._records[lease.event_id] = (outcome, binding)
        return outcome

    def verify(self, *, lease, desired_state, outcome) -> VerifiedPipelineOutcome:
        recorded, binding = self._records[lease.event_id]
        assert outcome == recorded
        if binding is not None:
            fact = self.facts.load(binding.pipeline_item_id)
            assert fact.revision_sha256 == binding.revision_sha256
            assert fact.snapshot_sha256 == binding.snapshot_sha256
            assert fact.state == "ready"
        return VerifiedPipelineOutcome(
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
            output_receipt_sha256=_sha(f"output-receipt:{lease.event_id}"),
            process_receipt_sha256=outcome.process_receipt_sha256,
            disposition=outcome.disposition,
            verifier_profile_sha256=_sha("linearizability-verifier-profile"),
            fact_binding=binding,
        )


def _completion(
    root: Path,
    *,
    fault_injector: Callable[[str], None] | None = None,
) -> AcquisitionEvidenceLifecycleStore:
    return AcquisitionEvidenceLifecycleStore(
        root.resolve(),
        clock=lambda: NOW,
        token_factory=lambda: "a" * 64,
        fault_injector=fault_injector,
        directory_fsync=lambda _path: None,
    )


def _publish(
    completion: AcquisitionEvidenceLifecycleStore,
    desired: DesiredState,
    *,
    job: str,
):
    return completion.publish(
        acquisition_job_id=job,
        consumer_id=CONSUMER,
        stream_id=STREAM,
        desired_state_sha256=desired.state_sha256,
        authority_sha256=desired_authority_proof(desired)["authority_sha256"],
        proof={"schema": "linearizability-test-handoff-v1"},
        payload=f"raw:{job}".encode("ascii"),
    )


def _runtime_parts(
    tmp_path: Path,
    *,
    desired_store: DesiredStateStore | None = None,
    desired_view: _SwitchableDesiredView | None = None,
    completion: AcquisitionEvidenceLifecycleStore | None = None,
    completion_lifecycle=None,
    facts: OperationalFactRevisionStore | None = None,
    snapshots: OperationalSnapshotStore | None = None,
    outbox: PublicationOutboxStore | None = None,
    published: list[bytes] | None = None,
    fact_events: set[str] | None = None,
):
    desired_store = desired_store or _configured_desired(
        tmp_path / "desired.sqlite3"
    )
    desired_view = desired_view or _SwitchableDesiredView(desired_store)
    completion = completion or _completion(tmp_path / "completion")
    facts = facts or OperationalFactRevisionStore(
        tmp_path / "facts.sqlite3", clock=lambda: NOW
    )
    snapshots = snapshots or OperationalSnapshotStore(
        tmp_path / "snapshots.sqlite3", clock=lambda: NOW
    )
    outbox = outbox or PublicationOutboxStore(
        tmp_path / "publication.sqlite3", clock=lambda: NOW
    )
    published = [] if published is None else published
    snapshot_runtime = OperationalSnapshotRuntime(
        publisher=published.append,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        desired_state_store=desired_view,
        snapshot_store=snapshots,
        outbox_store=outbox,
        fact_revision_store=facts,
        clock=lambda: NOW,
    )
    harness = _PipelineHarness(
        facts,
        snapshots,
        fact_events=set() if fact_events is None else fact_events,
    )
    bindings = _store_bindings(
        tmp_path,
        completion=completion,
        desired=desired_store,
        facts=facts,
        snapshots=snapshots,
        outbox=outbox,
    )
    runtime = WorkerFirstRuntimeCoordinator(
        desired_state_store=desired_view,
        completion_lifecycle=(
            completion if completion_lifecycle is None else completion_lifecycle
        ),
        fact_revision_lifecycle=facts,
        snapshot_lifecycle=snapshot_runtime,
        processor=harness,
        outcome_authority=harness,
        ownership_lease=_OwnershipLease(bindings),
        store_bindings=bindings,
        consumer_id=CONSUMER,
        stream_id=STREAM,
        lease_seconds=60.0,
        processing_timeout_seconds=30.0,
        cancel_grace_seconds=5.0,
        clock=lambda: NOW,
    )
    return {
        "desired_store": desired_store,
        "desired_view": desired_view,
        "completion": completion,
        "facts": facts,
        "snapshots": snapshots,
        "outbox": outbox,
        "published": published,
        "snapshot_runtime": snapshot_runtime,
        "harness": harness,
        "runtime": runtime,
    }


def _database_truth(completion: AcquisitionEvidenceLifecycleStore) -> dict:
    with closing(sqlite3.connect(completion.path)) as db:
        cursor = db.execute(
            "SELECT last_sequence FROM consumer_cursors "
            "WHERE consumer_id=? AND stream_id=?",
            (CONSUMER, STREAM),
        ).fetchone()
        return {
            "receipts": db.execute(
                "SELECT count(*) FROM consumption_receipts"
            ).fetchone()[0],
            "cursor": None if cursor is None else cursor[0],
            "states": dict(db.execute(
                "SELECT state,count(*) FROM completion_outbox GROUP BY state"
            ).fetchall()),
        }


def _transition(boundary: str, desired_view: _SwitchableDesiredView) -> None:
    if boundary == "d2":
        _apply_d2(desired_view.store)
    else:
        assert boundary == "none"
        desired_view.available = False


@pytest.mark.parametrize("boundary", ["d2", "none"])
def test_exact_fact_completion_linearizes_at_old_event_receipt_when_desired_changes_inside_ack(
    tmp_path,
    boundary,
):
    parts = _runtime_parts(tmp_path, fact_events=set())
    d1 = parts["desired_store"].load_active()
    assert d1 is not None
    producer = _publish(parts["completion"], d1, job=f"exact-{boundary}")
    parts["harness"].fact_events.add(producer.event_id)
    boundary_lifecycle = _AckBoundaryLifecycle(
        parts["completion"],
        lambda: _transition(boundary, parts["desired_view"]),
    )
    parts = _runtime_parts(
        tmp_path,
        desired_store=parts["desired_store"],
        desired_view=parts["desired_view"],
        completion=parts["completion"],
        completion_lifecycle=boundary_lifecycle,
        facts=parts["facts"],
        snapshots=parts["snapshots"],
        outbox=parts["outbox"],
        published=parts["published"],
        fact_events={producer.event_id},
    )

    async def scenario():
        await parts["runtime"].async_start()
        result = await parts["runtime"].async_tick()
        await parts["runtime"].async_stop()
        return result

    result = asyncio.run(scenario())
    envelopes = [decode_control_envelope(item) for item in parts["published"]]
    assert boundary_lifecycle.transition_calls == 1
    assert result["counts"]["acknowledged"] == 1
    assert _database_truth(parts["completion"]) == {
        "receipts": 1,
        "cursor": 1,
        "states": {"consumed": 1},
    }
    assert [item["snapshot"]["desired_state_sha256"] for item in envelopes] == [
        d1.state_sha256,
        d1.state_sha256,
    ]
    assert parts["outbox"].load().desired_state_sha256 == d1.state_sha256
    if boundary == "d2":
        d2 = parts["desired_store"].load_active()
        assert d2 is not None and d2.state_sha256 != d1.state_sha256
    else:
        assert parts["desired_view"].load_active() is None


def test_ack_receipt_commit_crash_restarts_and_reconciles_cursor_without_redelivery(
    tmp_path,
):
    fired = False

    def fault(point: str) -> None:
        nonlocal fired
        if point == "ack.after_receipt_commit" and not fired:
            fired = True
            raise RuntimeError("simulated power loss after receipt commit")

    completion = _completion(tmp_path / "completion", fault_injector=fault)
    parts = _runtime_parts(tmp_path, completion=completion)
    d1 = parts["desired_store"].load_active()
    assert d1 is not None
    producer = _publish(completion, d1, job="crash-d1")
    boundary = _AckBoundaryLifecycle(
        completion,
        lambda: _apply_d2(parts["desired_store"]),
    )
    parts = _runtime_parts(
        tmp_path,
        desired_store=parts["desired_store"],
        desired_view=parts["desired_view"],
        completion=completion,
        completion_lifecycle=boundary,
        facts=parts["facts"],
        snapshots=parts["snapshots"],
        outbox=parts["outbox"],
        published=parts["published"],
        fact_events={producer.event_id},
    )

    async def first_process():
        await parts["runtime"].async_start()
        result = await parts["runtime"].async_tick()
        await parts["runtime"].async_stop()
        return result

    failed = asyncio.run(first_process())
    assert fired is True
    assert failed["last_error_code"] == "pipeline_transition_failed"
    assert _database_truth(completion) == {
        "receipts": 1,
        "cursor": 0,
        "states": {"leased": 1},
    }

    reopened_desired = DesiredStateStore(parts["desired_store"].path)
    reopened_completion = _completion(tmp_path / "completion")
    reopened_facts = OperationalFactRevisionStore(
        parts["facts"].path, clock=lambda: NOW
    )
    reopened_snapshots = OperationalSnapshotStore(
        parts["snapshots"].path, clock=lambda: NOW
    )
    reopened_outbox = PublicationOutboxStore(
        parts["outbox"].path, clock=lambda: NOW
    )
    restarted = _runtime_parts(
        tmp_path,
        desired_store=reopened_desired,
        desired_view=_SwitchableDesiredView(reopened_desired),
        completion=reopened_completion,
        facts=reopened_facts,
        snapshots=reopened_snapshots,
        outbox=reopened_outbox,
        published=parts["published"],
        fact_events={producer.event_id},
    )

    async def restart():
        await restarted["runtime"].async_start()
        result = await restarted["runtime"].async_tick()
        await restarted["runtime"].async_stop()
        return result

    recovered = asyncio.run(restart())
    assert recovered["counts"]["leases"] == 0
    assert restarted["harness"].calls == []
    assert _database_truth(reopened_completion) == {
        "receipts": 1,
        "cursor": 1,
        "states": {"consumed": 1},
    }
    published = [decode_control_envelope(item) for item in parts["published"]]
    d1_fact = [
        item for item in published
        if item["snapshot"]["desired_state_sha256"] == d1.state_sha256
        and item["snapshot"]["providers"]["items"]
    ]
    assert len(d1_fact) == 1


def _load_ha_snapshot_contract():
    path = (
        Path(__file__).resolve().parents[1]
        / "custom_components"
        / "storm_tracker_v3"
        / "operational_snapshot.py"
    )
    module_name = "_stv3_linearizable_ha_operational_snapshot"
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_ha_cache_delivery_orders_never_restore_old_desired_authority(tmp_path):
    parts = _runtime_parts(tmp_path)
    d1 = parts["desired_store"].load_active()
    assert d1 is not None

    async def publish_two_desired_states():
        await parts["snapshot_runtime"].async_start()
        _apply_d2(parts["desired_store"])
        await parts["snapshot_runtime"].async_tick()
        await parts["snapshot_runtime"].async_stop()

    asyncio.run(publish_two_desired_states())
    assert len(parts["published"]) == 2
    d2 = parts["desired_store"].load_active()
    assert d2 is not None
    old_payload, new_payload = parts["published"]
    ha = _load_ha_snapshot_contract()

    installed_then_late = ha.OperationalSnapshotCache()
    installed_then_late.apply_bytes(
        old_payload,
        expected_desired_state_sha256=d1.state_sha256,
        receiver_now=NOW,
    )
    installed_then_late.mark_stale(reason="desired_state_transition")
    with pytest.raises(ha.SnapshotRevisionConflict):
        installed_then_late.apply_bytes(
            old_payload,
            expected_desired_state_sha256=d2.state_sha256,
            receiver_now=NOW,
        )
    assert installed_then_late.availability == "stale"
    installed_then_late.apply_bytes(
        new_payload,
        expected_desired_state_sha256=d2.state_sha256,
        receiver_now=NOW,
    )
    assert installed_then_late.get_status()["desired_state_sha256"] == d2.state_sha256
    assert installed_then_late.availability == "current"

    transition_before_first_delivery = ha.OperationalSnapshotCache()
    with pytest.raises(ha.SnapshotRevisionConflict):
        transition_before_first_delivery.apply_bytes(
            old_payload,
            expected_desired_state_sha256=d2.state_sha256,
            receiver_now=NOW,
        )
    assert transition_before_first_delivery.revision == 0

    new_then_old = ha.OperationalSnapshotCache()
    new_then_old.apply_bytes(
        new_payload,
        expected_desired_state_sha256=d2.state_sha256,
        receiver_now=NOW,
    )
    with pytest.raises(ha.SnapshotRevisionConflict):
        new_then_old.apply_bytes(
            old_payload,
            expected_desired_state_sha256=d2.state_sha256,
            receiver_now=NOW,
        )
    assert new_then_old.get_status()["desired_state_sha256"] == d2.state_sha256
    assert new_then_old.availability == "current"


@pytest.mark.parametrize("boundary", ["d2", "none"])
def test_factless_completion_has_identical_ack_boundary_semantics(
    tmp_path,
    boundary,
):
    parts = _runtime_parts(tmp_path)
    d1 = parts["desired_store"].load_active()
    assert d1 is not None
    producer = _publish(parts["completion"], d1, job=f"factless-{boundary}")
    ack_boundary = _AckBoundaryLifecycle(
        parts["completion"],
        lambda: _transition(boundary, parts["desired_view"]),
    )
    parts = _runtime_parts(
        tmp_path,
        desired_store=parts["desired_store"],
        desired_view=parts["desired_view"],
        completion=parts["completion"],
        completion_lifecycle=ack_boundary,
        facts=parts["facts"],
        snapshots=parts["snapshots"],
        outbox=parts["outbox"],
        published=parts["published"],
        fact_events=set(),
    )

    async def scenario():
        await parts["runtime"].async_start()
        first = await parts["runtime"].async_tick()
        second = await parts["runtime"].async_tick()
        await parts["runtime"].async_stop()
        return first, second

    first, second = asyncio.run(scenario())
    assert first["counts"]["acknowledged"] == 1
    assert first["counts"]["fact_triggers"] == 0
    assert _database_truth(parts["completion"])["cursor"] == 1
    assert parts["facts"].audit()["retained_rows"] == 0
    if boundary == "d2":
        assert second["state"] == "running"
        assert parts["outbox"].load().desired_state_sha256 != d1.state_sha256
    else:
        assert second["state"] == "unconfigured"
        assert parts["desired_view"].load_active() is None


def test_clean_d2_completion_converges_after_old_d1_ack_boundary(tmp_path):
    parts = _runtime_parts(tmp_path)
    d1 = parts["desired_store"].load_active()
    assert d1 is not None
    first = _publish(parts["completion"], d1, job="sequence-d1")
    boundary = _AckBoundaryLifecycle(
        parts["completion"],
        lambda: _apply_d2(parts["desired_store"]),
    )
    parts = _runtime_parts(
        tmp_path,
        desired_store=parts["desired_store"],
        desired_view=parts["desired_view"],
        completion=parts["completion"],
        completion_lifecycle=boundary,
        facts=parts["facts"],
        snapshots=parts["snapshots"],
        outbox=parts["outbox"],
        published=parts["published"],
        fact_events={first.event_id},
    )

    async def scenario():
        await parts["runtime"].async_start()
        first_result = await parts["runtime"].async_tick()
        d2 = parts["desired_store"].load_active()
        assert d2 is not None and d2.state_sha256 != d1.state_sha256
        second = _publish(parts["completion"], d2, job="sequence-d2")
        parts["harness"].fact_events.add(second.event_id)
        second_result = await parts["runtime"].async_tick()
        await parts["runtime"].async_stop()
        return first_result, second_result, d2

    first_result, second_result, d2 = asyncio.run(scenario())
    assert first_result["counts"]["acknowledged"] == 1
    assert second_result["counts"]["acknowledged"] == 2
    assert _database_truth(parts["completion"]) == {
        "receipts": 2,
        "cursor": 2,
        "states": {"consumed": 2},
    }
    envelopes = [decode_control_envelope(item) for item in parts["published"]]
    authorities = [item["snapshot"]["desired_state_sha256"] for item in envelopes]
    assert authorities == [
        d1.state_sha256,
        d1.state_sha256,
        d2.state_sha256,
        d2.state_sha256,
    ]
    first_d2 = authorities.index(d2.state_sha256)
    assert d1.state_sha256 not in authorities[first_d2:]
    assert envelopes[-1]["snapshot"]["providers"]["items"][0][
        "provider_id"
    ] == "dwd"
    assert parts["outbox"].load().desired_state_sha256 == d2.state_sha256
