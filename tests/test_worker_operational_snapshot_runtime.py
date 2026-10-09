from __future__ import annotations

import ast
import asyncio
from dataclasses import replace
import hashlib
import json
from pathlib import Path

import pytest

from storm_tracker_worker.desired_state import DesiredStateStore
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
    UNKNOWN_SNAPSHOT_TTL_SECONDS,
    build_unknown_operational_snapshot,
)
from storm_tracker_worker.operational_snapshot_publication import (
    PublicationOutboxStore,
)
from storm_tracker_worker.operational_snapshot_runtime import (
    MAX_DIAGNOSTIC_BYTES,
    OperationalSnapshotRuntime,
    OperationalSnapshotRuntimeError,
    SNAPSHOT_REFRESH_MARGIN_SECONDS,
    TransportOutbox,
)
from storm_tracker_worker.operational_snapshot_store import (
    OperationalSnapshotStore,
)
from storm_tracker_worker.tracking_core.operational_snapshot import (
    canonical_json_bytes,
    decode_control_envelope,
)
from storm_tracker_worker.worker_first_runtime import (
    FactPublicationBinding,
)


NOW = 1_800_200_000.0
MODULE = (
    Path(__file__).resolve().parents[1]
    / "storm_tracker_worker"
    / "operational_snapshot_runtime.py"
)


def _desired_document(
    *,
    authority_epoch: int,
    generation: int,
    previous: str | None,
    latitude: float = 50.8,
) -> dict:
    return {
        "contract": "stv3-worker-desired-state-v1",
        "producer_id": "ha:runtime-test",
        "authority_epoch": authority_epoch,
        "generation": generation,
        "issued_at": NOW + generation,
        "previous_state_sha256": previous,
        "targets": [{
            "target_id": "home",
            "display_name": "Thuis",
            "kind": "home",
            "available": True,
            "latitude": latitude,
            "longitude": 4.2,
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


def _configured_desired_store(path: Path) -> DesiredStateStore:
    store = DesiredStateStore(path)
    claim = store.claim_producer(
        "ha:runtime-test",
        "runtime-test-claim",
        claimed_at=NOW,
    )
    store.apply(
        _desired_document(
            authority_epoch=claim["authority_epoch"],
            generation=1,
            previous=None,
        ),
        received_at=NOW + 1,
    )
    return store


def _change_desired_state(store: DesiredStateStore) -> None:
    active = store.load_active()
    assert active is not None
    store.apply(
        _desired_document(
            authority_epoch=active.authority_epoch,
            generation=active.generation + 1,
            previous=active.state_sha256,
            latitude=50.81,
        ),
        received_at=NOW + 10,
    )


def _stores(tmp_path: Path):
    return (
        OperationalSnapshotStore(
            tmp_path / "snapshots.sqlite3",
            clock=lambda: NOW,
        ),
        PublicationOutboxStore(
            tmp_path / "publication.sqlite3",
            clock=lambda: NOW,
        ),
    )


def _stage_fact(
    store: OperationalFactRevisionStore,
    desired_store: DesiredStateStore,
    *,
    pipeline_item_id: str,
    snapshot_revision: int,
    queue_depth: int = 0,
):
    desired = desired_store.load_active()
    assert desired is not None
    snapshot = build_unknown_operational_snapshot(
        desired,
        revision=snapshot_revision,
        evaluated_at=NOW,
        worker_state="online",
        queue_depth=queue_depth,
        failed_jobs=0,
    )
    revision = CommittedOperationalFactRevision.create(
        desired_state_sha256=desired.state_sha256,
        authority_epoch=desired.authority_epoch,
        snapshot=snapshot,
        admission_receipts=(),
    )
    return store.stage(
        pipeline_item_id,
        desired_state=desired,
        revision=revision,
    )


def _stage_admitted_provider_fact(
    store: OperationalFactRevisionStore,
    desired_store: DesiredStateStore,
    *,
    pipeline_item_id: str,
    snapshot_revision: int,
    process_receipt_sha256: str,
):
    desired = desired_store.load_active()
    assert desired is not None
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
    source = "1" * 64
    certificate = "2" * 64
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
    record = store.stage(
        pipeline_item_id,
        desired_state=desired,
        revision=revision,
    )
    binding = FactPublicationBinding(
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
    return record, binding


def _runtime(
    tmp_path: Path,
    *,
    desired_store: DesiredStateStore,
    status: dict,
    publisher,
    snapshot_store=None,
    outbox_store=None,
    fact_revision_store=None,
    clock=None,
) -> tuple[
    OperationalSnapshotRuntime,
    OperationalSnapshotStore,
    PublicationOutboxStore,
]:
    default_snapshot, default_outbox = _stores(tmp_path)
    snapshot = default_snapshot if snapshot_store is None else snapshot_store
    outbox = default_outbox if outbox_store is None else outbox_store
    runtime_clock = (lambda: NOW) if clock is None else clock
    runtime = OperationalSnapshotRuntime(
        publisher=publisher,
        status_supplier=lambda: dict(status),
        desired_state_store=desired_store,
        snapshot_store=snapshot,
        outbox_store=outbox,
        fact_revision_store=fact_revision_store,
        clock=runtime_clock,
    )
    return runtime, snapshot, outbox


def test_module_has_no_network_provider_service_or_home_assistant_imports():
    tree = ast.parse(MODULE.read_text(encoding="utf-8"))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add(node.module or "")

    forbidden = {
        "aiohttp",
        "homeassistant",
        "paho",
        "requests",
        "urllib",
        "storm_tracker_worker.service",
    }
    assert not any(
        name in forbidden
        or ".providers" in name
        or name.endswith(".service")
        for name in imported
    )


def test_unconfigured_start_is_fail_closed_and_never_calls_status_or_publisher(
    tmp_path,
):
    calls = {"status": 0, "publisher": 0}

    def status_supplier():
        calls["status"] += 1
        return {"state": "online", "queue_depth": 0, "failed_jobs": 0}

    def publisher(_payload):
        calls["publisher"] += 1

    desired = DesiredStateStore(tmp_path / "desired.sqlite3")
    snapshot, outbox = _stores(tmp_path)
    runtime = OperationalSnapshotRuntime(
        publisher=publisher,
        status_supplier=status_supplier,
        desired_state_store=desired,
        snapshot_store=snapshot,
        outbox_store=outbox,
        clock=lambda: NOW,
    )

    status = asyncio.run(runtime.async_start())

    assert status["state"] == "unconfigured"
    assert status["desired_state_configured"] is False
    assert calls == {"status": 0, "publisher": 0}
    assert snapshot.head() is None
    assert outbox.load() is None


def test_initial_unknown_publication_and_identical_ticks_are_deduplicated(
    tmp_path,
):
    desired = _configured_desired_store(tmp_path / "desired.sqlite3")
    status = {
        "state": "online",
        "queue_depth": 2,
        "failed_jobs": 1,
        "token": "must-never-leave-the-supplier",
        "latitude": 51.0,
    }
    published: list[bytes] = []
    runtime, snapshots, _ = _runtime(
        tmp_path,
        desired_store=desired,
        status=status,
        publisher=published.append,
    )

    async def scenario():
        await runtime.async_start()
        await asyncio.gather(*(runtime.async_tick() for _ in range(20)))

    asyncio.run(scenario())

    assert len(published) == 1
    head = snapshots.head()
    assert head["revision"] == 1
    envelope = decode_control_envelope(published[0])
    assert envelope["snapshot"]["worker"] == {
        "state": "online",
        "queue_depth": 2,
        "failed_jobs": 1,
        "status_text": "",
    }
    assert envelope["snapshot"]["targets"]["items"][0][
        "current_precipitation"
    ]["state"] == "unknown"
    encoded = json.dumps(runtime.diagnostics(), sort_keys=True)
    assert "token" not in encoded
    assert "latitude" not in encoded
    assert "home" not in encoded
    assert len(encoded.encode("utf-8")) <= MAX_DIAGNOSTIC_BYTES
    assert runtime.diagnostics()["counts"]["unchanged_ticks"] == 20


def test_start_prefers_exact_ready_fact_and_reconciles_before_transport(tmp_path):
    desired = _configured_desired_store(tmp_path / "desired.sqlite3")
    facts = OperationalFactRevisionStore(
        tmp_path / "facts.sqlite3",
        clock=lambda: NOW,
    )
    staged = _stage_fact(
        facts,
        desired,
        pipeline_item_id="pipeline:start-ready",
        snapshot_revision=1,
        queue_depth=77,
    )
    seen = []
    runtime, snapshots, _ = _runtime(
        tmp_path,
        desired_store=desired,
        status={"state": "online", "queue_depth": 2, "failed_jobs": 0},
        publisher=seen.append,
        fact_revision_store=facts,
    )

    result = asyncio.run(runtime.async_start())

    assert result["state"] == "running"
    assert result["counts"]["fact_publications"] == 1
    assert snapshots.head()["revision"] == 1
    assert snapshots.head()["snapshot"]["worker"]["queue_depth"] == 77
    assert facts.load(staged.pipeline_item_id).state == "snapshot_committed"
    assert seen == [snapshots.head()["canonical_envelope"]]


def test_new_ready_fact_triggers_one_revision_and_duplicate_trigger_is_noop(tmp_path):
    desired = _configured_desired_store(tmp_path / "desired.sqlite3")
    facts = OperationalFactRevisionStore(
        tmp_path / "facts.sqlite3",
        clock=lambda: NOW,
    )
    seen = []
    runtime, snapshots, _ = _runtime(
        tmp_path,
        desired_store=desired,
        status={"state": "online", "queue_depth": 0, "failed_jobs": 0},
        publisher=seen.append,
        fact_revision_store=facts,
    )

    async def scenario():
        await runtime.async_start()
        staged = _stage_fact(
            facts,
            desired,
            pipeline_item_id="pipeline:new-fact",
            snapshot_revision=2,
        )
        first = await runtime.async_publish_ready_fact()
        second = await runtime.async_publish_ready_fact()
        return staged, first, second

    staged, first, second = asyncio.run(scenario())

    assert snapshots.head()["revision"] == 2
    assert len(seen) == 2
    assert facts.load(staged.pipeline_item_id).state == "snapshot_committed"
    assert first["counts"]["fact_publications"] == 1
    assert second["counts"]["fact_triggers"] == 2
    assert second["counts"]["fact_trigger_noops"] == 1


def test_exact_fact_publication_returns_same_durable_binding_on_retry(tmp_path):
    desired = _configured_desired_store(tmp_path / "desired.sqlite3")
    facts = OperationalFactRevisionStore(
        tmp_path / "facts.sqlite3",
        clock=lambda: NOW,
    )
    seen = []
    runtime, snapshots, _ = _runtime(
        tmp_path,
        desired_store=desired,
        status={"state": "online", "queue_depth": 0, "failed_jobs": 0},
        publisher=seen.append,
        fact_revision_store=facts,
    )
    process_receipt = "3" * 64

    async def scenario():
        await runtime.async_start()
        staged, binding = _stage_admitted_provider_fact(
            facts,
            desired,
            pipeline_item_id="pipeline:exact-fact",
            snapshot_revision=2,
            process_receipt_sha256=process_receipt,
        )
        first = await runtime.async_publish_exact_fact(binding)
        second = await runtime.async_publish_exact_fact(binding)
        return staged, binding, first, second

    staged, binding, first, second = asyncio.run(scenario())
    assert first == second
    assert first.matches(binding)
    assert first.process_receipt_sha256 == process_receipt
    assert facts.load(staged.pipeline_item_id).state == "snapshot_committed"
    assert snapshots.head()["revision"] == 2
    assert len(seen) == 2


def test_exact_fact_rejects_random_process_receipt_without_publishing(tmp_path):
    desired = _configured_desired_store(tmp_path / "desired.sqlite3")
    facts = OperationalFactRevisionStore(
        tmp_path / "facts.sqlite3",
        clock=lambda: NOW,
    )
    seen = []
    runtime, snapshots, _ = _runtime(
        tmp_path,
        desired_store=desired,
        status={"state": "online", "queue_depth": 0, "failed_jobs": 0},
        publisher=seen.append,
        fact_revision_store=facts,
    )

    async def scenario():
        await runtime.async_start()
        staged, binding = _stage_admitted_provider_fact(
            facts,
            desired,
            pipeline_item_id="pipeline:wrong-process",
            snapshot_revision=2,
            process_receipt_sha256="3" * 64,
        )
        with pytest.raises(
            OperationalSnapshotRuntimeError,
            match="exact_fact_publication_failed",
        ):
            await runtime.async_publish_exact_fact(
                replace(binding, process_receipt_sha256="9" * 64)
            )
        return staged, binding

    staged, binding = asyncio.run(scenario())
    assert facts.load(staged.pipeline_item_id).state == "ready"
    assert snapshots.head()["revision"] == 1
    assert len(seen) == 1
    assert binding.process_receipt_sha256 == "3" * 64


def test_restart_retries_pending_transport_without_duplicate_fact_revision(tmp_path):
    desired = _configured_desired_store(tmp_path / "desired.sqlite3")
    facts = OperationalFactRevisionStore(
        tmp_path / "facts.sqlite3",
        clock=lambda: NOW,
    )
    staged = _stage_fact(
        facts,
        desired,
        pipeline_item_id="pipeline:transport-crash",
        snapshot_revision=1,
    )
    snapshots, outbox = _stores(tmp_path)

    def offline(_payload):
        raise OSError("transport offline")

    first, _, _ = _runtime(
        tmp_path,
        desired_store=desired,
        status={"state": "online", "queue_depth": 0, "failed_jobs": 0},
        publisher=offline,
        snapshot_store=snapshots,
        outbox_store=outbox,
        fact_revision_store=facts,
    )
    first_status = asyncio.run(first.async_start())
    assert first_status["state"] == "degraded"
    assert snapshots.head()["revision"] == 1
    assert outbox.load().state == "pending"
    assert facts.load(staged.pipeline_item_id).state == "snapshot_committed"

    recovered_payloads = []
    second, _, _ = _runtime(
        tmp_path,
        desired_store=desired,
        status={"state": "online", "queue_depth": 9, "failed_jobs": 0},
        publisher=recovered_payloads.append,
        snapshot_store=snapshots,
        outbox_store=outbox,
        fact_revision_store=facts,
    )
    second_status = asyncio.run(second.async_start())

    assert second_status["state"] == "running"
    assert snapshots.head()["revision"] == 1
    assert outbox.load().state == "published"
    assert recovered_payloads == [snapshots.head()["canonical_envelope"]]
    assert second_status["counts"]["fact_publications"] == 0


def test_unchanged_tick_fast_path_does_not_reread_outbox_or_snapshot_head(
    tmp_path,
):
    desired = _configured_desired_store(tmp_path / "desired.sqlite3")
    status = {"state": "online", "queue_depth": 0, "failed_jobs": 0}
    snapshots, outbox = _stores(tmp_path)
    reads = {"head": 0, "outbox": 0, "status": 0}
    original_head = snapshots.head
    original_load = outbox.load

    def counted_head():
        reads["head"] += 1
        return original_head()

    def counted_load():
        reads["outbox"] += 1
        return original_load()

    def status_supplier():
        reads["status"] += 1
        return dict(status)

    snapshots.head = counted_head
    outbox.load = counted_load
    runtime = OperationalSnapshotRuntime(
        publisher=lambda _payload: None,
        status_supplier=status_supplier,
        desired_state_store=desired,
        snapshot_store=snapshots,
        outbox_store=outbox,
        clock=lambda: NOW,
    )

    async def scenario():
        await runtime.async_start()
        durable_reads = dict(reads)
        for _ in range(100):
            await runtime.async_tick()
        return durable_reads

    durable_reads = asyncio.run(scenario())

    assert reads["head"] == durable_reads["head"]
    assert reads["outbox"] == durable_reads["outbox"]
    assert reads["status"] == durable_reads["status"] + 100


def test_status_churn_never_creates_revision_but_desired_change_samples_latest(
    tmp_path,
):
    desired = _configured_desired_store(tmp_path / "desired.sqlite3")
    status = {"state": "online", "queue_depth": 0, "failed_jobs": 0}
    published: list[bytes] = []
    runtime, snapshots, _ = _runtime(
        tmp_path,
        desired_store=desired,
        status=status,
        publisher=published.append,
    )

    async def scenario():
        await runtime.async_start()
        status["ignored_status_text"] = "changed but outside the trigger"
        for index in range(100):
            status["queue_depth"] = index
            status["state"] = "busy" if index % 2 else "online"
            status["failed_jobs"] = index // 10
            await runtime.async_tick()
        _change_desired_state(desired)
        await runtime.async_tick()

    asyncio.run(scenario())

    assert snapshots.head()["revision"] == 2
    assert len(published) == 2
    decoded = [decode_control_envelope(value) for value in published]
    assert [item["snapshot"]["worker"]["queue_depth"] for item in decoded] == [0, 99]
    assert [item["snapshot"]["worker"]["state"] for item in decoded] == [
        "online", "busy",
    ]
    assert [item["snapshot"]["worker"]["failed_jobs"] for item in decoded] == [0, 9]
    assert [item["revision"] for item in decoded] == [1, 2]


def test_ttl_refreshes_once_before_expiry_and_samples_latest_status(tmp_path):
    desired = _configured_desired_store(tmp_path / "desired.sqlite3")
    status = {"state": "online", "queue_depth": 0, "failed_jobs": 0}
    published: list[bytes] = []
    now = [NOW]
    runtime, snapshots, _ = _runtime(
        tmp_path,
        desired_store=desired,
        status=status,
        publisher=published.append,
        clock=lambda: now[0],
    )

    async def scenario():
        await runtime.async_start()
        status.update({"state": "busy", "queue_depth": 17, "failed_jobs": 3})
        now[0] = NOW + UNKNOWN_SNAPSHOT_TTL_SECONDS - (
            SNAPSHOT_REFRESH_MARGIN_SECONDS + 0.001
        )
        await runtime.async_tick()
        assert snapshots.head()["revision"] == 1
        now[0] += 0.001
        await runtime.async_tick()
        await asyncio.gather(*(runtime.async_tick() for _ in range(50)))

    asyncio.run(scenario())

    assert snapshots.head()["revision"] == 2
    assert len(published) == 2
    renewed = decode_control_envelope(published[-1])["snapshot"]
    assert renewed["worker"] == {
        "state": "busy",
        "queue_depth": 17,
        "failed_jobs": 3,
        "status_text": "",
    }
    assert renewed["valid_until"] - renewed["evaluated_at"] == (
        UNKNOWN_SNAPSHOT_TTL_SECONDS
    )
    assert runtime.diagnostics()["counts"]["ttl_refreshes"] == 1


def test_restart_recovers_published_head_without_duplicate_revision(tmp_path):
    desired = _configured_desired_store(tmp_path / "desired.sqlite3")
    status = {"state": "online", "queue_depth": 0, "failed_jobs": 0}
    snapshot, outbox = _stores(tmp_path)
    first_seen: list[bytes] = []
    first, _, _ = _runtime(
        tmp_path,
        desired_store=desired,
        status=status,
        publisher=first_seen.append,
        snapshot_store=snapshot,
        outbox_store=outbox,
    )
    asyncio.run(first.async_start())
    asyncio.run(first.async_stop())

    second_seen: list[bytes] = []
    second, _, _ = _runtime(
        tmp_path,
        desired_store=desired,
        status=status,
        publisher=second_seen.append,
        snapshot_store=OperationalSnapshotStore(
            snapshot.path, clock=lambda: NOW + 1
        ),
        outbox_store=PublicationOutboxStore(
            outbox.path, clock=lambda: NOW + 1
        ),
    )
    second_status = asyncio.run(second.async_start())

    assert len(first_seen) == 1
    assert second_seen == []
    assert snapshot.head()["revision"] == 1
    assert second_status["state"] == "running"
    assert second_status["counts"]["unchanged_ticks"] == 1


def test_publisher_failure_is_durable_and_retry_does_not_create_revision(
    tmp_path,
):
    desired = _configured_desired_store(tmp_path / "desired.sqlite3")
    status = {"state": "busy", "queue_depth": 7, "failed_jobs": 2}
    online = False
    attempts: list[bytes] = []

    def publisher(payload: bytes):
        attempts.append(payload)
        if not online:
            raise OSError("offline")

    runtime, snapshots, outbox = _runtime(
        tmp_path,
        desired_store=desired,
        status=status,
        publisher=publisher,
    )

    first = asyncio.run(runtime.async_start())
    assert first["state"] == "degraded"
    assert snapshots.head()["revision"] == 1
    assert outbox.load().state == "pending"

    online = True
    recovered = asyncio.run(runtime.async_tick())

    assert recovered["state"] == "running"
    assert snapshots.head()["revision"] == 1
    assert outbox.load().state == "published"
    assert attempts == [snapshots.head()["canonical_envelope"]] * 2
    assert recovered["counts"]["publication_requests"] == 1


@pytest.mark.parametrize("corrupt_store", ("desired", "snapshot", "outbox"))
def test_corruption_is_fail_closed_and_never_reaches_publisher(
    tmp_path,
    corrupt_store,
):
    desired_path = tmp_path / "desired.sqlite3"
    snapshot_path = tmp_path / "snapshots.sqlite3"
    outbox_path = tmp_path / "publication.sqlite3"
    desired = _configured_desired_store(desired_path)
    snapshot = OperationalSnapshotStore(snapshot_path, clock=lambda: NOW)
    outbox = PublicationOutboxStore(outbox_path, clock=lambda: NOW)
    snapshot.initialize()
    outbox.initialize()

    paths = {
        "desired": desired_path,
        "snapshot": snapshot_path,
        "outbox": outbox_path,
    }
    paths[corrupt_store].write_bytes(b"not-a-sqlite-database")
    seen: list[bytes] = []
    runtime = OperationalSnapshotRuntime(
        publisher=seen.append,
        status_supplier=lambda: {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
        },
        desired_state_store=DesiredStateStore(desired_path),
        snapshot_store=OperationalSnapshotStore(snapshot_path, clock=lambda: NOW),
        outbox_store=PublicationOutboxStore(outbox_path, clock=lambda: NOW),
        clock=lambda: NOW,
    )

    result = asyncio.run(runtime.async_start())

    assert result["state"] == "degraded"
    assert result["last_error_code"] is not None
    assert seen == []


def test_async_status_supplier_is_supported_and_invalid_data_fails_closed(
    tmp_path,
):
    desired = _configured_desired_store(tmp_path / "desired.sqlite3")
    value = {"state": "online", "queue_depth": 0, "failed_jobs": 0}
    seen: list[bytes] = []
    snapshot, outbox = _stores(tmp_path)

    async def supplier():
        await asyncio.sleep(0)
        return dict(value)

    runtime = OperationalSnapshotRuntime(
        publisher=seen.append,
        status_supplier=supplier,
        desired_state_store=desired,
        snapshot_store=snapshot,
        outbox_store=outbox,
        clock=lambda: NOW,
    )

    asyncio.run(runtime.async_start())
    value["queue_depth"] = -1
    async def invalid_then_recover():
        failed_results = [await runtime.async_tick() for _ in range(5)]
        value["queue_depth"] = 0
        recovered = await runtime.async_tick()
        return failed_results, recovered

    failed_results, recovered = asyncio.run(invalid_then_recover())

    assert snapshot.head()["revision"] == 1
    assert len(seen) == 1
    assert all(item["state"] == "degraded" for item in failed_results)
    assert all(item["last_error_code"] == "runtime_failure" for item in failed_results)
    assert recovered["state"] == "running"
    assert recovered["last_error_code"] is None


def test_transport_outbox_protocol_declares_full_coordinator_surface():
    assert {
        "load",
        "stage",
        "adopt_pending",
        "mark_pending",
        "record_attempt",
        "mark_published",
        "discard_staged",
    } <= set(TransportOutbox.__dict__)


def test_stop_cancels_publish_but_fresh_runtime_recovers_exact_pending_bytes(
    tmp_path,
):
    desired = _configured_desired_store(tmp_path / "desired.sqlite3")
    status = {"state": "online", "queue_depth": 0, "failed_jobs": 0}
    snapshot, outbox = _stores(tmp_path)

    async def first_lifecycle():
        entered = asyncio.Event()
        never = asyncio.Event()

        async def blocked(_payload: bytes):
            entered.set()
            await never.wait()

        runtime, _, _ = _runtime(
            tmp_path,
            desired_store=desired,
            status=status,
            publisher=blocked,
            snapshot_store=snapshot,
            outbox_store=outbox,
        )
        starter = asyncio.create_task(runtime.async_start())
        await entered.wait()
        stopped = await asyncio.wait_for(runtime.async_stop(), timeout=2)
        outcome = await asyncio.gather(starter, return_exceptions=True)
        return stopped, outcome

    stopped, outcome = asyncio.run(first_lifecycle())
    assert stopped["state"] == "stopped"
    assert stopped["closed"] is True
    assert len(outcome) == 1
    assert isinstance(outcome[0], asyncio.CancelledError)
    assert snapshot.head()["revision"] == 1
    assert outbox.load().state == "pending"
    exact = snapshot.head()["canonical_envelope"]

    recovered_seen: list[bytes] = []
    recovered, _, _ = _runtime(
        tmp_path,
        desired_store=desired,
        status=status,
        publisher=recovered_seen.append,
        snapshot_store=OperationalSnapshotStore(snapshot.path, clock=lambda: NOW + 1),
        outbox_store=PublicationOutboxStore(outbox.path, clock=lambda: NOW + 1),
    )
    result = asyncio.run(recovered.async_start())

    assert recovered_seen == [exact]
    assert snapshot.head()["revision"] == 1
    assert outbox.load().state == "published"
    assert result["state"] == "running"


def test_tick_requires_start_and_stopped_instance_cannot_restart(tmp_path):
    desired = _configured_desired_store(tmp_path / "desired.sqlite3")
    status = {"state": "online", "queue_depth": 0, "failed_jobs": 0}
    runtime, _, _ = _runtime(
        tmp_path,
        desired_store=desired,
        status=status,
        publisher=lambda _payload: None,
    )

    with pytest.raises(RuntimeError, match="niet gestart"):
        asyncio.run(runtime.async_tick())
    asyncio.run(runtime.async_start())
    asyncio.run(runtime.async_stop())
    with pytest.raises(RuntimeError, match="gesloten"):
        asyncio.run(runtime.async_start())
