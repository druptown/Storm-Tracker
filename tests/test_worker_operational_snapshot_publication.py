from __future__ import annotations

import ast
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from copy import deepcopy
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import sqlite3
import threading

import pytest

import storm_tracker_worker.operational_snapshot_publication as publication_module
from storm_tracker_worker.desired_state import DesiredState, parse_desired_state
from storm_tracker_worker.operational_fact_revision import (
    CommittedOperationalFactRevision,
)
from storm_tracker_worker.operational_fact_store import (
    OperationalFactRevisionStore,
)
from storm_tracker_worker.operational_snapshot_builder import (
    build_unknown_operational_snapshot,
)
from storm_tracker_worker.operational_snapshot_publication import (
    MAX_OUTBOX_DATABASE_BYTES,
    OperationalSnapshotPublicationCoordinator,
    PublicationOutboxError,
    PublicationOutboxStore,
    PublicationTransportHead,
    deterministic_publication_id,
)
from storm_tracker_worker.operational_snapshot_store import (
    OperationalSnapshotStore,
    SnapshotHeadConflict,
)
from storm_tracker_worker.tracking_core.operational_snapshot import (
    canonical_json_bytes,
    decode_control_envelope,
)


NOW = 1_800_100_000.0


class DesiredReader:
    def __init__(self, desired: DesiredState | None) -> None:
        self.desired = desired
        self.reads = 0

    def load_active(self) -> DesiredState | None:
        self.reads += 1
        return self.desired


def _desired_state(count: int = 2, *, long_identity: bool = False) -> DesiredState:
    targets = []
    for index in range(count):
        suffix = f"-{index:02d}"
        target_id = (
            f"target-{index:02d}"
            if not long_identity
            else "t" * (155 - len(suffix)) + suffix
        )
        display_name = (
            f"Tracker {index:02d}"
            if not long_identity
            else "N" * 124 + f"{index:04d}"
        )
        targets.append({
            "target_id": target_id,
            "display_name": display_name,
            "kind": "home" if index == 0 else "device_tracker",
            "available": True,
            "latitude": 50.8 + index / 1000,
            "longitude": 4.2 + index / 1000,
            "position_observed_at": NOW - 5,
            "location_accuracy_m": 10.0,
            "observation_radius_km": 250.0,
            "country_code": "BE",
        })
    return parse_desired_state({
        "contract": "stv3-worker-desired-state-v1",
        "producer_id": "ha:publication-test",
        "authority_epoch": 1,
        "generation": 1,
        "issued_at": NOW,
        "previous_state_sha256": None,
        "targets": targets,
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": True,
            "region_sharing_distance_km": 100.0,
        },
    })


def _coordinator(
    tmp_path: Path,
    *,
    desired: DesiredState | None = None,
    publisher=None,
    snapshot_store=None,
    outbox_store=None,
    rich_snapshot_builder=None,
    fact_revision_store=None,
):
    reader = DesiredReader(_desired_state() if desired is None else desired)
    snapshot = snapshot_store or OperationalSnapshotStore(
        tmp_path / "snapshots.sqlite3", clock=lambda: NOW
    )
    outbox = outbox_store or PublicationOutboxStore(
        tmp_path / "publication.sqlite3", clock=lambda: NOW
    )
    published: list[bytes] = []

    async def default_publisher(payload: bytes) -> None:
        published.append(payload)

    coordinator = OperationalSnapshotPublicationCoordinator(
        desired_state_store=reader,
        snapshot_store=snapshot,
        outbox_store=outbox,
        publisher=publisher or default_publisher,
        clock=lambda: NOW,
        rich_snapshot_builder=rich_snapshot_builder,
        fact_revision_store=fact_revision_store,
    )
    return coordinator, reader, snapshot, outbox, published


def _publish(coordinator, *, status: str = "operationeel"):
    return asyncio.run(coordinator.async_publish(
        worker_state="online",
        queue_depth=2,
        failed_jobs=1,
        worker_status_text=status,
    ))


def _hard_crash_outbox(path: str, fault_point: str) -> None:
    def crash(point: str) -> None:
        if point == fault_point:
            os._exit(73)

    PublicationOutboxStore(
        path,
        clock=lambda: NOW,
        fault_injector=crash,
    ).stage(
        revision=1,
        envelope_sha256="a" * 64,
        desired_state_sha256="b" * 64,
        publication_id_sha256="c" * 64,
    )


def test_unconfigured_worker_commits_and_publishes_nothing(tmp_path):
    reader = DesiredReader(None)
    snapshot = OperationalSnapshotStore(tmp_path / "snapshots.sqlite3")
    outbox = PublicationOutboxStore(tmp_path / "publication.sqlite3")
    published = []
    coordinator = OperationalSnapshotPublicationCoordinator(
        desired_state_store=reader,
        snapshot_store=snapshot,
        outbox_store=outbox,
        publisher=published.append,
        clock=lambda: NOW,
    )

    status = _publish(coordinator)

    assert status["state"] == "unconfigured"
    assert status["dirty"] is False
    assert status["desired_state_configured"] is False
    assert snapshot.head() is None
    assert outbox.load() is None
    assert published == []


def test_empty_but_active_desired_state_is_still_configured(tmp_path):
    coordinator, _, snapshot, _, published = _coordinator(
        tmp_path, desired=_desired_state(0)
    )

    status = _publish(coordinator)

    assert status["desired_state_configured"] is True
    assert status["desired_target_count"] == 0
    assert snapshot.head()["snapshot"]["targets"]["items"] == []
    assert len(published) == 1


def test_commit_precedes_publish_and_publisher_gets_exact_reread_head(tmp_path):
    snapshot = OperationalSnapshotStore(
        tmp_path / "snapshots.sqlite3", clock=lambda: NOW
    )
    seen = []

    def publisher(payload: bytes) -> None:
        head = snapshot.head()
        assert head is not None
        assert head["canonical_envelope"] == payload
        seen.append(payload)

    coordinator, _, _, outbox, _ = _coordinator(
        tmp_path, snapshot_store=snapshot, publisher=publisher
    )

    status = _publish(coordinator)

    head = snapshot.head()
    assert status["transport_state"] == "published"
    assert seen == [head["canonical_envelope"]]
    assert hashlib.sha256(seen[0]).hexdigest() == head["envelope_sha256"]
    assert outbox.load().state == "published"
    decoded = decode_control_envelope(seen[0])
    assert decoded["revision"] == 1
    assert decoded["snapshot"]["provenance_complete"] is False
    assert all(
        target["current_precipitation"]["state"] == "unknown"
        for target in decoded["snapshot"]["targets"]["items"]
    )


def test_uncommitted_rich_builder_is_withheld_and_unknown_is_published(tmp_path):
    built = []

    def rich_builder(desired, **kwargs):
        snapshot = build_unknown_operational_snapshot(
            desired,
            revision=kwargs["revision"],
            evaluated_at=kwargs["evaluated_at"],
            worker_state=kwargs["worker_state"],
            queue_depth=kwargs["queue_depth"],
            failed_jobs=kwargs["failed_jobs"],
            reason_code=kwargs["reason_code"],
            worker_status_text="rich-builder-owned",
        )
        built.append(deepcopy(snapshot))
        return snapshot

    coordinator, _, snapshot, _, _ = _coordinator(
        tmp_path, rich_snapshot_builder=rich_builder
    )

    status = _publish(coordinator, status="caller-text")

    assert built
    assert status["counts"]["rich_snapshots_built"] == 0
    assert status["counts"]["rich_snapshots_withheld"] == 1
    assert status["counts"]["unknown_snapshots_built"] == 1
    assert snapshot.head()["snapshot"] != built[0]
    assert snapshot.head()["snapshot"]["worker"]["status_text"] == "caller-text"
    assert snapshot.head()["snapshot"]["provenance_complete"] is False


def test_exact_committed_revision_is_the_only_accepted_rich_builder_result(tmp_path):
    built = []

    def rich_builder(desired, **kwargs):
        snapshot = build_unknown_operational_snapshot(
            desired,
            revision=kwargs["revision"],
            evaluated_at=kwargs["evaluated_at"],
            worker_state=kwargs["worker_state"],
            queue_depth=kwargs["queue_depth"],
            failed_jobs=kwargs["failed_jobs"],
            reason_code=kwargs["reason_code"],
            worker_status_text=kwargs["worker_status_text"],
        )
        built.append(deepcopy(snapshot))
        return CommittedOperationalFactRevision.create(
            desired_state_sha256=desired.state_sha256,
            authority_epoch=desired.authority_epoch,
            snapshot=snapshot,
            admission_receipts=(),
        )

    coordinator, _, snapshot, _, _ = _coordinator(
        tmp_path, rich_snapshot_builder=rich_builder
    )

    status = _publish(coordinator, status="committed-builder")

    assert status["counts"]["rich_snapshots_built"] == 1
    assert status["counts"]["rich_snapshots_withheld"] == 0
    assert status["counts"]["unknown_snapshots_built"] == 0
    assert snapshot.head()["snapshot"] == built[0]


def test_committed_fact_handoff_is_reconciled_before_mqtt_publish(tmp_path):
    fact_store = OperationalFactRevisionStore(
        tmp_path / "facts.sqlite3",
        clock=lambda: NOW,
    )
    published_after_fact_commit = []

    def rich_builder(desired, **kwargs):
        snapshot = build_unknown_operational_snapshot(
            desired,
            revision=kwargs["revision"],
            evaluated_at=kwargs["evaluated_at"],
            worker_state=kwargs["worker_state"],
            queue_depth=kwargs["queue_depth"],
            failed_jobs=kwargs["failed_jobs"],
            reason_code=kwargs["reason_code"],
            worker_status_text=kwargs["worker_status_text"],
        )
        revision = CommittedOperationalFactRevision.create(
            desired_state_sha256=desired.state_sha256,
            authority_epoch=desired.authority_epoch,
            snapshot=snapshot,
            admission_receipts=(),
        )
        return fact_store.stage(
            "pipeline:dwd:fact-handoff",
            desired_state=desired,
            revision=revision,
        ).revision

    async def publisher(payload):
        record = fact_store.load("pipeline:dwd:fact-handoff")
        assert record.state == "snapshot_committed"
        published_after_fact_commit.append(payload)

    coordinator, _, snapshots, _, _ = _coordinator(
        tmp_path,
        publisher=publisher,
        rich_snapshot_builder=rich_builder,
        fact_revision_store=fact_store,
    )

    status = _publish(coordinator)

    record = fact_store.load("pipeline:dwd:fact-handoff")
    assert status["transport_state"] == "published"
    assert status["counts"]["fact_revisions_reconciled"] == 1
    assert record.state == "snapshot_committed"
    assert record.snapshot_sha256 == snapshots.head()["snapshot_sha256"]
    assert record.publication_envelope_sha256 == snapshots.head()["envelope_sha256"]
    assert published_after_fact_commit == [snapshots.head()["canonical_envelope"]]


@pytest.mark.parametrize("mismatch", ("revision", "future", "expired"))
def test_committed_revision_must_match_exact_publication_authority(
    tmp_path,
    mismatch,
):
    def rich_builder(desired, **kwargs):
        revision = kwargs["revision"] + (1 if mismatch == "revision" else 0)
        evaluated_at = kwargs["evaluated_at"]
        if mismatch == "future":
            evaluated_at += 1.0
        elif mismatch == "expired":
            evaluated_at -= 300.0
        snapshot = build_unknown_operational_snapshot(
            desired,
            revision=revision,
            evaluated_at=evaluated_at,
            worker_state=kwargs["worker_state"],
            queue_depth=kwargs["queue_depth"],
            failed_jobs=kwargs["failed_jobs"],
            reason_code=kwargs["reason_code"],
            worker_status_text=kwargs["worker_status_text"],
        )
        return CommittedOperationalFactRevision.create(
            desired_state_sha256=desired.state_sha256,
            authority_epoch=desired.authority_epoch,
            snapshot=snapshot,
            admission_receipts=(),
        )

    coordinator, _, store, _, _ = _coordinator(
        tmp_path,
        rich_snapshot_builder=rich_builder,
    )

    status = _publish(coordinator)

    assert status["state"] == "idle"
    assert status["counts"]["rich_snapshots_built"] == 0
    assert status["counts"]["rich_snapshots_withheld"] == 1
    assert status["counts"]["unknown_snapshots_built"] == 1
    assert store.head()["snapshot"]["revision"] == 1
    assert store.head()["snapshot"]["worker"]["queue_depth"] == 2


def test_rich_builder_exception_publishes_fresh_unknown_without_error_text(tmp_path):
    def broken_builder(*_args, **_kwargs):
        raise OSError("api_key=must-not-enter-diagnostics")

    coordinator, _, store, _, seen = _coordinator(
        tmp_path,
        rich_snapshot_builder=broken_builder,
    )

    status = _publish(coordinator)

    assert status["state"] == "idle"
    assert status["counts"]["rich_builder_failures"] == 1
    assert status["counts"]["rich_snapshots_withheld"] == 1
    assert status["counts"]["unknown_snapshots_built"] == 1
    assert "api_key" not in json.dumps(status)
    assert seen == [store.head()["canonical_envelope"]]


def test_deterministic_publication_id_binds_desired_state_and_snapshot():
    desired = _desired_state(1)
    snapshot = build_unknown_operational_snapshot(
        desired,
        revision=1,
        evaluated_at=NOW,
        worker_state="online",
        queue_depth=0,
        failed_jobs=0,
    )
    canonical = canonical_json_bytes(snapshot)

    first = deterministic_publication_id(
        desired_state_sha256=desired.state_sha256,
        canonical_snapshot=canonical,
    )
    second = deterministic_publication_id(
        desired_state_sha256=desired.state_sha256,
        canonical_snapshot=canonical,
    )
    changed_snapshot = deepcopy(snapshot)
    changed_snapshot["worker"]["queue_depth"] = 1

    assert first == second
    assert first.startswith("snapshot-publication:")
    assert first != deterministic_publication_id(
        desired_state_sha256="f" * 64,
        canonical_snapshot=canonical,
    )
    assert first != deterministic_publication_id(
        desired_state_sha256=desired.state_sha256,
        canonical_snapshot=canonical_json_bytes(changed_snapshot),
    )


def test_lost_publish_result_retries_byte_identically_without_new_revision(tmp_path):
    snapshots = tmp_path / "snapshots.sqlite3"
    outbox_path = tmp_path / "publication.sqlite3"
    first_seen = []

    def lost_result(payload: bytes) -> None:
        first_seen.append(payload)
        raise RuntimeError("publisher result lost")

    first, _, store, outbox, _ = _coordinator(
        tmp_path,
        snapshot_store=OperationalSnapshotStore(snapshots, clock=lambda: NOW),
        outbox_store=PublicationOutboxStore(outbox_path, clock=lambda: NOW),
        publisher=lost_result,
    )
    failed = _publish(first)
    assert failed["state"] == "pending"
    assert failed["last_error_code"] == "publication_failed"
    assert outbox.load().state == "pending"
    durable = store.head()["canonical_envelope"]
    assert first_seen == [durable]

    retry_seen = []
    recovered, _, recovered_store, recovered_outbox, _ = _coordinator(
        tmp_path,
        snapshot_store=OperationalSnapshotStore(snapshots, clock=lambda: NOW + 1),
        outbox_store=PublicationOutboxStore(outbox_path, clock=lambda: NOW + 1),
        publisher=retry_seen.append,
    )
    status = asyncio.run(recovered.async_recover())

    assert status["transport_state"] == "published"
    assert recovered_store.head()["revision"] == 1
    assert retry_seen == [durable]
    assert first_seen[0] == retry_seen[0]
    assert recovered_outbox.load().attempt_count == 2


def test_snapshot_commit_lost_result_is_recovered_from_staged_outbox(tmp_path):
    snapshot_path = tmp_path / "snapshots.sqlite3"
    outbox_path = tmp_path / "publication.sqlite3"

    def fail(point: str) -> None:
        if point == "after_commit":
            raise RuntimeError("snapshot commit result lost")

    coordinator, _, store, outbox, published = _coordinator(
        tmp_path,
        snapshot_store=OperationalSnapshotStore(
            snapshot_path, clock=lambda: NOW, fault_injector=fail
        ),
        outbox_store=PublicationOutboxStore(outbox_path, clock=lambda: NOW),
    )
    failed = _publish(coordinator)
    assert failed["state"] == "pending"
    assert store.head()["revision"] == 1
    assert outbox.load().state == "staged"
    assert published == []

    recovered_seen = []
    recovered, _, recovered_store, recovered_outbox, _ = _coordinator(
        tmp_path,
        snapshot_store=OperationalSnapshotStore(snapshot_path),
        outbox_store=PublicationOutboxStore(outbox_path),
        publisher=recovered_seen.append,
    )
    asyncio.run(recovered.async_recover())

    assert recovered_store.head()["revision"] == 1
    assert recovered_seen == [recovered_store.head()["canonical_envelope"]]
    assert recovered_outbox.load().state == "published"


def test_lost_outbox_stage_result_without_snapshot_is_discarded_then_reusable(
    tmp_path,
):
    snapshot_path = tmp_path / "snapshots.sqlite3"
    outbox_path = tmp_path / "publication.sqlite3"

    def fail(point: str) -> None:
        if point == "stage.after_commit":
            raise RuntimeError("stage result lost")

    first, _, snapshots, _, published = _coordinator(
        tmp_path,
        snapshot_store=OperationalSnapshotStore(snapshot_path),
        outbox_store=PublicationOutboxStore(outbox_path, fault_injector=fail),
    )
    status = _publish(first)
    assert status["state"] == "error"
    assert snapshots.head() is None
    assert published == []
    assert PublicationOutboxStore(outbox_path).load().state == "staged"

    recovered, _, _, recovered_outbox, recovered_seen = _coordinator(
        tmp_path,
        snapshot_store=OperationalSnapshotStore(snapshot_path),
        outbox_store=PublicationOutboxStore(outbox_path),
    )
    asyncio.run(recovered.async_recover())
    assert recovered_outbox.load() is None
    assert recovered_seen == []

    status = _publish(recovered)
    assert status["snapshot_revision"] == 1
    assert recovered_outbox.load().state == "published"
    assert len(recovered_seen) == 1


@pytest.mark.parametrize(
    "fault_point,published_is_durable,expected_retry_count",
    (
        ("published.before_commit", False, 2),
        ("published.after_commit", True, 1),
    ),
)
def test_publish_receipt_crash_boundary_is_at_least_once_and_never_new_revision(
    tmp_path, fault_point, published_is_durable, expected_retry_count,
):
    snapshot_path = tmp_path / "snapshots.sqlite3"
    outbox_path = tmp_path / "publication.sqlite3"
    seen = []

    def fail(point: str) -> None:
        if point == fault_point:
            raise RuntimeError("receipt boundary")

    first, _, store, _, _ = _coordinator(
        tmp_path,
        snapshot_store=OperationalSnapshotStore(snapshot_path),
        outbox_store=PublicationOutboxStore(outbox_path, fault_injector=fail),
        publisher=seen.append,
    )
    _publish(first)
    state = PublicationOutboxStore(outbox_path).load()
    assert state.state == ("published" if published_is_durable else "pending")
    exact = store.head()["canonical_envelope"]
    assert seen == [exact]

    recovered, _, recovered_store, recovered_outbox, _ = _coordinator(
        tmp_path,
        snapshot_store=OperationalSnapshotStore(snapshot_path),
        outbox_store=PublicationOutboxStore(outbox_path),
        publisher=seen.append,
    )
    asyncio.run(recovered.async_recover())

    assert recovered_store.head()["revision"] == 1
    assert recovered_outbox.load().state == "published"
    assert seen == [exact] * expected_retry_count


@pytest.mark.parametrize(
    "fault_point,committed",
    (("stage.before_commit", False), ("stage.after_commit", True)),
)
def test_abrupt_outbox_process_death_respects_sqlite_commit_boundary(
    tmp_path, fault_point, committed,
):
    path = tmp_path / f"hard-crash-{fault_point}.sqlite3"
    process = multiprocessing.get_context("spawn").Process(
        target=_hard_crash_outbox, args=(str(path), fault_point)
    )
    process.start()
    process.join(timeout=20)
    if process.is_alive():
        process.terminate()
        process.join(timeout=5)
        pytest.fail("hard-crash child bleef hangen")
    assert process.exitcode == 73
    recovered = PublicationOutboxStore(path).load()
    assert (recovered is not None) is committed
    if recovered is not None:
        assert recovered.state == "staged"
        assert recovered.revision == 1


def test_existing_snapshot_head_without_authority_receipt_is_not_published(
    tmp_path,
):
    desired = _desired_state(1)
    snapshot = build_unknown_operational_snapshot(
        desired,
        revision=1,
        evaluated_at=NOW,
        worker_state="unknown",
        queue_depth=0,
        failed_jobs=0,
    )
    snapshot_store = OperationalSnapshotStore(
        tmp_path / "snapshots.sqlite3", clock=lambda: NOW
    )
    snapshot_store.commit(
        "independent:commit", expected_revision=0, snapshot=snapshot
    )
    seen = []
    coordinator, _, _, outbox, _ = _coordinator(
        tmp_path,
        desired=desired,
        snapshot_store=snapshot_store,
        outbox_store=PublicationOutboxStore(tmp_path / "publication.sqlite3"),
        publisher=seen.append,
    )

    asyncio.run(coordinator.async_recover())

    assert snapshot_store.head()["revision"] == 1
    assert seen == []
    assert outbox.load().state == "superseded"


def test_head_outbox_mismatch_fails_closed_without_publish(tmp_path):
    desired = _desired_state(1)
    snapshot = build_unknown_operational_snapshot(
        desired,
        revision=1,
        evaluated_at=NOW,
        worker_state="unknown",
        queue_depth=0,
        failed_jobs=0,
    )
    store = OperationalSnapshotStore(tmp_path / "snapshots.sqlite3")
    store.commit("commit:one", expected_revision=0, snapshot=snapshot)
    outbox = PublicationOutboxStore(tmp_path / "publication.sqlite3")
    outbox.adopt_pending(
        revision=1,
        envelope_sha256="f" * 64,
        publication_id_sha256="e" * 64,
    )
    seen = []
    coordinator, _, _, _, _ = _coordinator(
        tmp_path,
        desired=desired,
        snapshot_store=store,
        outbox_store=outbox,
        publisher=seen.append,
    )

    status = asyncio.run(coordinator.async_recover())

    assert status["last_error_code"] == "head_mismatch"
    assert status["transport_state"] == "pending"
    assert seen == []
    assert store.head()["revision"] == 1


def test_rolled_back_snapshot_head_fails_closed_without_publish(tmp_path):
    desired = _desired_state(1)
    store = OperationalSnapshotStore(tmp_path / "snapshots.sqlite3")
    for revision in (1, 2):
        snapshot = build_unknown_operational_snapshot(
            desired,
            revision=revision,
            evaluated_at=NOW,
            worker_state="unknown",
            queue_depth=0,
            failed_jobs=0,
        )
        store.commit(
            f"commit:{revision}",
            expected_revision=revision - 1,
            snapshot=snapshot,
        )
    with closing(sqlite3.connect(store.path)) as database, database:
        revision, digest, committed_at = database.execute(
            "SELECT revision,envelope_sha256,committed_at "
            "FROM snapshot_envelopes WHERE revision=1"
        ).fetchone()
        database.execute(
            "UPDATE active_snapshot SET revision=?,envelope_sha256=?,activated_at=?",
            (revision, digest, committed_at),
        )
    seen = []
    coordinator, _, _, _, _ = _coordinator(
        tmp_path, desired=desired, snapshot_store=store, publisher=seen.append
    )

    status = asyncio.run(coordinator.async_recover())

    assert status["state"] == "error"
    assert status["last_error_code"] == "publication_failed"
    assert seen == []


def test_single_flight_coalesces_to_first_and_latest_revision(tmp_path):
    async def scenario():
        started = asyncio.Event()
        release = asyncio.Event()
        seen = []
        active = 0
        maximum_active = 0

        async def publisher(payload: bytes) -> None:
            nonlocal active, maximum_active
            active += 1
            maximum_active = max(maximum_active, active)
            seen.append(payload)
            if len(seen) == 1:
                started.set()
                await release.wait()
            active -= 1

        coordinator, _, store, _, _ = _coordinator(
            tmp_path, publisher=publisher
        )
        first = asyncio.create_task(coordinator.async_publish(
            worker_state="online",
            queue_depth=0,
            failed_jobs=0,
            worker_status_text="first",
        ))
        await started.wait()
        updates = [
            asyncio.create_task(coordinator.async_publish(
                worker_state="online",
                queue_depth=index,
                failed_jobs=0,
                worker_status_text=f"update-{index:02d}",
            ))
            for index in range(1, 41)
        ]
        duplicates = [
            asyncio.create_task(coordinator.async_publish(
                worker_state="online",
                queue_depth=40,
                failed_jobs=0,
                worker_status_text="update-40",
            ))
            for _ in range(20)
        ]
        await asyncio.sleep(0)
        release.set()
        await asyncio.gather(first, *updates, *duplicates)
        return coordinator.transport_status(), store.head(), seen, maximum_active

    status, head, seen, maximum_active = asyncio.run(scenario())

    assert maximum_active == 1
    assert len(seen) == 2
    assert [decode_control_envelope(value)["revision"] for value in seen] == [1, 2]
    assert head["revision"] == 2
    assert head["snapshot"]["worker"]["status_text"] == "update-40"
    assert status["counts"]["coalesced_requests"] == 20


def test_coalesced_latest_revision_reloads_current_desired_state(tmp_path):
    async def scenario():
        desired_a = _desired_state(2)
        desired_b = _desired_state(1)
        started = asyncio.Event()
        release = asyncio.Event()
        seen: list[bytes] = []

        async def publisher(payload: bytes) -> None:
            seen.append(payload)
            if len(seen) == 1:
                started.set()
                await release.wait()

        coordinator, reader, store, _, _ = _coordinator(
            tmp_path, desired=desired_a, publisher=publisher
        )
        first = asyncio.create_task(coordinator.async_publish(
            worker_state="online",
            queue_depth=1,
            failed_jobs=0,
            worker_status_text="desired-a",
        ))
        await started.wait()
        reader.desired = desired_b
        latest = asyncio.create_task(coordinator.async_publish(
            worker_state="online",
            queue_depth=2,
            failed_jobs=0,
            worker_status_text="desired-b",
        ))
        await asyncio.sleep(0)
        release.set()
        await asyncio.gather(first, latest)
        return desired_a, desired_b, reader, store, seen

    desired_a, desired_b, reader, store, seen = asyncio.run(scenario())

    decoded = [decode_control_envelope(payload) for payload in seen]
    assert [item["revision"] for item in decoded] == [1, 2]
    assert {
        item["target_id"]
        for item in decoded[0]["snapshot"]["targets"]["items"]
    } == {target.target_id for target in desired_a.targets}
    assert {
        item["target_id"]
        for item in decoded[1]["snapshot"]["targets"]["items"]
    } == {target.target_id for target in desired_b.targets}
    assert decoded[1]["snapshot"]["worker"]["status_text"] == "desired-b"
    assert store.head()["revision"] == 2
    # Every envelope is checked against current mutable authority immediately
    # before transport, in addition to the per-revision build read.
    assert reader.reads == 4


def test_explicit_request_survives_fresh_process_pending_recovery(tmp_path):
    snapshot_path = tmp_path / "snapshots.sqlite3"
    outbox_path = tmp_path / "publication.sqlite3"
    first_seen: list[bytes] = []

    def offline(payload: bytes) -> None:
        first_seen.append(payload)
        raise OSError("transport offline")

    first, _, first_store, first_outbox, _ = _coordinator(
        tmp_path,
        desired=_desired_state(2),
        snapshot_store=OperationalSnapshotStore(snapshot_path, clock=lambda: NOW),
        outbox_store=PublicationOutboxStore(outbox_path, clock=lambda: NOW),
        publisher=offline,
    )
    _publish(first, status="before-restart")
    assert first_store.head()["revision"] == 1
    assert first_outbox.load().state == "pending"

    recovered_seen: list[bytes] = []
    recovered, _, store, outbox, _ = _coordinator(
        tmp_path,
        desired=_desired_state(1),
        snapshot_store=OperationalSnapshotStore(snapshot_path, clock=lambda: NOW + 1),
        outbox_store=PublicationOutboxStore(outbox_path, clock=lambda: NOW + 1),
        publisher=recovered_seen.append,
    )
    status = asyncio.run(recovered.async_publish(
        worker_state="busy",
        queue_depth=7,
        failed_jobs=2,
        worker_status_text="after-restart",
    ))

    decoded = [decode_control_envelope(payload) for payload in recovered_seen]
    assert [item["revision"] for item in decoded] == [2]
    assert decoded[0]["snapshot"]["worker"]["status_text"] == "after-restart"
    assert len(decoded[0]["snapshot"]["targets"]["items"]) == 1
    assert store.head()["revision"] == 2
    assert outbox.load().state == "published"
    assert status["dirty"] is False


def test_superseded_pending_is_durable_across_an_extra_restart(tmp_path):
    snapshot_path = tmp_path / "snapshots.sqlite3"
    outbox_path = tmp_path / "publication.sqlite3"
    desired_a = _desired_state(2)
    desired_b = _desired_state(1)

    def offline(_payload: bytes) -> None:
        raise OSError("transport offline")

    first, _, _, first_outbox, _ = _coordinator(
        tmp_path,
        desired=desired_a,
        snapshot_store=OperationalSnapshotStore(snapshot_path, clock=lambda: NOW),
        outbox_store=PublicationOutboxStore(outbox_path, clock=lambda: NOW),
        publisher=offline,
    )
    _publish(first)
    assert first_outbox.load().state == "pending"

    forbidden: list[bytes] = []
    recovery, _, _, recovery_outbox, _ = _coordinator(
        tmp_path,
        desired=desired_b,
        snapshot_store=OperationalSnapshotStore(
            snapshot_path, clock=lambda: NOW + 1
        ),
        outbox_store=PublicationOutboxStore(
            outbox_path, clock=lambda: NOW + 1
        ),
        publisher=forbidden.append,
    )
    status = asyncio.run(recovery.async_recover())
    assert forbidden == []
    assert status["transport_state"] == "superseded"
    tombstone = recovery_outbox.load()
    assert tombstone.state == "superseded"
    assert tombstone.superseded_at == NOW + 1

    seen: list[bytes] = []
    final, _, store, outbox, _ = _coordinator(
        tmp_path,
        desired=desired_b,
        snapshot_store=OperationalSnapshotStore(
            snapshot_path, clock=lambda: NOW + 2
        ),
        outbox_store=PublicationOutboxStore(
            outbox_path, clock=lambda: NOW + 2
        ),
        publisher=seen.append,
    )
    final_status = asyncio.run(final.async_publish(
        worker_state="online",
        queue_depth=0,
        failed_jobs=0,
    ))
    assert [
        decode_control_envelope(payload)["revision"] for payload in seen
    ] == [2]
    assert store.head()["revision"] == 2
    assert outbox.load().state == "published"
    assert final_status["counts"]["superseded"] == 0


def test_pending_is_superseded_when_desired_state_was_removed(tmp_path):
    snapshot_path = tmp_path / "snapshots.sqlite3"
    outbox_path = tmp_path / "publication.sqlite3"

    def offline(_payload: bytes) -> None:
        raise OSError("transport offline")

    first, _, _, first_outbox, _ = _coordinator(
        tmp_path,
        desired=_desired_state(1),
        snapshot_store=OperationalSnapshotStore(snapshot_path, clock=lambda: NOW),
        outbox_store=PublicationOutboxStore(outbox_path, clock=lambda: NOW),
        publisher=offline,
    )
    _publish(first)
    assert first_outbox.load().state == "pending"

    seen: list[bytes] = []
    recovered, reader, _, outbox, _ = _coordinator(
        tmp_path,
        desired=_desired_state(1),
        snapshot_store=OperationalSnapshotStore(
            snapshot_path, clock=lambda: NOW + 1
        ),
        outbox_store=PublicationOutboxStore(
            outbox_path, clock=lambda: NOW + 1
        ),
        publisher=seen.append,
    )
    reader.desired = None
    status = asyncio.run(recovered.async_recover())
    assert seen == []
    assert outbox.load().state == "superseded"
    assert status["desired_state_configured"] is False
    assert status["counts"]["superseded"] == 1


@pytest.mark.parametrize("mutation", ("missing", "extra", "duplicate"))
def test_uncommitted_target_mutation_is_withheld_from_store(tmp_path, mutation):
    desired = _desired_state(2)

    def rich_builder(desired_state, **kwargs):
        snapshot = build_unknown_operational_snapshot(
            desired_state,
            revision=kwargs["revision"],
            evaluated_at=kwargs["evaluated_at"],
            worker_state=kwargs["worker_state"],
            queue_depth=kwargs["queue_depth"],
            failed_jobs=kwargs["failed_jobs"],
            reason_code=kwargs["reason_code"],
            worker_status_text=kwargs["worker_status_text"],
        )
        items = snapshot["targets"]["items"]
        region_target_ids = snapshot["regions"]["items"][0]["target_ids"]
        if mutation == "missing":
            removed = items.pop()["target_id"]
            region_target_ids.remove(removed)
        elif mutation == "extra":
            extra = deepcopy(items[-1])
            extra["target_id"] = "zz-extra-target"
            extra["display_name"] = "Extra target"
            items.append(extra)
            region_target_ids.append(extra["target_id"])
            region_target_ids.sort()
        else:
            items.append(deepcopy(items[-1]))
        return snapshot

    coordinator, _, store, outbox, seen = _coordinator(
        tmp_path,
        desired=desired,
        rich_snapshot_builder=rich_builder,
    )

    status = _publish(coordinator)

    assert status["state"] == "idle"
    assert status["counts"]["rich_snapshots_withheld"] == 1
    assert status["counts"]["unknown_snapshots_built"] == 1
    assert status["last_error_code"] is None
    assert [item["target_id"] for item in store.head()["snapshot"]["targets"]["items"]] == [
        "target-00",
        "target-01",
    ]
    assert outbox.load().state == "published"
    assert seen == [store.head()["canonical_envelope"]]


def test_two_coordinators_with_different_candidates_allow_one_cas_head(tmp_path):
    desired = _desired_state(1)
    snapshot_path = tmp_path / "snapshots.sqlite3"
    outbox_path = tmp_path / "publication.sqlite3"
    OperationalSnapshotStore(snapshot_path).initialize()
    PublicationOutboxStore(outbox_path).initialize()
    second_head_barrier = threading.Barrier(2)
    published = []
    published_lock = threading.Lock()

    class BarrierSnapshotStore:
        def __init__(self):
            self.delegate = OperationalSnapshotStore(snapshot_path, clock=lambda: NOW)
            self.head_calls = 0

        def head(self):
            self.head_calls += 1
            value = self.delegate.head()
            if self.head_calls == 2:
                second_head_barrier.wait(timeout=10)
            return value

        def commit(self, *args, **kwargs):
            return self.delegate.commit(*args, **kwargs)

    def run(candidate: str):
        def publisher(payload: bytes) -> None:
            with published_lock:
                published.append(payload)

        coordinator = OperationalSnapshotPublicationCoordinator(
            desired_state_store=DesiredReader(desired),
            snapshot_store=BarrierSnapshotStore(),
            outbox_store=PublicationOutboxStore(outbox_path, clock=lambda: NOW),
            publisher=publisher,
            clock=lambda: NOW,
        )
        return _publish(coordinator, status=candidate)

    with ThreadPoolExecutor(max_workers=2) as pool:
        statuses = list(pool.map(run, ("candidate-a", "candidate-b")))

    store = OperationalSnapshotStore(snapshot_path)
    assert store.head()["revision"] == 1
    assert len(published) == 1
    assert sum(status["transport_state"] == "published" for status in statuses) == 1
    assert sum(status["last_error_code"] == "outbox_error" for status in statuses) == 1


def test_snapshot_cas_conflict_is_reported_and_never_published(tmp_path):
    class ConflictingSnapshotStore:
        def __init__(self):
            self.delegate = OperationalSnapshotStore(
                tmp_path / "snapshots.sqlite3", clock=lambda: NOW
            )

        def head(self):
            return self.delegate.head()

        def commit(self, *_args, **_kwargs):
            raise SnapshotHeadConflict("simulated competing durable head")

    seen = []
    coordinator, _, store, outbox, _ = _coordinator(
        tmp_path,
        snapshot_store=ConflictingSnapshotStore(),
        publisher=seen.append,
    )

    status = _publish(coordinator)

    assert status["last_error_code"] == "cas_conflict"
    assert status["counts"]["cas_conflicts"] == 1
    assert store.head() is None
    assert outbox.load().state == "staged"
    assert seen == []


def test_publisher_error_keeps_unknown_pending_and_never_invokes_fallback(tmp_path):
    builder_calls = 0

    def builder(*_args, **_kwargs):
        nonlocal builder_calls
        builder_calls += 1
        return None

    def offline(_payload: bytes) -> None:
        raise OSError("transport offline")

    coordinator, _, store, outbox, _ = _coordinator(
        tmp_path, publisher=offline, rich_snapshot_builder=builder
    )

    status = _publish(coordinator)

    assert builder_calls == 1
    assert status["transport_state"] == "pending"
    assert outbox.load().state == "pending"
    snapshot = store.head()["snapshot"]
    assert snapshot["provenance_complete"] is False
    assert all(
        item["current_precipitation"]["state"] == "unknown"
        for item in snapshot["targets"]["items"]
    )


def test_all_64_targets_publish_under_transport_budget(tmp_path):
    coordinator, _, store, _, seen = _coordinator(
        tmp_path, desired=_desired_state(64)
    )

    status = _publish(coordinator)

    assert status["desired_target_count"] == 64
    assert status["transport_state"] == "published"
    assert store.head()["envelope_bytes"] <= 64 * 1024
    assert seen == [store.head()["canonical_envelope"]]


def test_oversize_rich_snapshot_fails_before_store_outbox_or_publish(tmp_path):
    desired = _desired_state(64, long_identity=True)

    def rich_builder(desired_state, **kwargs):
        return build_unknown_operational_snapshot(
            desired_state,
            revision=kwargs["revision"],
            evaluated_at=kwargs["evaluated_at"],
            worker_state=kwargs["worker_state"],
            queue_depth=kwargs["queue_depth"],
            failed_jobs=kwargs["failed_jobs"],
            reason_code=kwargs["reason_code"],
            worker_status_text=kwargs["worker_status_text"],
        )

    coordinator, _, store, outbox, seen = _coordinator(
        tmp_path, desired=desired, rich_snapshot_builder=rich_builder
    )

    status = _publish(coordinator)

    assert status["state"] == "error"
    assert store.head() is None
    assert outbox.load() is None
    assert seen == []


def test_secret_shaped_snapshot_is_never_published_or_stored_in_outbox(tmp_path):
    secret = "api_key=THIS_VALUE_MUST_NEVER_PERSIST"
    coordinator, _, store, outbox, seen = _coordinator(tmp_path)

    status = _publish(coordinator, status=secret)

    assert status["state"] == "pending"
    assert store.head() is None
    assert outbox.load().state == "staged"
    assert seen == []
    persisted = b"".join(
        path.read_bytes()
        for path in tmp_path.iterdir()
        if path.is_file() and path.name.startswith("publication.sqlite3")
    )
    assert secret.encode() not in persisted
    with closing(sqlite3.connect(outbox.path)) as database, database:
        columns = database.execute(
            "PRAGMA table_info(publication_transport_head)"
        ).fetchall()
    assert all(str(row[2]).upper() != "BLOB" for row in columns)
    assert not {"snapshot", "envelope", "payload", "target_id"}.intersection(
        {row[1] for row in columns}
    )


def test_status_is_o1_privacy_minimal_and_performs_no_store_reads():
    class NeverRead:
        def __getattr__(self, name):
            def fail(*_args, **_kwargs):
                raise AssertionError(f"unexpected store read: {name}")
            return fail

    coordinator = OperationalSnapshotPublicationCoordinator(
        desired_state_store=NeverRead(),
        snapshot_store=NeverRead(),
        outbox_store=NeverRead(),
        publisher=lambda _payload: None,
    )

    for _ in range(10_000):
        status = coordinator.transport_status()

    encoded = json.dumps(status, sort_keys=True)
    assert status["counts"]["desired_state_reads"] == 0
    assert status["counts"]["head_readbacks"] == 0
    assert "latitude" not in encoded
    assert "longitude" not in encoded
    assert "target_id" not in encoded
    assert "producer_id" not in encoded


def test_close_cancels_only_one_inflight_publish_and_leaves_durable_pending(
    tmp_path,
):
    async def scenario():
        started = asyncio.Event()
        never = asyncio.Event()

        async def publisher(_payload: bytes):
            started.set()
            await never.wait()

        coordinator, _, store, outbox, _ = _coordinator(
            tmp_path, publisher=publisher
        )
        caller = asyncio.create_task(coordinator.async_publish(
            worker_state="online",
            queue_depth=0,
            failed_jobs=0,
        ))
        await started.wait()
        await coordinator.async_close()
        outcome = await asyncio.gather(caller, return_exceptions=True)
        return coordinator.transport_status(), store.head(), outbox.load(), outcome

    status, head, transport, outcome = asyncio.run(scenario())

    assert status["state"] == "closed"
    assert status["in_flight"] is False
    assert status["counts"]["cancellations"] == 1
    assert head["revision"] == 1
    assert transport.state == "pending"
    assert transport.attempt_count == 1
    assert isinstance(outcome[0], asyncio.CancelledError)


def test_outbox_transitions_are_idempotent_bounded_and_metadata_only(tmp_path):
    path = tmp_path / "publication.sqlite3"
    outbox = PublicationOutboxStore(path, clock=lambda: NOW)
    staged = outbox.stage(
        revision=1,
        envelope_sha256="a" * 64,
        desired_state_sha256="b" * 64,
        publication_id_sha256="c" * 64,
    )
    assert outbox.stage(
        revision=1,
        envelope_sha256="a" * 64,
        desired_state_sha256="b" * 64,
        publication_id_sha256="c" * 64,
    ) == staged
    assert outbox.mark_pending(
        revision=1, envelope_sha256="a" * 64
    ).state == "pending"
    assert outbox.record_attempt(
        revision=1, envelope_sha256="a" * 64
    ).attempt_count == 1
    published = outbox.mark_published(
        revision=1, envelope_sha256="a" * 64
    )
    assert published.state == "published"
    assert outbox.mark_published(
        revision=1, envelope_sha256="a" * 64
    ) == published

    with pytest.raises(PublicationOutboxError):
        outbox.stage(
            revision=3,
            envelope_sha256="d" * 64,
            desired_state_sha256="e" * 64,
            publication_id_sha256="f" * 64,
        )
    with closing(outbox._connect()) as database, database:
        tables = {
            row[0] for row in database.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ) if not row[0].startswith("sqlite_")
        }
        columns = {
            row[1] for row in database.execute(
                "PRAGMA table_info(publication_transport_head)"
            )
        }
    assert tables == {"publication_manifest", "publication_transport_head"}
    assert columns == {
        "singleton", "revision", "envelope_sha256", "desired_state_sha256",
        "publication_id_sha256", "state", "attempt_count", "staged_at",
        "last_attempt_at", "published_at", "superseded_at", "record_sha256",
    }


def test_superseded_tombstone_is_idempotent_and_allows_only_next_revision(
    tmp_path,
):
    outbox = PublicationOutboxStore(
        tmp_path / "publication.sqlite3", clock=lambda: NOW
    )
    outbox.stage(
        revision=1,
        envelope_sha256="a" * 64,
        desired_state_sha256="b" * 64,
        publication_id_sha256="c" * 64,
    )
    outbox.mark_pending(revision=1, envelope_sha256="a" * 64)
    tombstone = outbox.mark_superseded(
        revision=1, envelope_sha256="a" * 64
    )
    assert tombstone.state == "superseded"
    assert tombstone.superseded_at == NOW
    assert outbox.mark_superseded(
        revision=1, envelope_sha256="a" * 64
    ) == tombstone
    with pytest.raises(PublicationOutboxError):
        outbox.record_attempt(revision=1, envelope_sha256="a" * 64)
    with pytest.raises(PublicationOutboxError):
        outbox.stage(
            revision=3,
            envelope_sha256="d" * 64,
            desired_state_sha256="e" * 64,
            publication_id_sha256="f" * 64,
        )
    assert outbox.stage(
        revision=2,
        envelope_sha256="d" * 64,
        desired_state_sha256="e" * 64,
        publication_id_sha256="f" * 64,
    ).state == "staged"


def _create_schema_v1_outbox(path: Path) -> PublicationTransportHead:
    record = PublicationTransportHead(
        revision=1,
        envelope_sha256="a" * 64,
        desired_state_sha256="b" * 64,
        publication_id_sha256="c" * 64,
        state="published",
        attempt_count=1,
        staged_at=NOW - 2,
        last_attempt_at=NOW - 1,
        published_at=NOW,
        superseded_at=None,
    )
    checksum = publication_module._legacy_transport_record_sha256(record)
    with closing(sqlite3.connect(path)) as database, database:
        database.executescript(
            """
            CREATE TABLE publication_manifest (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            ) STRICT;
            CREATE TABLE publication_transport_head (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                revision INTEGER NOT NULL CHECK(revision BETWEEN 1 AND 1000000000),
                envelope_sha256 TEXT NOT NULL CHECK(length(envelope_sha256) = 64),
                desired_state_sha256 TEXT NOT NULL
                    CHECK(length(desired_state_sha256) = 64),
                publication_id_sha256 TEXT NOT NULL
                    CHECK(length(publication_id_sha256) = 64),
                state TEXT NOT NULL CHECK(state IN ('staged','pending','published')),
                attempt_count INTEGER NOT NULL
                    CHECK(attempt_count BETWEEN 0 AND 1000000000),
                staged_at REAL NOT NULL CHECK(staged_at >= 0),
                last_attempt_at REAL CHECK(last_attempt_at IS NULL OR last_attempt_at >= 0),
                published_at REAL CHECK(published_at IS NULL OR published_at >= 0),
                record_sha256 TEXT NOT NULL CHECK(length(record_sha256) = 64)
            ) STRICT;
            """
        )
        database.executemany(
            "INSERT INTO publication_manifest(key,value) VALUES(?,?)",
            (
                ("algorithm_version", "worker-operational-publication-outbox-v1"),
                ("schema_version", "1"),
            ),
        )
        database.execute(
            "INSERT INTO publication_transport_head VALUES(1,?,?,?,?,?,?,?,?,?,?)",
            (
                record.revision,
                record.envelope_sha256,
                record.desired_state_sha256,
                record.publication_id_sha256,
                record.state,
                record.attempt_count,
                record.staged_at,
                record.last_attempt_at,
                record.published_at,
                checksum,
            ),
        )
    return record


def test_schema_v1_outbox_migrates_atomically_to_supersession_capable_v2(
    tmp_path,
):
    path = tmp_path / "publication.sqlite3"
    legacy = _create_schema_v1_outbox(path)

    migrated = PublicationOutboxStore(path).load()

    assert migrated == legacy
    with closing(sqlite3.connect(path)) as database:
        manifest = dict(
            database.execute("SELECT key,value FROM publication_manifest")
        )
        columns = {
            row[1]
            for row in database.execute(
                "PRAGMA table_info(publication_transport_head)"
            )
        }
    assert manifest == {
        "algorithm_version": "worker-operational-publication-outbox-v2",
        "schema_version": "2",
    }
    assert "superseded_at" in columns
    assert not (tmp_path / "publication.sqlite3-journal").exists()


def test_schema_v1_migration_failure_rolls_back_without_partial_schema(tmp_path):
    path = tmp_path / "publication.sqlite3"
    _create_schema_v1_outbox(path)

    def fail(point: str) -> None:
        if point == "migration.after_copy":
            raise RuntimeError("fault")

    with pytest.raises(RuntimeError, match="fault"):
        PublicationOutboxStore(path, fault_injector=fail).load()

    with closing(sqlite3.connect(path)) as database:
        manifest = dict(
            database.execute("SELECT key,value FROM publication_manifest")
        )
        tables = {
            row[0]
            for row in database.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
            if not row[0].startswith("sqlite_")
        }
        columns = {
            row[1]
            for row in database.execute(
                "PRAGMA table_info(publication_transport_head)"
            )
        }
    assert manifest["schema_version"] == "1"
    assert tables == {"publication_manifest", "publication_transport_head"}
    assert "superseded_at" not in columns
    assert PublicationOutboxStore(path).load().state == "published"


def test_outbox_delete_journal_has_no_unbounded_wal_sidecar(tmp_path):
    path = tmp_path / "publication.sqlite3"
    outbox = PublicationOutboxStore(path, clock=lambda: NOW)
    outbox.stage(
        revision=1,
        envelope_sha256="a" * 64,
        desired_state_sha256="b" * 64,
        publication_id_sha256="c" * 64,
    )
    outbox.mark_pending(revision=1, envelope_sha256="a" * 64)
    for _ in range(64):
        outbox.record_attempt(revision=1, envelope_sha256="a" * 64)
    outbox.mark_published(revision=1, envelope_sha256="a" * 64)

    with closing(outbox._connect()) as database, database:
        journal_mode = database.execute("PRAGMA journal_mode").fetchone()[0]
        page_size = int(database.execute("PRAGMA page_size").fetchone()[0])
        maximum_pages = int(
            database.execute("PRAGMA max_page_count").fetchone()[0]
        )

    assert journal_mode == "delete"
    assert maximum_pages == MAX_OUTBOX_DATABASE_BYTES // page_size
    assert path.stat().st_size <= MAX_OUTBOX_DATABASE_BYTES
    assert not Path(f"{path}-wal").exists()
    assert not Path(f"{path}-journal").exists()


def test_outbox_checksum_rejects_semantically_valid_state_tamper(tmp_path):
    path = tmp_path / "publication.sqlite3"
    outbox = PublicationOutboxStore(path, clock=lambda: NOW)
    outbox.stage(
        revision=1,
        envelope_sha256="a" * 64,
        desired_state_sha256="b" * 64,
        publication_id_sha256="c" * 64,
    )
    with closing(sqlite3.connect(path)) as database, database:
        database.execute(
            "UPDATE publication_transport_head SET state='pending' WHERE singleton=1"
        )

    with pytest.raises(PublicationOutboxError, match="checksum"):
        PublicationOutboxStore(path).load()


def test_module_ast_has_no_service_provider_network_or_legacy_wiring():
    root = Path(__file__).parents[1]
    path = root / "storm_tracker_worker" / "operational_snapshot_publication.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imports = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.add(node.module or "")
    forbidden = (
        "service", "homeassistant", "custom_components", "provider", "raster",
        "engine", "requests", "aiohttp", "urllib", "paho", "mqtt",
    )
    assert not [
        imported for imported in imports
        if any(token in imported.lower() for token in forbidden)
    ]
    worker_init = (root / "storm_tracker_worker" / "__init__.py").read_text(
        encoding="utf-8"
    )
    assert "operational_snapshot_publication" not in worker_init
