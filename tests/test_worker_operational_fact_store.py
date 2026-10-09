from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import sqlite3

import pytest

from storm_tracker_worker.desired_state import parse_desired_state
from storm_tracker_worker.operational_fact_revision import (
    CommittedOperationalFactRevision,
)
from storm_tracker_worker.operational_fact_store import (
    OperationalFactRevisionStore,
    OperationalFactStoreCapacityError,
    OperationalFactStoreConflict,
    OperationalFactStoreCorruption,
    OperationalFactStoreError,
    OperationalFactStoreExpired,
)
from storm_tracker_worker.operational_snapshot_builder import (
    build_unknown_operational_snapshot,
)
from storm_tracker_worker.tracking_core.operational_snapshot import (
    canonical_json_bytes,
    encode_control_envelope,
)


NOW = 1_800_400_000.0


def _desired(*, generation: int = 1, latitude: float = 51.0):
    return parse_desired_state({
        "authority_epoch": 4,
        "contract": "stv3-worker-desired-state-v1",
        "generation": generation,
        "issued_at": NOW - 60.0 + generation,
        "previous_state_sha256": None,
        "producer_id": "ha:fact-store-test",
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": True,
            "region_sharing_distance_km": 100.0,
        },
        "targets": [{
            "available": True,
            "country_code": "BE",
            "display_name": "Thuis",
            "kind": "home",
            "latitude": latitude,
            "location_accuracy_m": 10.0,
            "longitude": 4.4,
            "observation_radius_km": 250.0,
            "position_observed_at": NOW - 30.0,
            "target_id": "home",
        }],
    })


def _revision(
    desired,
    *,
    snapshot_revision: int = 1,
    evaluated_at: float = NOW,
    status_text: str = "",
):
    snapshot = build_unknown_operational_snapshot(
        desired,
        revision=snapshot_revision,
        evaluated_at=evaluated_at,
        worker_state="online",
        queue_depth=0,
        failed_jobs=0,
        worker_status_text=status_text,
    )
    return CommittedOperationalFactRevision.create(
        desired_state_sha256=desired.state_sha256,
        authority_epoch=desired.authority_epoch,
        snapshot=snapshot,
        admission_receipts=(),
    )


def _snapshot_sha(revision) -> str:
    import hashlib

    return hashlib.sha256(canonical_json_bytes(revision.snapshot)).hexdigest()


def _envelope_sha(revision) -> str:
    import hashlib

    return hashlib.sha256(encode_control_envelope(revision.snapshot)).hexdigest()


def test_stage_load_publish_and_snapshot_lookup_are_exact_and_idempotent(tmp_path):
    desired = _desired()
    revision = _revision(desired)
    store = OperationalFactRevisionStore(
        tmp_path / "facts.sqlite3",
        clock=lambda: NOW + 1.0,
    )

    staged = store.stage(
        "pipeline:dwd:0001",
        desired_state=desired,
        revision=revision,
    )
    duplicate = store.stage(
        "pipeline:dwd:0001",
        desired_state=desired,
        revision=revision,
    )

    assert staged == duplicate
    assert staged.state == "ready"
    assert staged.revision == revision
    assert store.next_ready(desired) == staged

    committed = store.mark_snapshot_committed(
        staged.pipeline_item_id,
        revision_sha256=revision.revision_sha256,
        snapshot_sha256=_snapshot_sha(revision),
        publication_envelope_sha256=_envelope_sha(revision),
    )
    repeated = store.mark_snapshot_committed(
        staged.pipeline_item_id,
        revision_sha256=revision.revision_sha256,
        snapshot_sha256=_snapshot_sha(revision),
        publication_envelope_sha256=_envelope_sha(revision),
    )

    assert committed == repeated
    assert committed.state == "snapshot_committed"
    assert store.next_ready(desired) is None
    assert store.committed_for_snapshot(_snapshot_sha(revision)) == committed
    assert store.audit() == {
        "contract": "stv3-operational-fact-store-v1",
        "snapshot_committed": 1,
        "ready": 0,
        "retained_rows": 1,
        "status": "ok",
        "superseded": 0,
    }


def test_restart_reconciles_exact_durable_snapshot_head_before_pipeline_ack(tmp_path):
    desired = _desired()
    revision = _revision(desired)
    store = OperationalFactRevisionStore(tmp_path / "facts.sqlite3")
    staged = store.stage(
        "pipeline:dwd:recovery",
        desired_state=desired,
        revision=revision,
    )
    envelope = encode_control_envelope(revision.snapshot)
    head = {
        "canonical_envelope": envelope,
        "envelope_sha256": _envelope_sha(revision),
        "revision": revision.snapshot["revision"],
        "snapshot": revision.snapshot,
        "snapshot_sha256": _snapshot_sha(revision),
    }

    recovered = OperationalFactRevisionStore(store.path).reconcile_snapshot_head(head)

    assert recovered is not None
    assert recovered.pipeline_item_id == staged.pipeline_item_id
    assert recovered.state == "snapshot_committed"
    assert OperationalFactRevisionStore(store.path).reconcile_snapshot_head(head) == recovered

    tampered = {**head, "envelope_sha256": "f" * 64}
    with pytest.raises(OperationalFactStoreCorruption, match="envelopehash"):
        store.reconcile_snapshot_head(tampered)


def test_unknown_snapshot_head_has_no_fact_link(tmp_path):
    desired = _desired()
    revision = _revision(desired)
    envelope = encode_control_envelope(revision.snapshot)
    store = OperationalFactRevisionStore(tmp_path / "facts.sqlite3")

    assert store.reconcile_snapshot_head({
        "canonical_envelope": envelope,
        "envelope_sha256": _envelope_sha(revision),
        "revision": revision.snapshot["revision"],
        "snapshot": revision.snapshot,
        "snapshot_sha256": _snapshot_sha(revision),
    }) is None


def test_pipeline_identity_slot_and_terminal_transitions_are_fail_closed(tmp_path):
    desired = _desired()
    first = _revision(desired, status_text="first")
    second = _revision(desired, status_text="second")
    store = OperationalFactRevisionStore(tmp_path / "facts.sqlite3")
    staged = store.stage("item:first", desired_state=desired, revision=first)

    with pytest.raises(OperationalFactStoreConflict, match="andere revision"):
        store.stage("item:first", desired_state=desired, revision=second)
    with pytest.raises(OperationalFactStoreConflict, match="snapshotrevision"):
        store.stage("item:second", desired_state=desired, revision=second)

    superseded = store.mark_superseded(
        staged.pipeline_item_id,
        revision_sha256=staged.revision_sha256,
    )
    assert superseded.state == "superseded"
    assert store.mark_superseded(
        staged.pipeline_item_id,
        revision_sha256=staged.revision_sha256,
    ) == superseded
    with pytest.raises(OperationalFactStoreConflict, match="ready"):
        store.mark_snapshot_committed(
            staged.pipeline_item_id,
            revision_sha256=staged.revision_sha256,
            snapshot_sha256=staged.snapshot_sha256,
            publication_envelope_sha256=_envelope_sha(first),
        )


def test_stage_revalidates_current_desired_authority_and_rejects_secrets(tmp_path):
    desired = _desired()
    changed = _desired(generation=2, latitude=51.1)
    store = OperationalFactRevisionStore(tmp_path / "facts.sqlite3")

    with pytest.raises(OperationalFactStoreError, match="niet admitted"):
        store.stage(
            "item:stale",
            desired_state=changed,
            revision=_revision(desired),
        )

    secret = _revision(desired, status_text="api_key=do-not-persist")
    with pytest.raises(OperationalFactStoreError, match="credentialvormige"):
        store.stage("item:secret", desired_state=desired, revision=secret)
    assert not store.path.exists() or b"do-not-persist" not in store.path.read_bytes()


def test_ready_revision_must_still_cover_store_clock(tmp_path):
    desired = _desired()
    clock = [NOW + 1.0]
    store = OperationalFactRevisionStore(
        tmp_path / "facts.sqlite3",
        clock=lambda: clock[0],
    )
    store.stage(
        "item:expiring",
        desired_state=desired,
        revision=_revision(desired, evaluated_at=NOW),
    )

    clock[0] = NOW + 300.0
    with pytest.raises(OperationalFactStoreExpired, match="verlopen"):
        store.next_ready(desired)


def test_expected_snapshot_revision_and_control_authority_are_rechecked(tmp_path):
    desired = _desired()
    store = OperationalFactRevisionStore(tmp_path / "facts.sqlite3")
    staged = store.stage(
        "item:authority",
        desired_state=desired,
        revision=_revision(desired, snapshot_revision=2),
    )

    with pytest.raises(OperationalFactStoreConflict, match="snapshothead"):
        store.next_ready(desired, expected_snapshot_revision=1)
    assert store.next_ready(desired, expected_snapshot_revision=2) == staged

    replacement = _desired(generation=2, latitude=51.1)
    assert store.supersede_obsolete(replacement) == 1
    assert store.load(staged.pipeline_item_id).state == "superseded"
    assert store.supersede_obsolete(replacement) == 0


def test_publication_selector_terminalizes_stale_and_future_rows_but_never_skips_ahead(
    tmp_path,
):
    desired = _desired()
    clock = [NOW]
    stale_store = OperationalFactRevisionStore(
        tmp_path / "stale.sqlite3",
        clock=lambda: clock[0],
    )
    stale = stale_store.stage(
        "item:stale-revision",
        desired_state=desired,
        revision=_revision(desired, snapshot_revision=1),
    )
    assert stale_store.select_for_publication(
        desired,
        expected_snapshot_revision=2,
    ) is None
    assert stale_store.load(stale.pipeline_item_id).terminal_reason == (
        "snapshot_revision_superseded"
    )

    future_store = OperationalFactRevisionStore(
        tmp_path / "future.sqlite3",
        clock=lambda: clock[0],
    )
    clock[0] = NOW - 1.0
    future = future_store.stage(
        "item:future-clock",
        desired_state=desired,
        revision=_revision(desired, snapshot_revision=1),
    )
    assert future_store.select_for_publication(
        desired,
        expected_snapshot_revision=1,
    ) is None
    assert future_store.load(future.pipeline_item_id).terminal_reason == (
        "fact_revision_future_clock"
    )

    clock[0] = NOW
    ahead_store = OperationalFactRevisionStore(
        tmp_path / "ahead.sqlite3",
        clock=lambda: clock[0],
    )
    ahead_store.stage(
        "item:ahead",
        desired_state=desired,
        revision=_revision(desired, snapshot_revision=3),
    )
    with pytest.raises(OperationalFactStoreConflict, match="voor de snapshothead"):
        ahead_store.select_for_publication(
            desired,
            expected_snapshot_revision=2,
        )


def test_bounded_retention_prunes_only_terminal_prefix(tmp_path):
    desired = _desired()
    store = OperationalFactRevisionStore(
        tmp_path / "facts.sqlite3",
        clock=lambda: NOW + 1.0,
        retention_rows=2,
    )
    first = _revision(desired, snapshot_revision=1)
    second = _revision(desired, snapshot_revision=2)
    third = _revision(desired, snapshot_revision=3)

    first_record = store.stage("item:1", desired_state=desired, revision=first)
    store.mark_snapshot_committed(
        "item:1",
        revision_sha256=first.revision_sha256,
        snapshot_sha256=_snapshot_sha(first),
        publication_envelope_sha256=_envelope_sha(first),
    )
    store.stage("item:2", desired_state=desired, revision=second)
    store.mark_superseded("item:2", revision_sha256=second.revision_sha256)
    store.stage("item:3", desired_state=desired, revision=third)

    with pytest.raises(OperationalFactStoreConflict, match="niet bewaard"):
        store.load(first_record.pipeline_item_id)
    diagnostics = store.diagnostics()
    assert diagnostics["first_retained_sequence"] == 2
    assert diagnostics["head_sequence"] == 3
    assert diagnostics["retained_rows"] == 2

    blocked = OperationalFactRevisionStore(
        tmp_path / "blocked.sqlite3",
        clock=lambda: NOW + 1.0,
        retention_rows=2,
    )
    blocked.stage("blocked:1", desired_state=desired, revision=first)
    blocked.stage("blocked:2", desired_state=desired, revision=second)
    with pytest.raises(OperationalFactStoreCapacityError, match="niet-terminale"):
        blocked.stage("blocked:3", desired_state=desired, revision=third)
    assert blocked.diagnostics()["retained_rows"] == 2


def test_supersede_all_ready_terminalizes_only_unpublished_rows(tmp_path):
    desired = _desired()
    store = OperationalFactRevisionStore(
        tmp_path / "facts.sqlite3",
        clock=lambda: NOW + 1.0,
        retention_rows=8,
    )
    committed_revision = _revision(desired, snapshot_revision=1)
    ready_revision_2 = _revision(desired, snapshot_revision=2)
    ready_revision_3 = _revision(desired, snapshot_revision=3)

    store.stage(
        "item:committed",
        desired_state=desired,
        revision=committed_revision,
    )
    store.mark_snapshot_committed(
        "item:committed",
        revision_sha256=committed_revision.revision_sha256,
        snapshot_sha256=_snapshot_sha(committed_revision),
        publication_envelope_sha256=_envelope_sha(committed_revision),
    )
    store.stage(
        "item:ready:2",
        desired_state=desired,
        revision=ready_revision_2,
    )
    store.stage(
        "item:ready:3",
        desired_state=desired,
        revision=ready_revision_3,
    )

    assert store.supersede_all_ready() == 2
    assert store.load("item:committed").state == "snapshot_committed"
    for item_id in ("item:ready:2", "item:ready:3"):
        record = store.load(item_id)
        assert record.state == "superseded"
        assert record.terminal_reason == "desired_state_unavailable"
    assert store.supersede_all_ready() == 0


@pytest.mark.parametrize("fault_point", ("stage.after_insert", "stage.after_state"))
def test_stage_fault_rolls_back_row_and_checkpoint(tmp_path, fault_point):
    desired = _desired()

    def fault(point):
        if point == fault_point:
            raise RuntimeError("simulated crash")

    path = tmp_path / "facts.sqlite3"
    store = OperationalFactRevisionStore(path, fault_injector=fault)
    with pytest.raises(RuntimeError, match="simulated"):
        store.stage(
            "item:crash",
            desired_state=desired,
            revision=_revision(desired),
        )

    reopened = OperationalFactRevisionStore(path)
    assert reopened.diagnostics()["head_sequence"] == 0
    assert reopened.diagnostics()["retained_rows"] == 0


def test_publish_fault_rolls_back_terminal_transition(tmp_path):
    desired = _desired()
    path = tmp_path / "facts.sqlite3"
    setup = OperationalFactRevisionStore(path)
    revision = _revision(desired)
    setup.stage("item:publish", desired_state=desired, revision=revision)

    def fault(point):
        if point == "publish.before_commit":
            raise RuntimeError("simulated crash")

    crashing = OperationalFactRevisionStore(path, fault_injector=fault)
    with pytest.raises(RuntimeError, match="simulated"):
        crashing.mark_snapshot_committed(
            "item:publish",
            revision_sha256=revision.revision_sha256,
            snapshot_sha256=_snapshot_sha(revision),
            publication_envelope_sha256=_envelope_sha(revision),
        )
    assert OperationalFactRevisionStore(path).load("item:publish").state == "ready"


def test_tamper_of_canonical_revision_or_record_checksum_is_detected(tmp_path):
    desired = _desired()
    path = tmp_path / "facts.sqlite3"
    store = OperationalFactRevisionStore(path)
    store.stage("item:tamper", desired_state=desired, revision=_revision(desired))

    with closing(sqlite3.connect(path)) as db, db:
        db.execute(
            "UPDATE fact_revisions SET canonical_revision=? WHERE pipeline_item_id=?",
            (b"{}", "item:tamper"),
        )
    with pytest.raises(OperationalFactStoreCorruption):
        OperationalFactRevisionStore(path)


def test_two_store_instances_allow_only_one_candidate_for_same_snapshot_slot(tmp_path):
    desired = _desired()
    path = tmp_path / "facts.sqlite3"
    OperationalFactRevisionStore(path)
    revisions = (
        _revision(desired, status_text="candidate-a"),
        _revision(desired, status_text="candidate-b"),
    )

    def stage(index: int):
        store = OperationalFactRevisionStore(path)
        try:
            return store.stage(
                f"item:concurrent:{index}",
                desired_state=desired,
                revision=revisions[index],
            ).pipeline_item_id
        except OperationalFactStoreConflict:
            return "conflict"

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(executor.map(stage, (0, 1)))

    assert results.count("conflict") == 1
    assert OperationalFactRevisionStore(path).diagnostics()["retained_rows"] == 1
