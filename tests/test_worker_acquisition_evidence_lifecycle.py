from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from pathlib import Path
import hashlib
import sqlite3

import pytest

from storm_tracker_worker import acquisition_scheduler_bridge as bridge
from storm_tracker_worker import autonomous_scheduler
from storm_tracker_worker import desired_state
from storm_tracker_worker import worker_acquisition_plan as acquisition
from storm_tracker_worker.acquisition_evidence_lifecycle import (
    AcquisitionEvidenceCapacityError,
    AcquisitionEvidenceConflict,
    AcquisitionEvidenceIntegrityError,
    AcquisitionEvidenceLifecycleError,
    AcquisitionEvidenceLifecycleStore,
    AcquisitionEvidenceLimits,
    EvidenceRetentionPolicy,
    publish_dwd_completion,
)
from storm_tracker_worker.acquisition_execution_store import ExecutionReceiptStore
from storm_tracker_worker.dwd_rv_capture_handoff import inspect_dwd_rv_archive
from storm_tracker_worker.dwd_rv_native import (
    DWD_ARCHIVE_PRODUCT,
    DWD_CAPTURE_PRODUCT,
    DWD_COLLECTOR,
    DWD_COLLECTOR_VERSION,
    DWD_TAR_INSPECTOR_VERSION,
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


NOW = 1_800_000_000.0
REPOSITORY = Path(__file__).resolve().parents[1]
LIVE_DWD = REPOSITORY / ".analysis" / "dwd-rv-live.tar"
SYNC = lambda _path: None


class FaultOnce:
    def __init__(self, point: str) -> None:
        self.point = point
        self.hit = False

    def __call__(self, point: str) -> None:
        if point == self.point and not self.hit:
            self.hit = True
            raise RuntimeError(f"fault:{point}")


def _store(tmp_path: Path, *, fault=None, limits=None, token="a" * 64):
    return AcquisitionEvidenceLifecycleStore(
        (tmp_path / "lifecycle").resolve(),
        clock=lambda: NOW,
        token_factory=lambda: token,
        limits=limits,
        fault_injector=fault,
        directory_fsync=SYNC,
    )


def _publish(store, suffix="1", *, payload=b"exact-raw", proof=None):
    return store.publish(
        acquisition_job_id=f"job-{suffix}",
        consumer_id="decoder-v1",
        stream_id="radar.test.raw",
        desired_state_sha256="1" * 64,
        authority_sha256="2" * 64,
        proof=proof or {"schema": "test-proof-v1", "sequence": suffix},
        payload=payload,
    )


def _consume(store, receipt, *, output="3" * 64, now=NOW):
    lease = store.claim_next(
        receipt.consumer_id,
        receipt.stream_id,
        "decoder-owner",
        lease_seconds=60.0,
        now=now,
    )
    assert lease is not None
    assert lease.event_id == receipt.event_id
    return store.acknowledge(
        lease.event_id,
        lease.lease_token,
        output_sha256=output,
        now=now + 1.0,
    )


def test_producer_receipt_is_durable_before_ready_and_retry_recovers(tmp_path):
    fault = FaultOnce("publish.after_producer_commit")
    store = _store(tmp_path, fault=fault)
    with pytest.raises(RuntimeError, match="after_producer_commit"):
        _publish(store)

    with closing(sqlite3.connect(store.path)) as db:
        assert db.execute("SELECT count(*) FROM producer_receipts").fetchone()[0] == 1
        assert db.execute("SELECT count(*) FROM completion_outbox").fetchone()[0] == 0

    reopened = _store(tmp_path)
    receipt = _publish(reopened)
    replay = _publish(reopened)
    assert replay == receipt
    lease = reopened.claim_next("decoder-v1", "radar.test.raw", "owner-1", now=NOW)
    assert lease is not None and lease.payload == b"exact-raw"
    assert lease.attempt_count == 1


def test_consumption_receipt_recovers_cursor_without_redelivery(tmp_path):
    fault = FaultOnce("ack.after_receipt_commit")
    store = _store(tmp_path, fault=fault)
    receipt = _publish(store)
    lease = store.claim_next("decoder-v1", "radar.test.raw", "owner-1", now=NOW)
    assert lease is not None
    with pytest.raises(RuntimeError, match="after_receipt_commit"):
        store.acknowledge(
            lease.event_id,
            lease.lease_token,
            output_sha256="3" * 64,
            now=NOW + 1.0,
        )
    terminal = store.terminal_event_identity("radar.test.raw", 1)
    assert terminal is not None
    assert terminal.terminal_kind == "consumed"
    assert terminal.event_id == receipt.event_id
    assert terminal.terminal_output_sha256 == "3" * 64
    assert terminal.terminal_at == NOW + 1.0

    reopened = _store(tmp_path)
    assert reopened.claim_next(
        "decoder-v1", "radar.test.raw", "owner-2", now=NOW + 2.0
    ) is None
    recovered = reopened.acknowledge(
        lease.event_id,
        lease.lease_token,
        output_sha256="3" * 64,
        now=NOW + 2.0,
    )
    assert recovered.event_id == receipt.event_id
    with closing(sqlite3.connect(reopened.path)) as db:
        assert db.execute(
            "SELECT last_sequence FROM consumer_cursors"
        ).fetchone()[0] == 1
        assert db.execute(
            "SELECT count(*) FROM consumption_receipts"
        ).fetchone()[0] == 1


def test_durable_clock_high_water_tracks_observations_not_future_lease(tmp_path):
    store = _store(tmp_path)
    assert store.durable_clock_high_water() == 0.0
    receipt = _publish(store)
    assert store.durable_clock_high_water() == NOW

    lease = store.claim_next(
        receipt.consumer_id,
        receipt.stream_id,
        "clock-owner",
        lease_seconds=600.0,
        now=NOW + 2.0,
    )
    assert lease is not None
    assert lease.lease_expires_at == NOW + 602.0
    assert store.durable_clock_high_water() == NOW + 2.0

    store.acknowledge(
        lease.event_id,
        lease.lease_token,
        output_sha256="3" * 64,
        now=NOW + 3.0,
    )
    assert store.durable_clock_high_water() == NOW + 3.0
    assert _store(tmp_path).durable_clock_high_water() == NOW + 3.0


def test_durable_clock_watermark_is_monotone_and_trigger_bound(tmp_path):
    store = _store(tmp_path)
    _publish(store)
    assert store.durable_clock_high_water() == NOW
    with closing(sqlite3.connect(store.path)) as db:
        with pytest.raises(sqlite3.IntegrityError, match="cannot regress"):
            db.execute(
                "UPDATE lifecycle_clock SET high_water=? WHERE singleton=1",
                (NOW - 1.0,),
            )
        db.rollback()
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            db.execute("DELETE FROM lifecycle_clock WHERE singleton=1")
        db.rollback()
        db.execute("DROP TRIGGER lifecycle_clock_completion_outbox_update")
        db.commit()
    with pytest.raises(
        AcquisitionEvidenceIntegrityError, match="clock trigger authority"
    ):
        store.durable_clock_high_water()


def test_reject_leased_commits_real_rejection_output_before_cursor(tmp_path):
    store = _store(tmp_path)
    producer = _publish(store)
    lease = store.claim_next(
        "decoder-v1", "radar.test.raw", "owner-1", now=NOW
    )
    assert lease is not None

    receipt = store.reject_leased(
        lease.event_id,
        lease.lease_token,
        expected_event_sha256=lease.event_sha256,
        reason_code="desired_state_superseded_before_processing",
        active_desired_state_sha256="4" * 64,
        now=NOW + 1.0,
    )
    assert receipt.event_id == producer.event_id
    assert receipt.output_sha256 not in {
        producer.event_sha256,
        producer.desired_state_sha256,
        "4" * 64,
    }
    terminal = store.terminal_event_identity("radar.test.raw", 1)
    assert terminal is not None
    assert terminal.terminal_kind == "rejected"
    assert terminal.terminal_output_sha256 == receipt.output_sha256
    with closing(sqlite3.connect(store.path)) as db:
        db.row_factory = sqlite3.Row
        rejection = db.execute(
            "SELECT * FROM rejection_receipts WHERE event_id=?",
            (producer.event_id,),
        ).fetchone()
        assert rejection is not None
        assert receipt.output_sha256 == rejection["rejection_receipt_sha256"]
        assert rejection["rejected_desired_state_sha256"] == "1" * 64
        assert rejection["active_desired_state_sha256"] == "4" * 64
        assert rejection["reason_code"] == (
            "desired_state_superseded_before_processing"
        )
        assert db.execute(
            "SELECT state FROM completion_outbox WHERE event_id=?",
            (producer.event_id,),
        ).fetchone()[0] == "consumed"
        assert db.execute(
            "SELECT last_sequence FROM consumer_cursors"
        ).fetchone()[0] == 1
    assert store.diagnostics()["rejection_receipts"] == 1
    assert store.claim_next(
        "decoder-v1", "radar.test.raw", "owner-2", now=NOW + 2.0
    ) is None

    # A coordinator crash can replay the exact operation after the lease and
    # terminal state are gone; it receives the same durable receipt.
    replay = store.reject_leased(
        lease.event_id,
        lease.lease_token,
        expected_event_sha256=lease.event_sha256,
        reason_code="desired_state_superseded_before_processing",
        active_desired_state_sha256="4" * 64,
        now=NOW + 2.0,
    )
    assert replay == receipt


def test_reject_leased_gates_event_lease_reason_and_desired_identity(tmp_path):
    store = _store(tmp_path)
    producer = _publish(store)
    lease = store.claim_next(
        "decoder-v1", "radar.test.raw", "owner-1", now=NOW
    )
    assert lease is not None
    common = {
        "event_id": lease.event_id,
        "lease_token": lease.lease_token,
        "reason_code": "desired_state_changed_before_ack",
        "active_desired_state_sha256": "4" * 64,
        "now": NOW + 1.0,
    }
    with pytest.raises(AcquisitionEvidenceConflict, match="event checksum"):
        store.reject_leased(
            expected_event_sha256="f" * 64,
            **common,
        )
    with pytest.raises(AcquisitionEvidenceConflict, match="lease"):
        store.reject_leased(
            lease.event_id,
            "b" * 64,
            expected_event_sha256=lease.event_sha256,
            reason_code="desired_state_changed_before_ack",
            active_desired_state_sha256="4" * 64,
            now=NOW + 1.0,
        )
    with pytest.raises(AcquisitionEvidenceLifecycleError, match="allowlisted"):
        store.reject_leased(
            lease.event_id,
            lease.lease_token,
            expected_event_sha256=lease.event_sha256,
            reason_code="arbitrary_drop",
            active_desired_state_sha256="4" * 64,
            now=NOW + 1.0,
        )
    with pytest.raises(AcquisitionEvidenceLifecycleError, match="different active"):
        store.reject_leased(
            lease.event_id,
            lease.lease_token,
            expected_event_sha256=lease.event_sha256,
            reason_code="desired_state_changed_before_ack",
            active_desired_state_sha256=producer.desired_state_sha256,
            now=NOW + 1.0,
        )
    with pytest.raises(AcquisitionEvidenceLifecycleError, match="requires an active"):
        store.reject_leased(
            lease.event_id,
            lease.lease_token,
            expected_event_sha256=lease.event_sha256,
            reason_code="desired_state_changed_before_ack",
            active_desired_state_sha256=None,
            now=NOW + 1.0,
        )
    with closing(sqlite3.connect(store.path)) as db:
        assert db.execute("SELECT count(*) FROM rejection_receipts").fetchone()[0] == 0
        assert db.execute("SELECT count(*) FROM consumption_receipts").fetchone()[0] == 0
        assert db.execute(
            "SELECT state FROM completion_outbox WHERE event_id=?",
            (producer.event_id,),
        ).fetchone()[0] == "leased"


@pytest.mark.parametrize(
    "point",
    [
        "reject.before_receipt_commit",
        "reject.after_receipt_commit",
        "reject.before_cursor_commit",
        "reject.after_cursor_commit",
    ],
)
def test_reject_leased_restart_is_exact_once_at_every_commit_window(
    tmp_path, point
):
    fault = FaultOnce(point)
    store = _store(tmp_path, fault=fault)
    producer = _publish(store)
    lease = store.claim_next(
        "decoder-v1", "radar.test.raw", "owner-1", now=NOW
    )
    assert lease is not None
    with pytest.raises(RuntimeError, match=re_escape(point)):
        store.reject_leased(
            lease.event_id,
            lease.lease_token,
            expected_event_sha256=lease.event_sha256,
            reason_code="desired_state_changed_after_processing",
            active_desired_state_sha256="4" * 64,
            now=NOW + 1.0,
        )

    reopened = _store(tmp_path)
    recovered = reopened.reject_leased(
        lease.event_id,
        lease.lease_token,
        expected_event_sha256=lease.event_sha256,
        reason_code="desired_state_changed_after_processing",
        active_desired_state_sha256="4" * 64,
        now=NOW + 2.0,
    )
    assert recovered.event_id == producer.event_id
    with closing(sqlite3.connect(reopened.path)) as db:
        assert db.execute("SELECT count(*) FROM rejection_receipts").fetchone()[0] == 1
        assert db.execute("SELECT count(*) FROM consumption_receipts").fetchone()[0] == 1
        assert db.execute(
            "SELECT last_sequence FROM consumer_cursors"
        ).fetchone()[0] == 1
    assert reopened.claim_next(
        "decoder-v1", "radar.test.raw", "owner-2", now=NOW + 3.0
    ) is None


def test_concurrent_rejection_is_one_durable_identity(tmp_path):
    first = _store(tmp_path, token="a" * 64)
    producer = _publish(first)
    lease = first.claim_next(
        "decoder-v1", "radar.test.raw", "owner-1", now=NOW
    )
    assert lease is not None
    second = _store(tmp_path, token="b" * 64)

    def reject(store):
        return store.reject_leased(
            lease.event_id,
            lease.lease_token,
            expected_event_sha256=lease.event_sha256,
            reason_code="desired_state_superseded_before_processing",
            active_desired_state_sha256="4" * 64,
            now=NOW + 1.0,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        receipts = tuple(pool.map(reject, (first, second)))
    assert receipts[0] == receipts[1]
    assert receipts[0].event_id == producer.event_id
    with closing(sqlite3.connect(first.path)) as db:
        assert db.execute("SELECT count(*) FROM rejection_receipts").fetchone()[0] == 1
        assert db.execute("SELECT count(*) FROM consumption_receipts").fetchone()[0] == 1


def test_concurrent_claim_is_single_lease_and_expiry_replays(tmp_path):
    first = _store(tmp_path, token="a" * 64)
    receipt = _publish(first)
    second = _store(tmp_path, token="b" * 64)

    def claim(store, owner):
        return store.claim_next(
            "decoder-v1", "radar.test.raw", owner,
            lease_seconds=10.0, now=NOW,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        values = tuple(
            pool.map(
                lambda args: claim(*args),
                ((first, "one"), (second, "two")),
            )
        )
    leases = [item for item in values if item is not None]
    assert len(leases) == 1
    replay = second.claim_next(
        "decoder-v1", "radar.test.raw", "two", lease_seconds=10.0,
        now=NOW + 11.0,
    )
    assert replay is not None and replay.event_id == receipt.event_id
    assert replay.attempt_count == 2
    with pytest.raises(AcquisitionEvidenceConflict):
        second.acknowledge(
            replay.event_id,
            "a" * 64,
            output_sha256="3" * 64,
            now=NOW + 12.0,
        )


def test_concurrent_publish_and_archive_use_stable_cas_identity(tmp_path):
    first = _store(tmp_path)
    second = _store(tmp_path)
    with ThreadPoolExecutor(max_workers=2) as pool:
        publications = tuple(pool.map(lambda store: _publish(store), (first, second)))
    assert publications[0] == publications[1]
    receipt = publications[0]
    _consume(first, receipt)

    def rotate(store):
        try:
            return store.rotate_archive(
                policy=EvidenceRetentionPolicy(), now=NOW + 10.0
            )
        except (AcquisitionEvidenceConflict, AcquisitionEvidenceIntegrityError):
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        rotations = tuple(pool.map(rotate, (first, second)))
    assert any(item is not None and item.status == "archived" for item in rotations)
    reopened = _store(tmp_path)
    reopened.resume_pending_prunes()
    assert reopened.lookup_sequence("radar.test.raw", 1).payload == b"exact-raw"
    assert reopened.audit_archive()["archived_completions"] == 1

def test_supersession_is_cas_bound_and_cursor_never_skips_unaccounted_gap(tmp_path):
    store = _store(tmp_path)
    first = _publish(store, "1")
    second = _publish(store, "2")
    with pytest.raises(AcquisitionEvidenceConflict):
        store.supersede(
            first.event_id,
            expected_event_sha256="f" * 64,
            replacement_event_id=second.event_id,
            now=NOW,
        )
    store.supersede(
        first.event_id,
        expected_event_sha256=first.event_sha256,
        replacement_event_id=second.event_id,
        now=NOW,
    )
    lease = store.claim_next("decoder-v1", "radar.test.raw", "owner", now=NOW)
    assert lease is not None and lease.event_id == second.event_id

    broken = _store(tmp_path / "broken")
    _publish(broken, "1")
    _publish(broken, "2")
    with closing(sqlite3.connect(broken.path)) as db:
        event_id = db.execute(
            "SELECT event_id FROM completion_outbox WHERE stream_sequence=1"
        ).fetchone()[0]
        producer_id = db.execute(
            "SELECT producer_receipt_sha256 FROM completion_outbox WHERE event_id=?",
            (event_id,),
        ).fetchone()[0]
        db.execute("DELETE FROM completion_outbox WHERE event_id=?", (event_id,))
        db.execute(
            "DELETE FROM producer_receipts WHERE producer_receipt_sha256=?",
            (producer_id,),
        )
        db.commit()
    with pytest.raises(AcquisitionEvidenceIntegrityError, match="gap"):
        broken.claim_next("decoder-v1", "radar.test.raw", "owner", now=NOW)


def test_secret_size_and_unaccounted_capacity_fail_closed(tmp_path):
    limits = replace(
        AcquisitionEvidenceLimits(),
        max_object_bytes=16,
        max_hot_bytes=16,
        max_hot_objects=2,
        max_proof_bytes=256,
    )
    store = _store(tmp_path, limits=limits)
    with pytest.raises(AcquisitionEvidenceLifecycleError, match="credential"):
        _publish(store, proof={"api_key": "must-not-persist"})
    with pytest.raises(AcquisitionEvidenceCapacityError):
        _publish(store, payload=b"x" * 17)
    orphan = store.hot_directory / ("f" * 64 + ".raw")
    orphan.write_bytes(b"x" * 16)
    with pytest.raises(AcquisitionEvidenceCapacityError, match="hot byte"):
        _publish(store, payload=b"z")
    assert store.diagnostics()["hot_completions"] == 0


@pytest.mark.parametrize("point", ["object.after_write", "object.after_fsync"])
def test_object_staging_is_checksum_recovered_after_restart(tmp_path, point):
    fault = FaultOnce(point)
    store = _store(tmp_path, fault=fault)
    with pytest.raises(RuntimeError, match=re_escape(point)):
        _publish(store)
    reopened = _store(tmp_path)
    receipt = _publish(reopened)
    assert reopened.lookup_sequence(receipt.stream_id, 1).payload == b"exact-raw"
    assert reopened.audit_archive()["hot_objects"] == 1


@pytest.mark.parametrize(
    "point",
    [
        "archive.after_segment_write",
        "archive.after_segment_fsync",
        "archive.after_segment_replace",
    ],
)
def test_segment_publication_restart_reuses_exact_orphan(tmp_path, point):
    fault = FaultOnce(point)
    store = _store(tmp_path, fault=fault)
    receipt = _publish(store)
    _consume(store, receipt)
    with pytest.raises(RuntimeError, match=re_escape(point)):
        store.rotate_archive(policy=EvidenceRetentionPolicy(), now=NOW + 10.0)
    reopened = _store(tmp_path)
    result = reopened.rotate_archive(
        policy=EvidenceRetentionPolicy(), now=NOW + 10.0
    )
    assert result.status == "archived"
    assert reopened.lookup_sequence("radar.test.raw", 1).payload == b"exact-raw"
    assert reopened.audit_archive()["healthy"]


def test_archive_capacity_rejects_whole_rotation_before_copy(tmp_path):
    limits = replace(
        AcquisitionEvidenceLimits(),
        max_archive_bytes=64,
        max_segment_bytes=8 * 1024 * 1024,
    )
    store = _store(tmp_path, limits=limits)
    receipt = _publish(store)
    _consume(store, receipt)
    with pytest.raises(AcquisitionEvidenceCapacityError, match="complete archive"):
        store.rotate_archive(policy=EvidenceRetentionPolicy(), now=NOW + 10.0)
    assert tuple(store.archive_object_directory.iterdir()) == ()
    assert tuple(store.archive_segment_directory.iterdir()) == ()
    assert store.lookup_sequence("radar.test.raw", 1).status == "hot"


@pytest.mark.parametrize(
    "point",
    [
        "archive.after_manifest_commit",
        "prune.after_phase_0_commit",
        "prune.after_phase_1_commit",
        "prune.after_phase_2_commit",
        "prune.after_phase_3_commit",
        "prune.after_hot_object_unlink",
        "prune.after_file_phase_commit",
        "prune.after_phase_5_commit",
    ],
)
def test_archive_and_child_first_prune_resume_every_faultpoint(tmp_path, point):
    fault = FaultOnce(point)
    store = _store(tmp_path, fault=fault)
    receipt = _publish(store)
    _consume(store, receipt)
    with pytest.raises(RuntimeError, match=re_escape(point)):
        store.rotate_archive(policy=EvidenceRetentionPolicy(), now=NOW + 10.0)
    assert fault.hit

    reopened = _store(tmp_path)
    reopened.resume_pending_prunes()
    lookup = reopened.lookup_sequence("radar.test.raw", 1)
    assert lookup.status == "archived" and lookup.payload == b"exact-raw"
    expired = reopened.lookup_sequence(
        "radar.test.raw", 1, include_archive=False
    )
    assert expired.status == "window_expired"
    audit = reopened.audit_archive()
    assert audit["healthy"] and audit["hot_completions"] == 0
    assert _publish(reopened) == receipt
    assert reopened.audit_archive()["hot_objects"] == 0


def re_escape(value: str) -> str:
    return value.replace(".", r"\.")


def test_archive_tamper_and_unknown_lookup_are_explicit(tmp_path):
    store = _store(tmp_path)
    receipt = _publish(store)
    _consume(store, receipt)
    store.rotate_archive(policy=EvidenceRetentionPolicy(), now=NOW + 10.0)
    archived_receipt = store.acknowledge(
        receipt.event_id,
        "a" * 64,
        output_sha256="3" * 64,
        now=NOW + 11.0,
    )
    assert archived_receipt.event_id == receipt.event_id
    with pytest.raises(AcquisitionEvidenceConflict, match="archived consumer"):
        store.acknowledge(
            receipt.event_id,
            "a" * 64,
            output_sha256="4" * 64,
            now=NOW + 11.0,
        )
    assert store.lookup_sequence("radar.test.raw", 2).status == "unknown"
    archived = store.lookup_sequence("radar.test.raw", 1)
    object_path = store.archive_object_directory / (
        hashlib.sha256(b"exact-raw").hexdigest() + ".raw"
    )
    object_path.write_bytes(b"tampered!!")
    with pytest.raises(AcquisitionEvidenceIntegrityError):
        store.lookup_sequence("radar.test.raw", 1)
    object_path.write_bytes(archived.payload)
    segment = next(store.archive_segment_directory.iterdir())
    segment.write_bytes(segment.read_bytes() + b" ")
    with pytest.raises(AcquisitionEvidenceIntegrityError):
        store.audit_archive()


def test_rejection_receipt_is_archived_pruned_and_replayable(tmp_path):
    store = _store(tmp_path)
    producer = _publish(store)
    lease = store.claim_next(
        "decoder-v1", "radar.test.raw", "owner-1", now=NOW
    )
    assert lease is not None
    receipt = store.reject_leased(
        lease.event_id,
        lease.lease_token,
        expected_event_sha256=lease.event_sha256,
        reason_code="active_desired_state_unavailable",
        active_desired_state_sha256=None,
        now=NOW + 1.0,
    )
    rotated = store.rotate_archive(
        policy=EvidenceRetentionPolicy(), now=NOW + 10.0
    )
    assert rotated.status == "archived"
    assert store.durable_clock_high_water() == NOW + 10.0
    assert store.lookup_sequence("radar.test.raw", 1).status == "archived"
    terminal = store.terminal_event_identity("radar.test.raw", 1)
    assert terminal is not None
    assert terminal.status == "archived"
    assert terminal.terminal_kind == "rejected"
    assert terminal.terminal_output_sha256 == receipt.output_sha256
    with closing(sqlite3.connect(store.path)) as db:
        assert db.execute("SELECT count(*) FROM rejection_receipts").fetchone()[0] == 0
        assert db.execute("SELECT count(*) FROM consumption_receipts").fetchone()[0] == 0
    replay = store.reject_leased(
        producer.event_id,
        lease.lease_token,
        expected_event_sha256=producer.event_sha256,
        reason_code="active_desired_state_unavailable",
        active_desired_state_sha256=None,
        now=NOW + 20.0,
    )
    assert replay == receipt
    with pytest.raises(AcquisitionEvidenceConflict, match="rejection receipt conflicts"):
        store.reject_leased(
            producer.event_id,
            lease.lease_token,
            expected_event_sha256=producer.event_sha256,
            reason_code="desired_state_changed_before_ack",
            active_desired_state_sha256="4" * 64,
            now=NOW + 20.0,
        )
    assert store.audit_archive()["healthy"]


def test_tampered_hot_rejection_receipt_fails_closed(tmp_path):
    store = _store(tmp_path)
    producer = _publish(store)
    lease = store.claim_next(
        "decoder-v1", "radar.test.raw", "owner-1", now=NOW
    )
    assert lease is not None
    store.reject_leased(
        lease.event_id,
        lease.lease_token,
        expected_event_sha256=lease.event_sha256,
        reason_code="desired_state_changed_before_ack",
        active_desired_state_sha256="4" * 64,
        now=NOW + 1.0,
    )
    with closing(sqlite3.connect(store.path)) as db:
        db.execute(
            "UPDATE rejection_receipts SET active_desired_state_sha256=? "
            "WHERE event_id=?",
            ("5" * 64, producer.event_id),
        )
        db.commit()
    with pytest.raises(AcquisitionEvidenceIntegrityError, match="checksum"):
        store.reject_leased(
            producer.event_id,
            lease.lease_token,
            expected_event_sha256=producer.event_sha256,
            reason_code="desired_state_changed_before_ack",
            active_desired_state_sha256="4" * 64,
            now=NOW + 2.0,
        )


def test_missing_known_bytes_are_not_silently_recreated(tmp_path):
    hot = _store(tmp_path / "hot")
    receipt = _publish(hot)
    hot_path = hot.hot_directory / f"{receipt.content_sha256}.raw"
    hot_path.unlink()
    with pytest.raises(AcquisitionEvidenceIntegrityError, match="missing"):
        _publish(hot)
    assert not hot_path.exists()

    archived = _store(tmp_path / "archive")
    first = _publish(archived, "1")
    _consume(archived, first)
    archived.rotate_archive(policy=EvidenceRetentionPolicy(), now=NOW + 10.0)
    archive_path = (
        archived.archive_object_directory / f"{first.content_sha256}.raw"
    )
    archive_path.unlink()
    second = _publish(archived, "2")
    _consume(archived, second, now=NOW + 20.0)
    with pytest.raises(AcquisitionEvidenceIntegrityError, match="archive object"):
        archived.rotate_archive(
            policy=EvidenceRetentionPolicy(), now=NOW + 30.0
        )
    assert not archive_path.exists()


def _desired() -> desired_state.DesiredState:
    return desired_state.parse_desired_state({
        "authority_epoch": 1,
        "contract": desired_state.CONTRACT,
        "generation": 1,
        "issued_at": NOW - 60.0,
        "previous_state_sha256": None,
        "producer_id": "ha-dwd-lifecycle-test",
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": True,
            "region_sharing_distance_km": 150.0,
        },
        "targets": [{
            "available": True,
            "country_code": "BE",
            "display_name": "Home",
            "kind": "home",
            "latitude": 51.04,
            "location_accuracy_m": None,
            "longitude": 4.48,
            "observation_radius_km": 250.0,
            "position_observed_at": NOW - 60.0,
            "target_id": "zone.home",
        }],
    })


def _dwd_authorities(tmp_path: Path):
    state = _desired()
    plan = acquisition.build_worker_acquisition_plan(state)
    jobs = acquisition.build_worker_acquisition_slot_jobs(plan, as_of=NOW)
    job = next(item for item in jobs if item.workload_id == "radar.dwd.radolan_rv")
    schedule = next(
        item
        for item in bridge.build_acquisition_schedule_intents(plan)
        if item.intent_id == job.intent_id
    )
    desired_store = desired_state.DesiredStateStore(
        (tmp_path / "desired.sqlite3").resolve()
    )
    desired_store.claim_producer(
        state.producer_id, "dwd-lifecycle-claim", claimed_at=NOW - 100.0
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

    payload = LIVE_DWD.read_bytes()
    inventory = inspect_dwd_rv_archive(payload)
    raw_limits = RawArtifactLimits(
        max_object_bytes=8 * 1024 * 1024,
        max_total_bytes=16 * 1024 * 1024,
        max_object_entries=8,
        max_staging_entries=4,
        max_receipt_entries=16,
        max_capture_entries=16,
        max_capture_members=4,
        max_catalog_bytes=4 * 1024 * 1024,
    )
    source = RawArtifactStore((tmp_path / "source-raw").resolve(), limits=raw_limits)
    source.initialize()
    raw_receipt = source.store(
        provider="dwd",
        product=DWD_ARCHIVE_PRODUCT,
        fetch_id=f"{job.job_id}:rv_tar",
        content_type="application/x-tar",
        received_at=NOW - 10.0,
        upstream_id=f"{DWD_ARCHIVE_PRODUCT}|latest|content_sha256={hashlib.sha256(payload).hexdigest()}",
        provenance={
            "capture_stage": NATIVE_CAPTURE_STAGE,
            "collector": DWD_COLLECTOR,
            "collector_version": DWD_COLLECTOR_VERSION,
            "provenance_scope": "exact_upstream_tar_bytes_only",
            "representation": NATIVE_REPRESENTATION,
            "structure_inspector_version": DWD_TAR_INSPECTOR_VERSION,
        },
        payload=payload,
    )
    capture = source.commit_capture(
        provider="dwd",
        product=DWD_CAPTURE_PRODUCT,
        fetch_id=f"{job.job_id}:capture",
        upstream_capture_id=(
            f"{DWD_CAPTURE_PRODUCT}|member={inventory.observation.member_name}"
            f"|content_sha256={raw_receipt.content_sha256}"
        ),
        product_timestamp=inventory.base_time,
        members={"rv_tar": raw_receipt.receipt_id},
    )
    profile = RawCapacityProfile(
        global_max_bytes=16 * 1024 * 1024,
        global_max_entries=16,
        providers=(ProviderCapacityLimit("dwd", 16 * 1024 * 1024, 16),),
        max_reservations=16,
        max_catalog_bytes=4 * 1024 * 1024,
        sqlite_busy_timeout_seconds=2.0,
    )
    ledger = RawCapacityLedger(
        (tmp_path / "capacity.sqlite3").resolve(), profile=profile,
        clock=lambda: NOW,
    )
    ledger.reserve(
        reservation_id="dwd-lifecycle-reservation",
        provider="dwd",
        job_identity=job.job_id,
        requested_bytes=8 * 1024 * 1024,
        requested_entries=1,
    )
    capacity = ledger.commit(
        reservation_id="dwd-lifecycle-reservation",
        actual_bytes=len(payload),
        actual_entries=1,
        content_sha256=raw_receipt.content_sha256,
        receipt_sha256=raw_receipt.receipt_sha256,
        capture_sha256=capture.capture_receipt_sha256,
    )
    executions = ExecutionReceiptStore(
        (tmp_path / "execution.sqlite3").resolve(), clock=lambda: NOW
    )
    executions.record(
        scheduler_job_id=claim.job_id,
        acquisition_job_id=job.job_id,
        binding_sha256="b" * 64,
        attempt_count=claim.attempt_count,
        outcome="completed_shadow_raw",
        reason_code="shadow_raw_capture_complete",
        terminal=True,
        content_sha256=raw_receipt.content_sha256,
        raw_receipt_sha256=raw_receipt.receipt_sha256,
        capture_sha256=capture.capture_receipt_sha256,
        capacity_commit_sha256=capacity.commit_sha256,
    )
    scheduler.complete(claim.job_id, claim.lease_token, now=NOW + 1.0)
    handoff = RawArtifactStore((tmp_path / "handoff-raw").resolve(), limits=raw_limits)
    handoff.initialize()
    return state, plan, job, scheduler, executions, ledger, source, handoff, capture, payload


def test_real_dwd_raw_v2_completion_is_exact_and_archive_replayable(tmp_path):
    (
        state, plan, job, scheduler, executions, ledger, source, handoff,
        capture, payload,
    ) = _dwd_authorities(tmp_path)
    lifecycle = _store(tmp_path)
    receipt = publish_dwd_completion(
        lifecycle_store=lifecycle,
        acquisition_plan=plan,
        desired_state=state,
        scheduler=scheduler,
        execution_store=executions,
        capacity_ledger=ledger,
        source_raw_store=source,
        handoff_raw_store=handoff,
        source_capture_id=capture.capture_id,
        acquisition_job_id=job.job_id,
    )
    lease = lifecycle.claim_next(
        "dwd-rv-quantitative-decoder-v1",
        "radar.dwd.radolan_rv",
        "decoder-owner",
        now=NOW + 2.0,
    )
    assert lease is not None and lease.payload == payload
    assert lease.proof["schema"] == "stv3-worker-dwd-rv-completion-proof-v1"
    assert lease.proof["meteorological_claim"] is False
    assert lease.proof["raw_refetch_performed"] is False
    assert lease.proof["scheduler_terminal"]["job_id"] == scheduler.terminal_proof(
        executions.load_terminal(job.job_id).scheduler_job_id
    ).job_id
    lifecycle.acknowledge(
        lease.event_id,
        lease.lease_token,
        output_sha256="d" * 64,
        now=NOW + 3.0,
    )
    lifecycle.rotate_archive(policy=EvidenceRetentionPolicy(), now=NOW + 4.0)
    replay = publish_dwd_completion(
        lifecycle_store=lifecycle,
        acquisition_plan=plan,
        desired_state=state,
        scheduler=scheduler,
        execution_store=executions,
        capacity_ledger=ledger,
        source_raw_store=source,
        handoff_raw_store=handoff,
        source_capture_id=capture.capture_id,
        acquisition_job_id=job.job_id,
    )
    assert replay == receipt
    assert lifecycle.lookup_sequence("radar.dwd.radolan_rv", 1).payload == payload
    assert lifecycle.audit_archive()["healthy"]


def test_dwd_wrong_desired_authority_fails_before_ready(tmp_path):
    (
        state, plan, job, scheduler, executions, ledger, source, handoff,
        capture, _payload,
    ) = _dwd_authorities(tmp_path)
    forged = desired_state.parse_desired_state({
        **state.as_dict(),
        "generation": 2,
        "issued_at": state.issued_at + 1.0,
    })
    lifecycle = _store(tmp_path)
    with pytest.raises(AcquisitionEvidenceConflict, match="desired"):
        publish_dwd_completion(
            lifecycle_store=lifecycle,
            acquisition_plan=plan,
            desired_state=forged,
            scheduler=scheduler,
            execution_store=executions,
            capacity_ledger=ledger,
            source_raw_store=source,
            handoff_raw_store=handoff,
            source_capture_id=capture.capture_id,
            acquisition_job_id=job.job_id,
        )
    assert lifecycle.diagnostics()["hot_completions"] == 0


def test_lifecycle_remains_unwired_from_service_and_operational_publishers():
    service = (REPOSITORY / "storm_tracker_worker" / "service.py").read_text(
        encoding="utf-8"
    )
    assert "acquisition_evidence_lifecycle" not in service
    source = (
        REPOSITORY / "storm_tracker_worker" / "acquisition_evidence_lifecycle.py"
    ).read_text(encoding="utf-8")
    assert "mqtt" not in source.casefold()
    assert "operational_snapshot" not in source
    assert "capture_latest_dwd_rv_tar" not in source
