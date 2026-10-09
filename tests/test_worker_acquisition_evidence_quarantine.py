from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sqlite3

import pytest

from storm_tracker_worker.acquisition_evidence_lifecycle import (
    ARCHIVED_COMPLETION_SCHEMA,
    LEGACY_ARCHIVED_COMPLETION_SCHEMA,
    LEGACY_ARCHIVED_COMPLETION_SCHEMA_V2,
    AcquisitionEvidenceConflict,
    AcquisitionEvidenceIntegrityError,
    AcquisitionEvidenceLifecycleError,
    AcquisitionEvidenceLifecycleStore,
    EvidenceRetentionPolicy,
    QUARANTINE_REASON_CODES,
)


NOW = 1_800_000_000.0
SYNC = lambda _path: None
FINGERPRINT = "4" * 64


def _mapping_sha(value: dict) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    ).hexdigest()


class FaultOnce:
    def __init__(self, point: str) -> None:
        self.point = point
        self.hit = False

    def __call__(self, point: str) -> None:
        if point == self.point and not self.hit:
            self.hit = True
            raise RuntimeError(f"fault:{point}")


def _store(
    tmp_path: Path,
    *,
    fault=None,
    token: str = "a" * 64,
) -> AcquisitionEvidenceLifecycleStore:
    return AcquisitionEvidenceLifecycleStore(
        (tmp_path / "lifecycle").resolve(),
        clock=lambda: NOW,
        token_factory=lambda: token,
        fault_injector=fault,
        directory_fsync=SYNC,
    )


def _publish(store: AcquisitionEvidenceLifecycleStore, suffix: str = "1"):
    return store.publish(
        acquisition_job_id=f"job-{suffix}",
        consumer_id="decoder-v1",
        stream_id="radar.test.raw",
        desired_state_sha256="1" * 64,
        authority_sha256="2" * 64,
        proof={"schema": "test-proof-v1", "sequence": suffix},
        payload=f"exact-raw-{suffix}".encode(),
    )


def _lease(store: AcquisitionEvidenceLifecycleStore, *, now: float = NOW):
    lease = store.claim_next(
        "decoder-v1",
        "radar.test.raw",
        "decoder-owner",
        lease_seconds=60.0,
        now=now,
    )
    assert lease is not None
    return lease


def _quarantine(
    store: AcquisitionEvidenceLifecycleStore,
    lease,
    *,
    reason: str = "source_raw_integrity_failed",
    fingerprint: str = FINGERPRINT,
    now: float = NOW + 1.0,
):
    return store.quarantine_leased(
        lease.event_id,
        lease.lease_token,
        expected_event_sha256=lease.event_sha256,
        reason_code=reason,
        evidence_fingerprint_sha256=fingerprint,
        now=now,
    )


def test_quarantine_is_explicit_atomic_terminal_and_not_redelivered(tmp_path):
    store = _store(tmp_path)
    producer = _publish(store)
    lease = _lease(store)

    terminal = _quarantine(store, lease)
    assert terminal.terminal_kind == "quarantined"
    quarantine = terminal.quarantine_receipt
    consumption_result = terminal.consumption_receipt
    assert quarantine.event_id == producer.event_id
    assert quarantine.event_sha256 == producer.event_sha256
    assert quarantine.producer_receipt_sha256 == producer.producer_receipt_sha256
    assert quarantine.consumer_id == producer.consumer_id
    assert quarantine.stream_id == producer.stream_id
    assert quarantine.stream_sequence == producer.stream_sequence == 1
    assert quarantine.evidence_fingerprint_sha256 == FINGERPRINT
    assert consumption_result.event_id == quarantine.event_id
    assert consumption_result.event_sha256 == quarantine.event_sha256
    assert (
        consumption_result.producer_receipt_sha256
        == quarantine.producer_receipt_sha256
    )
    assert consumption_result.consumer_id == quarantine.consumer_id
    assert consumption_result.stream_id == quarantine.stream_id
    assert consumption_result.stream_sequence == quarantine.stream_sequence
    assert (
        consumption_result.output_sha256
        == quarantine.quarantine_receipt_sha256
    )
    quarantine_mapping = quarantine.as_dict()
    quarantine_sha = quarantine_mapping.pop("quarantine_receipt_sha256")
    assert _mapping_sha(quarantine_mapping) == quarantine_sha
    consumption_mapping = consumption_result.as_dict()
    consumption_sha = consumption_mapping.pop("receipt_sha256")
    assert _mapping_sha(consumption_mapping) == consumption_sha

    with closing(sqlite3.connect(store.path)) as db:
        db.row_factory = sqlite3.Row
        child = db.execute(
            "SELECT * FROM quarantine_receipts WHERE event_id=?",
            (producer.event_id,),
        ).fetchone()
        consumption = db.execute(
            "SELECT * FROM consumption_receipts WHERE event_id=?",
            (producer.event_id,),
        ).fetchone()
        outbox = db.execute(
            "SELECT * FROM completion_outbox WHERE event_id=?",
            (producer.event_id,),
        ).fetchone()
        cursor = db.execute(
            "SELECT * FROM consumer_cursors WHERE consumer_id=? AND stream_id=?",
            (producer.consumer_id, producer.stream_id),
        ).fetchone()
        assert child is not None and consumption is not None
        assert consumption["output_sha256"] == child["quarantine_receipt_sha256"]
        assert (
            quarantine.quarantine_receipt_sha256
            == child["quarantine_receipt_sha256"]
        )
        assert consumption_result.receipt_sha256 == consumption["receipt_sha256"]
        assert outbox["state"] == "consumed"
        assert outbox["lease_owner"] is None
        assert outbox["lease_token_sha256"] is None
        assert outbox["terminal_at"] == NOW + 1.0
        assert cursor["last_sequence"] == 1
    assert store.claim_next(
        producer.consumer_id,
        producer.stream_id,
        "other-owner",
        now=NOW + 2.0,
    ) is None
    diagnostics = store.diagnostics()
    assert diagnostics["quarantine_receipts"] == 1
    assert diagnostics["terminal_kinds"] == {
        "normal_consumed": 0,
        "quarantined": 1,
        "rejected": 0,
        "superseded": 0,
    }


@pytest.mark.parametrize("reason", sorted(QUARANTINE_REASON_CODES))
def test_closed_permanent_reason_catalog_is_accepted(tmp_path, reason):
    store = _store(tmp_path)
    _publish(store)
    receipt = _quarantine(store, _lease(store), reason=reason)
    assert receipt.quarantine_receipt.reason_code == reason


@pytest.mark.parametrize(
    "reason",
    [
        "cancelled",
        "deadline_exceeded",
        "disk_full",
        "output_capacity_exceeded",
        "system_store_unavailable",
        "timeout",
    ],
)
def test_transient_or_system_failures_cannot_enter_quarantine(tmp_path, reason):
    store = _store(tmp_path)
    producer = _publish(store)
    lease = _lease(store)
    with pytest.raises(AcquisitionEvidenceLifecycleError, match="allowlisted"):
        _quarantine(store, lease, reason=reason)
    with closing(sqlite3.connect(store.path)) as db:
        assert db.execute(
            "SELECT count(*) FROM quarantine_receipts"
        ).fetchone()[0] == 0
        assert db.execute(
            "SELECT count(*) FROM consumption_receipts"
        ).fetchone()[0] == 0
        assert db.execute(
            "SELECT state FROM completion_outbox WHERE event_id=?",
            (producer.event_id,),
        ).fetchone()[0] == "leased"


def test_event_lease_fingerprint_and_expiry_gates_fail_closed(tmp_path):
    store = _store(tmp_path)
    _publish(store)
    lease = _lease(store)
    common = {
        "event_id": lease.event_id,
        "lease_token": lease.lease_token,
        "reason_code": "decoder_output_integrity_failed",
        "evidence_fingerprint_sha256": FINGERPRINT,
        "now": NOW + 1.0,
    }
    with pytest.raises(AcquisitionEvidenceConflict, match="event checksum"):
        store.quarantine_leased(
            expected_event_sha256="f" * 64,
            **common,
        )
    with pytest.raises(AcquisitionEvidenceConflict, match="lease"):
        store.quarantine_leased(
            lease.event_id,
            "b" * 64,
            expected_event_sha256=lease.event_sha256,
            reason_code="decoder_output_integrity_failed",
            evidence_fingerprint_sha256=FINGERPRINT,
            now=NOW + 1.0,
        )
    with pytest.raises(AcquisitionEvidenceLifecycleError, match="fingerprint"):
        _quarantine(store, lease, fingerprint="f" * 63)
    with pytest.raises(AcquisitionEvidenceConflict, match="expired"):
        _quarantine(store, lease, now=NOW + 61.0)


def test_exact_replay_conflicts_and_normal_success_lookups_distinguish_quarantine(
    tmp_path,
):
    store = _store(tmp_path)
    _publish(store)
    lease = _lease(store)
    receipt = _quarantine(store, lease)
    assert _quarantine(store, lease, now=NOW + 2.0) == receipt
    with pytest.raises(
        AcquisitionEvidenceConflict, match="quarantine receipt conflicts"
    ):
        _quarantine(
            store,
            lease,
            fingerprint="5" * 64,
            now=NOW + 2.0,
        )
    with pytest.raises(
        AcquisitionEvidenceConflict, match="quarantine receipt conflicts"
    ):
        _quarantine(
            store,
            lease,
            reason="canonical_evidence_integrity_failed",
            now=NOW + 2.0,
        )
    with pytest.raises(AcquisitionEvidenceConflict, match="quarantined"):
        store.acknowledge(
            lease.event_id,
            lease.lease_token,
            output_sha256=(
                receipt.quarantine_receipt.quarantine_receipt_sha256
            ),
            now=NOW + 2.0,
        )
    with pytest.raises(AcquisitionEvidenceConflict, match="quarantined"):
        store.load_terminal_consumption("radar.test.raw", 1)


def test_concurrent_quarantine_has_one_durable_identity(tmp_path):
    first = _store(tmp_path, token="a" * 64)
    _publish(first)
    lease = _lease(first)
    second = _store(tmp_path, token="b" * 64)

    def quarantine(store):
        return _quarantine(store, lease)

    with ThreadPoolExecutor(max_workers=2) as pool:
        receipts = tuple(pool.map(quarantine, (first, second)))
    assert receipts[0] == receipts[1]
    with closing(sqlite3.connect(first.path)) as db:
        assert db.execute(
            "SELECT count(*) FROM quarantine_receipts"
        ).fetchone()[0] == 1
        assert db.execute(
            "SELECT count(*) FROM consumption_receipts"
        ).fetchone()[0] == 1
        assert db.execute(
            "SELECT last_sequence FROM consumer_cursors"
        ).fetchone()[0] == 1


@pytest.mark.parametrize(
    "point",
    [
        "quarantine.before_receipt_commit",
        "quarantine.after_receipt_commit",
        "quarantine.before_cursor_commit",
        "quarantine.after_cursor_commit",
    ],
)
def test_quarantine_restart_is_exact_once_at_every_atomic_faultpoint(
    tmp_path,
    point,
):
    fault = FaultOnce(point)
    store = _store(tmp_path, fault=fault)
    producer = _publish(store)
    lease = _lease(store)
    with pytest.raises(RuntimeError, match=point.replace(".", r"\.")):
        _quarantine(store, lease)
    assert fault.hit

    with closing(sqlite3.connect(store.path)) as db:
        child_count = db.execute(
            "SELECT count(*) FROM quarantine_receipts"
        ).fetchone()[0]
        consumption_count = db.execute(
            "SELECT count(*) FROM consumption_receipts"
        ).fetchone()[0]
        state = db.execute(
            "SELECT state FROM completion_outbox WHERE event_id=?",
            (producer.event_id,),
        ).fetchone()[0]
        cursor = db.execute(
            "SELECT last_sequence FROM consumer_cursors"
        ).fetchone()[0]
    expected = 1 if point == "quarantine.after_cursor_commit" else 0
    assert child_count == consumption_count == expected
    assert state == ("consumed" if expected else "leased")
    assert cursor == expected

    reopened = _store(tmp_path)
    recovered = _quarantine(reopened, lease, now=NOW + 2.0)
    assert recovered.quarantine_receipt.event_id == producer.event_id
    with closing(sqlite3.connect(reopened.path)) as db:
        assert db.execute(
            "SELECT count(*) FROM quarantine_receipts"
        ).fetchone()[0] == 1
        assert db.execute(
            "SELECT count(*) FROM consumption_receipts"
        ).fetchone()[0] == 1
        assert db.execute(
            "SELECT last_sequence FROM consumer_cursors"
        ).fetchone()[0] == 1


def test_hot_quarantine_tamper_fails_closed(tmp_path):
    store = _store(tmp_path)
    _publish(store)
    lease = _lease(store)
    _quarantine(store, lease)
    with closing(sqlite3.connect(store.path)) as db:
        db.execute(
            "UPDATE quarantine_receipts SET evidence_fingerprint_sha256=? "
            "WHERE event_id=?",
            ("5" * 64, lease.event_id),
        )
        db.commit()
    with pytest.raises(AcquisitionEvidenceIntegrityError, match="checksum"):
        _quarantine(store, lease, now=NOW + 2.0)
    with pytest.raises(AcquisitionEvidenceIntegrityError, match="checksum"):
        store.load_terminal_consumption("radar.test.raw", 1)


def test_public_terminal_pair_fails_closed_on_consumption_tamper(tmp_path):
    store = _store(tmp_path)
    _publish(store)
    lease = _lease(store)
    _quarantine(store, lease)
    with closing(sqlite3.connect(store.path)) as db:
        db.execute(
            "UPDATE consumption_receipts SET receipt_sha256=? WHERE event_id=?",
            ("f" * 64, lease.event_id),
        )
        db.commit()
    with pytest.raises(AcquisitionEvidenceIntegrityError, match="checksum"):
        _quarantine(store, lease, now=NOW + 2.0)


@pytest.mark.parametrize("mutation", ["overlap", "orphan", "nonterminal"])
def test_diagnostics_rejects_terminal_topology_tamper(tmp_path, mutation):
    store = _store(tmp_path)
    producer = _publish(store)
    lease = _lease(store)
    _quarantine(store, lease)
    with closing(sqlite3.connect(store.path)) as db:
        if mutation == "overlap":
            db.execute(
                "INSERT INTO rejection_receipts VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "f" * 64,
                    lease.event_id,
                    lease.event_sha256,
                    lease.producer_receipt_sha256,
                    lease.consumer_id,
                    lease.stream_id,
                    lease.stream_sequence,
                    producer.desired_state_sha256,
                    None,
                    "active_desired_state_unavailable",
                    NOW + 1.0,
                ),
            )
        elif mutation == "orphan":
            db.execute(
                "DELETE FROM consumption_receipts WHERE event_id=?",
                (lease.event_id,),
            )
        else:
            db.execute(
                "UPDATE completion_outbox SET state='leased' WHERE event_id=?",
                (lease.event_id,),
            )
        db.commit()
    with pytest.raises(
        AcquisitionEvidenceIntegrityError,
        match="terminal receipt topology",
    ):
        store.diagnostics()


def test_diagnostics_preserves_valid_rejection_crash_reconciliation(tmp_path):
    fault = FaultOnce("reject.after_receipt_commit")
    store = _store(tmp_path, fault=fault)
    _publish(store)
    lease = _lease(store)
    with pytest.raises(RuntimeError, match="reject.after_receipt_commit"):
        store.reject_leased(
            lease.event_id,
            lease.lease_token,
            expected_event_sha256=lease.event_sha256,
            reason_code="active_desired_state_unavailable",
            active_desired_state_sha256=None,
            now=NOW + 1.0,
        )
    diagnostics = _store(tmp_path).diagnostics()
    assert diagnostics["pending_terminal_receipts"] == 1
    assert diagnostics["rejection_receipts"] == 1
    assert diagnostics["terminal_kinds"]["rejected"] == 0


def test_v3_archive_seals_prunes_reopens_and_replays_quarantine(tmp_path):
    store = _store(tmp_path)
    _publish(store)
    lease = _lease(store)
    receipt = _quarantine(store, lease)
    result = store.rotate_archive(
        policy=EvidenceRetentionPolicy(),
        now=NOW + 10.0,
    )
    assert result.status == "archived"
    with closing(sqlite3.connect(store.path)) as db:
        assert db.execute(
            "SELECT count(*) FROM quarantine_receipts"
        ).fetchone()[0] == 0
        assert db.execute(
            "SELECT count(*) FROM consumption_receipts"
        ).fetchone()[0] == 0
        assert db.execute("SELECT count(*) FROM completion_outbox").fetchone()[0] == 0
    reopened = _store(tmp_path)
    audit = reopened.audit_archive()
    assert audit["healthy"]
    assert audit["archived_completions"] == 1
    assert audit["archived_quarantines"] == 1
    assert audit["archived_rejections"] == 0
    assert _quarantine(reopened, lease, now=NOW + 20.0) == receipt
    with pytest.raises(AcquisitionEvidenceConflict, match="quarantined"):
        reopened.load_terminal_consumption("radar.test.raw", 1)
    with pytest.raises(AcquisitionEvidenceConflict, match="quarantined"):
        reopened.acknowledge(
            lease.event_id,
            lease.lease_token,
            output_sha256=(
                receipt.quarantine_receipt.quarantine_receipt_sha256
            ),
            now=NOW + 20.0,
        )


def test_v3_archive_quarantine_tamper_is_detected(tmp_path):
    store = _store(tmp_path)
    _publish(store)
    _quarantine(store, _lease(store))
    store.rotate_archive(policy=EvidenceRetentionPolicy(), now=NOW + 10.0)
    segment = next(store.archive_segment_directory.iterdir())
    segment.write_bytes(segment.read_bytes() + b" ")
    with pytest.raises(AcquisitionEvidenceIntegrityError):
        store.audit_archive()
    with pytest.raises(AcquisitionEvidenceIntegrityError):
        store.lookup_sequence("radar.test.raw", 1)


def test_schema_v2_migrates_append_only_and_preserves_normal_success(tmp_path):
    store = _store(tmp_path)
    first = _publish(store, "1")
    lease = _lease(store)
    store.acknowledge(
        lease.event_id,
        lease.lease_token,
        output_sha256="3" * 64,
        now=NOW + 1.0,
    )
    with closing(sqlite3.connect(store.path)) as db:
        # Reconstruct the historical v2 authority shape.  Schema v2 predates
        # the immutable store-instance manifest triggers installed by v3.
        for trigger in store._reclamation_trigger_definitions():
            db.execute(f'DROP TRIGGER IF EXISTS "{trigger}"')
        db.execute("DROP TRIGGER lifecycle_schema_no_update")
        db.execute("DROP TRIGGER lifecycle_schema_no_delete")
        db.execute("DROP TABLE lifecycle_reclamation_object_references")
        db.execute("DROP TABLE lifecycle_reclamation_files")
        db.execute("DROP TABLE lifecycle_reclamation_members")
        db.execute("DROP TABLE lifecycle_reclamation_decisions")
        db.execute("DROP TABLE lifecycle_reclamation_meta")
        db.execute("DROP TABLE lifecycle_archive_floor")
        db.execute("DROP TABLE quarantine_receipts")
        db.execute(
            "UPDATE lifecycle_schema SET schema_version=2 WHERE singleton=1"
        )
        db.commit()

    reopened = _store(tmp_path)
    with closing(sqlite3.connect(reopened.path)) as db:
        assert db.execute(
            "SELECT schema_version FROM lifecycle_schema WHERE singleton=1"
        ).fetchone()[0] == 4
        assert db.execute(
            "SELECT count(*) FROM quarantine_receipts"
        ).fetchone()[0] == 0
    terminal = reopened.load_terminal_consumption(first.stream_id, 1)
    assert terminal is not None
    assert terminal.producer_receipt == first
    _publish(reopened, "2")
    second_lease = _lease(reopened, now=NOW + 2.0)
    assert second_lease.stream_sequence == 2
    terminal = _quarantine(reopened, second_lease, now=NOW + 3.0)
    assert terminal.quarantine_receipt.stream_sequence == 2


def _sealed_record(store: AcquisitionEvidenceLifecycleStore) -> dict:
    segment = next(store.archive_segment_directory.iterdir())
    bundle = json.loads(segment.read_bytes())
    return bundle["records"][0]


def test_legacy_v1_v2_archive_records_remain_valid_and_v3_kind_is_exact(tmp_path):
    success = _store(tmp_path / "success")
    _publish(success)
    lease = _lease(success)
    success.acknowledge(
        lease.event_id,
        lease.lease_token,
        output_sha256="3" * 64,
        now=NOW + 1.0,
    )
    success.rotate_archive(policy=EvidenceRetentionPolicy(), now=NOW + 10.0)
    current = _sealed_record(success)
    assert current["schema"] == ARCHIVED_COMPLETION_SCHEMA
    assert current["terminal_kind"] == "consumed"

    version_two = deepcopy(current)
    version_two["schema"] = LEGACY_ARCHIVED_COMPLETION_SCHEMA_V2
    version_two.pop("quarantine_receipt")
    version_two.pop("terminal_kind")
    assert success._validate_archived_record(version_two) == version_two

    version_one = deepcopy(version_two)
    version_one["schema"] = LEGACY_ARCHIVED_COMPLETION_SCHEMA
    version_one.pop("rejection_receipt")
    assert success._validate_archived_record(version_one) == version_one

    invalid_kind = deepcopy(current)
    invalid_kind["terminal_kind"] = "quarantined"
    with pytest.raises(AcquisitionEvidenceIntegrityError, match="terminal kind"):
        success._validate_archived_record(invalid_kind)

    rejected = _store(tmp_path / "rejected")
    _publish(rejected)
    rejected_lease = _lease(rejected)
    rejected.reject_leased(
        rejected_lease.event_id,
        rejected_lease.lease_token,
        expected_event_sha256=rejected_lease.event_sha256,
        reason_code="active_desired_state_unavailable",
        active_desired_state_sha256=None,
        now=NOW + 1.0,
    )
    rejected.rotate_archive(policy=EvidenceRetentionPolicy(), now=NOW + 10.0)
    rejection_v2 = _sealed_record(rejected)
    rejection_v2["schema"] = LEGACY_ARCHIVED_COMPLETION_SCHEMA_V2
    rejection_v2.pop("quarantine_receipt")
    rejection_v2.pop("terminal_kind")
    assert rejected._validate_archived_record(rejection_v2) == rejection_v2


def test_quarantine_inputs_and_diagnostics_are_bounded_and_secret_free(tmp_path):
    store = _store(tmp_path)
    producer = _publish(store)
    lease = _lease(store)
    secret = "password=must-not-persist"
    with pytest.raises(AcquisitionEvidenceLifecycleError, match="allowlisted") as err:
        _quarantine(store, lease, reason=secret)
    assert secret not in str(err.value)
    receipt = _quarantine(store, lease)
    diagnostics = store.diagnostics()
    encoded = json.dumps(diagnostics, sort_keys=True)
    assert len(encoded.encode()) <= 4096
    for forbidden in (
        producer.event_id,
        producer.acquisition_job_id,
        lease.lease_token,
        FINGERPRINT,
        str(store.root),
        "exact-raw-1",
        secret,
    ):
        assert forbidden not in encoded
    assert receipt.quarantine_receipt.quarantine_receipt_sha256 not in encoded
    assert receipt.consumption_receipt.receipt_sha256 not in encoded
    assert secret.encode() not in store.path.read_bytes()
