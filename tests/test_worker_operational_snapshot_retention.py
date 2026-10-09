from __future__ import annotations

from contextlib import closing
from copy import deepcopy
import hashlib
import json
import re
import sqlite3
from pathlib import Path

import pytest

from storm_tracker_worker import operational_snapshot_store as store_module
from storm_tracker_worker.operational_snapshot_store import (
    OperationalSnapshotStore,
    SnapshotCommitWindowExpired,
    SnapshotLegacyContractMigrationRequired,
    SnapshotStoreCorruption,
)


NOW = 1_800_300_000.0
MODULE = (
    Path(__file__).resolve().parents[1]
    / "storm_tracker_worker"
    / "operational_snapshot_store.py"
)


def _snapshot(revision: int) -> dict:
    empty = {"complete": True, "overflow_count": 0, "items": []}
    return {
        "contract": "operational_snapshot_v1",
        "revision": revision,
        "desired_state_sha256": "d" * 64,
        "product_valid_at": NOW + revision,
        "received_at": NOW + revision,
        "evaluated_at": NOW + revision,
        "valid_until": NOW + revision + 300.0,
        "provenance_complete": False,
        "provenance_source_ids": [],
        "provenance_reason_codes": ["retention_test"],
        "worker": {
            "state": "online",
            "queue_depth": revision % 3,
            "failed_jobs": 0,
            "status_text": "",
        },
        "regions": deepcopy(empty),
        "targets": deepcopy(empty),
        "providers": deepcopy(empty),
        "ui_storms": deepcopy(empty),
        "map_artifact": {"artifact_handle": None, "etag": None},
    }


def _commit_range(store: OperationalSnapshotStore, first: int, last: int):
    results = {}
    for revision in range(first, last + 1):
        results[revision] = store.commit(
            f"retention:{revision}",
            expected_revision=revision - 1,
            snapshot=_snapshot(revision),
        )
    return results


def _downgrade_fixture_to_exact_v1(path: Path) -> None:
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("DROP TABLE snapshot_retention_state")
        db.executemany(
            "UPDATE manifest SET value=? WHERE key=?",
            (
                (
                    store_module.LEGACY_STORE_ALGORITHM_VERSION,
                    "algorithm_version",
                ),
                (
                    str(store_module.LEGACY_STORE_SCHEMA_VERSION),
                    "schema_version",
                ),
            ),
        )


def _rewrite_head_as_pre_expiry_contract(path: Path) -> None:
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("PRAGMA foreign_keys=OFF")
        row = db.execute("SELECT * FROM snapshot_envelopes").fetchone()
        receipt = db.execute("SELECT * FROM snapshot_commit_receipts").fetchone()
        envelope = json.loads(bytes(row[4]).decode("utf-8"))
        envelope["snapshot"].pop("valid_until")
        snapshot_sha = hashlib.sha256(
            store_module._canonical_json(envelope["snapshot"])
        ).hexdigest()
        envelope["snapshot_sha256"] = snapshot_sha
        canonical_envelope = store_module._canonical_json(envelope)
        envelope_sha = hashlib.sha256(canonical_envelope).hexdigest()
        revision = int(row[0])
        operation_sha = store_module._operation_digest(
            expected_revision=revision - 1,
            envelope_sha256=envelope_sha,
        )
        result = json.loads(bytes(receipt[4]).decode("utf-8"))
        result.update({
            "envelope_bytes": len(canonical_envelope),
            "envelope_sha256": envelope_sha,
            "snapshot_sha256": snapshot_sha,
        })
        canonical_result = store_module._canonical_json(result)
        result_sha = hashlib.sha256(canonical_result).hexdigest()
        receipt_sha = store_module._receipt_digest(
            commit_id=str(receipt[0]),
            operation_sha256=operation_sha,
            revision=revision,
            envelope_sha256=envelope_sha,
            result_sha256=result_sha,
            committed_at=float(receipt[7]),
        )
        db.execute(
            "UPDATE snapshot_envelopes SET snapshot_sha256=?,envelope_sha256=?,"
            "envelope_bytes=?,canonical_envelope=? WHERE revision=?",
            (
                snapshot_sha,
                envelope_sha,
                len(canonical_envelope),
                canonical_envelope,
                revision,
            ),
        )
        db.execute(
            "UPDATE active_snapshot SET envelope_sha256=? WHERE revision=?",
            (envelope_sha, revision),
        )
        db.execute(
            "UPDATE snapshot_commit_receipts SET operation_sha256=?,"
            "envelope_sha256=?,canonical_result=?,result_sha256=?,receipt_sha256=? "
            "WHERE revision=?",
            (
                operation_sha,
                envelope_sha,
                canonical_result,
                result_sha,
                receipt_sha,
                revision,
            ),
        )


def test_tail_is_bounded_contiguous_and_retained_retry_is_exact(tmp_path):
    path = tmp_path / "bounded.sqlite3"
    store = OperationalSnapshotStore(
        path,
        clock=lambda: NOW,
        retention_rows=3,
    )
    results = _commit_range(store, 1, 12)

    assert store.head()["revision"] == 12
    assert store.audit()["snapshots"] == 3
    status = store.retention_status()
    assert status["first_retained_revision"] == 10
    assert status["head_revision"] == 12
    assert status["retained_revisions"] == 3
    with closing(sqlite3.connect(path)) as db:
        assert db.execute(
            "SELECT revision FROM snapshot_envelopes ORDER BY revision"
        ).fetchall() == [(10,), (11,), (12,)]
        assert db.execute(
            "SELECT revision FROM snapshot_commit_receipts ORDER BY revision"
        ).fetchall() == [(10,), (11,), (12,)]
        retention = db.execute(
            "SELECT first_retained_revision,pruned_through_revision,"
            "retained_revision_count,head_revision,last_pruned_receipt_sha256 "
            "FROM snapshot_retention_state"
        ).fetchone()
        assert retention[:4] == (10, 9, 3, 12)
        assert re.fullmatch(r"[0-9a-f]{64}", retention[4])
        assert db.execute("PRAGMA foreign_key_check").fetchone() is None

    assert store.commit(
        "retention:11",
        expected_revision=10,
        snapshot=_snapshot(11),
    ) == results[11]
    with pytest.raises(SnapshotCommitWindowExpired, match="idempotentievenster"):
        store.commit(
            "retention:1",
            expected_revision=0,
            snapshot=_snapshot(1),
        )
    assert store.head()["revision"] == 12


@pytest.mark.parametrize(
    "fault_point",
    (
        "after_retention_receipt_prune",
        "after_retention_envelope_prune",
        "after_retention_update",
        "before_commit",
    ),
)
def test_prune_and_new_head_share_one_crash_boundary(tmp_path, fault_point):
    path = tmp_path / f"crash-{fault_point}.sqlite3"
    baseline = OperationalSnapshotStore(
        path,
        clock=lambda: NOW,
        retention_rows=2,
    )
    _commit_range(baseline, 1, 2)

    def fail(point: str) -> None:
        if point == fault_point:
            raise RuntimeError("simulated retention crash")

    crashing = OperationalSnapshotStore(
        path,
        clock=lambda: NOW + 1,
        retention_rows=2,
        fault_injector=fail,
    )
    with pytest.raises(RuntimeError, match="retention crash"):
        crashing.commit(
            "retention:3",
            expected_revision=2,
            snapshot=_snapshot(3),
        )

    recovered = OperationalSnapshotStore(
        path,
        clock=lambda: NOW + 2,
        retention_rows=2,
    )
    assert recovered.head()["revision"] == 2
    assert recovered.retention_status()["first_retained_revision"] == 1
    with closing(sqlite3.connect(path)) as db:
        assert db.execute(
            "SELECT revision FROM snapshot_envelopes ORDER BY revision"
        ).fetchall() == [(1,), (2,)]
        assert db.execute(
            "SELECT revision FROM snapshot_commit_receipts ORDER BY revision"
        ).fetchall() == [(1,), (2,)]

    assert recovered.commit(
        "retention:3",
        expected_revision=2,
        snapshot=_snapshot(3),
    )["revision"] == 3
    assert recovered.retention_status()["first_retained_revision"] == 2


def test_exact_v1_store_migrates_atomically_to_bounded_v2_tail(tmp_path):
    path = tmp_path / "legacy.sqlite3"
    original = OperationalSnapshotStore(
        path,
        clock=lambda: NOW,
        retention_rows=10,
    )
    _commit_range(original, 1, 5)
    _downgrade_fixture_to_exact_v1(path)

    migrated = OperationalSnapshotStore(
        path,
        clock=lambda: NOW + 1,
        retention_rows=2,
    )
    assert migrated.head()["revision"] == 5
    assert migrated.retention_status()["first_retained_revision"] == 4
    with closing(sqlite3.connect(path)) as db:
        assert dict(db.execute("SELECT key,value FROM manifest")) == {
            "algorithm_version": store_module.STORE_ALGORITHM_VERSION,
            "schema_version": str(store_module.STORE_SCHEMA_VERSION),
        }
        assert db.execute(
            "SELECT revision FROM snapshot_envelopes ORDER BY revision"
        ).fetchall() == [(4,), (5,)]
        assert db.execute(
            "SELECT revision FROM snapshot_commit_receipts ORDER BY revision"
        ).fetchall() == [(4,), (5,)]
        assert db.execute("PRAGMA foreign_key_check").fetchone() is None


def test_pre_expiry_v1_store_is_explicitly_blocked_without_mutation(tmp_path):
    path = tmp_path / "pre-expiry.sqlite3"
    original = OperationalSnapshotStore(path, clock=lambda: NOW)
    _commit_range(original, 1, 1)
    _downgrade_fixture_to_exact_v1(path)
    _rewrite_head_as_pre_expiry_contract(path)

    with pytest.raises(
        SnapshotLegacyContractMigrationRequired,
        match="valid_until|expiryconversie",
    ):
        OperationalSnapshotStore(path, retention_rows=2).initialize()

    with closing(sqlite3.connect(path)) as db:
        assert dict(db.execute("SELECT key,value FROM manifest")) == {
            "algorithm_version": store_module.LEGACY_STORE_ALGORITHM_VERSION,
            "schema_version": str(store_module.LEGACY_STORE_SCHEMA_VERSION),
        }
        assert db.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name='snapshot_retention_state'"
        ).fetchone() is None
        raw = bytes(
            db.execute(
                "SELECT canonical_envelope FROM snapshot_envelopes"
            ).fetchone()[0]
        )
        assert "valid_until" not in json.loads(raw)["snapshot"]


@pytest.mark.parametrize(
    "fault_point",
    (
        "migration_after_receipt_prune",
        "migration_after_envelope_prune",
        "migration_after_schema",
        "migration_before_commit",
    ),
)
def test_failed_v1_migration_rolls_back_schema_manifest_and_history(
    tmp_path,
    fault_point,
):
    path = tmp_path / f"legacy-crash-{fault_point}.sqlite3"
    original = OperationalSnapshotStore(
        path,
        clock=lambda: NOW,
        retention_rows=10,
    )
    _commit_range(original, 1, 3)
    _downgrade_fixture_to_exact_v1(path)

    def fail(point: str) -> None:
        if point == fault_point:
            raise RuntimeError("simulated migration crash")

    with pytest.raises(RuntimeError, match="migration crash"):
        OperationalSnapshotStore(
            path,
            clock=lambda: NOW + 1,
            retention_rows=2,
            fault_injector=fail,
        ).initialize()

    with closing(sqlite3.connect(path)) as db:
        assert dict(db.execute("SELECT key,value FROM manifest")) == {
            "algorithm_version": store_module.LEGACY_STORE_ALGORITHM_VERSION,
            "schema_version": str(store_module.LEGACY_STORE_SCHEMA_VERSION),
        }
        assert db.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name='snapshot_retention_state'"
        ).fetchone() is None
        assert db.execute(
            "SELECT revision FROM snapshot_envelopes ORDER BY revision"
        ).fetchall() == [(1,), (2,), (3,)]
        assert db.execute(
            "SELECT revision FROM snapshot_commit_receipts ORDER BY revision"
        ).fetchall() == [(1,), (2,), (3,)]

    clean = OperationalSnapshotStore(
        path,
        clock=lambda: NOW + 2,
        retention_rows=2,
    )
    assert clean.head()["revision"] == 3
    assert clean.retention_status()["first_retained_revision"] == 2


def test_retention_checkpoint_detects_prefix_deletion_and_checksum_tamper(
    tmp_path,
):
    prefix_path = tmp_path / "prefix.sqlite3"
    prefix_store = OperationalSnapshotStore(
        prefix_path,
        clock=lambda: NOW,
        retention_rows=2,
    )
    _commit_range(prefix_store, 1, 4)
    with closing(sqlite3.connect(prefix_path)) as db, db:
        db.execute("PRAGMA foreign_keys=OFF")
        db.execute("DELETE FROM snapshot_commit_receipts WHERE revision=3")
        db.execute("DELETE FROM snapshot_envelopes WHERE revision=3")
    with pytest.raises(SnapshotStoreCorruption, match="revisiongat"):
        OperationalSnapshotStore(prefix_path).head()

    checksum_path = tmp_path / "checksum.sqlite3"
    checksum_store = OperationalSnapshotStore(
        checksum_path,
        clock=lambda: NOW,
        retention_rows=2,
    )
    _commit_range(checksum_store, 1, 3)
    with closing(sqlite3.connect(checksum_path)) as db, db:
        db.execute(
            "UPDATE snapshot_retention_state SET state_sha256=?",
            ("f" * 64,),
        )
    with pytest.raises(SnapshotStoreCorruption, match="checkpointchecksum"):
        OperationalSnapshotStore(checksum_path).head()


def test_active_head_fk_blocks_delete_and_freelist_pages_are_reused(tmp_path):
    path = tmp_path / "reuse.sqlite3"
    store = OperationalSnapshotStore(
        path,
        clock=lambda: NOW,
        retention_rows=4,
        max_database_bytes=1024 * 1024,
    )
    page_counts = []
    for revision in range(1, 101):
        store.commit(
            f"retention:{revision}",
            expected_revision=revision - 1,
            snapshot=_snapshot(revision),
        )
        if revision % 10 == 0:
            page_counts.append(store.retention_status()["page_count"])

    status = store.retention_status()
    assert status["head_revision"] == 100
    assert status["retained_revisions"] == 4
    assert status["used_database_bytes"] <= 1024 * 1024
    assert max(page_counts[-4:]) <= max(page_counts[2:6]) + 2
    with closing(sqlite3.connect(path)) as db:
        db.execute("PRAGMA foreign_keys=ON")
        with pytest.raises(sqlite3.IntegrityError):
            db.execute("DELETE FROM snapshot_envelopes WHERE revision=100")
        db.rollback()
        assert db.execute("PRAGMA foreign_key_check").fetchone() is None

    source = MODULE.read_text(encoding="utf-8")
    assert re.search(r"[\"']\s*VACUUM\b", source, re.IGNORECASE) is None
