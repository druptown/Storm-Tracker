from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from copy import deepcopy
import hashlib
import json
import multiprocessing
import os
import sqlite3

import pytest

from storm_tracker_worker import operational_snapshot_store as store_module
from storm_tracker_worker.operational_snapshot_store import (
    OperationalSnapshotStore,
    SnapshotCommitConflict,
    SnapshotHeadConflict,
    SnapshotStoreCapacityError,
    SnapshotStoreCorruption,
)
from storm_tracker_worker.tracking_core.operational_snapshot import (
    encode_control_envelope,
)


NOW = 1_800_000_100.0


def _hard_crash_commit(path: str, fault_point: str) -> None:
    def crash(point: str) -> None:
        if point == fault_point:
            os._exit(77)

    OperationalSnapshotStore(
        path,
        clock=lambda: NOW,
        fault_injector=crash,
    ).commit("commit:hard-crash", expected_revision=0, snapshot=_snapshot())


def _snapshot(revision: int = 1, *, status_text: str = "operationeel") -> dict:
    empty = {"complete": True, "overflow_count": 0, "items": []}
    return {
        "contract": "operational_snapshot_v1",
        "revision": revision,
        "desired_state_sha256": "d" * 64,
        "product_valid_at": NOW - 20.0,
        "received_at": NOW - 10.0,
        "evaluated_at": NOW - 1.0,
        "valid_until": NOW + 299.0,
        "provenance_complete": False,
        "provenance_source_ids": [],
        "provenance_reason_codes": ["initial_worker_snapshot"],
        "worker": {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
            "status_text": status_text,
        },
        "regions": deepcopy(empty),
        "targets": deepcopy(empty),
        "providers": deepcopy(empty),
        "ui_storms": deepcopy(empty),
        "map_artifact": {"artifact_handle": None, "etag": None},
    }


def _unknown_snapshot(revision: int = 1) -> dict:
    snapshot = _snapshot(revision, status_text="")
    snapshot["product_valid_at"] = None
    snapshot["received_at"] = None
    snapshot["worker"]["state"] = "unknown"
    snapshot["provenance_complete"] = False
    snapshot["provenance_source_ids"] = []
    snapshot["provenance_reason_codes"] = ["no_current_meteorological_fact"]
    return snapshot


def test_commit_persists_exact_canonical_envelope_and_reopens(tmp_path):
    path = tmp_path / "operational.sqlite3"
    snapshot = _snapshot()
    expected_bytes = encode_control_envelope(snapshot)
    expected_envelope_sha = hashlib.sha256(expected_bytes).hexdigest()
    store = OperationalSnapshotStore(path, clock=lambda: NOW)

    result = store.commit(
        "commit:snapshot:1", expected_revision=0, snapshot=snapshot
    )

    assert result == {
        "committed_at": NOW,
        "envelope_bytes": len(expected_bytes),
        "envelope_sha256": expected_envelope_sha,
        "method": store_module.METHOD,
        "revision": 1,
        "snapshot_sha256": json.loads(expected_bytes)["snapshot_sha256"],
    }
    head = OperationalSnapshotStore(path).head()
    assert head is not None
    assert head["canonical_envelope"] == expected_bytes
    assert head["snapshot"] == json.loads(expected_bytes)["snapshot"]
    assert head["revision"] == 1

    with closing(sqlite3.connect(path)) as db, db:
        assert db.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert db.execute("PRAGMA synchronous").fetchone()[0] == 2
        assert db.execute("SELECT count(*) FROM snapshot_envelopes").fetchone()[0] == 1
        assert db.execute("SELECT count(*) FROM snapshot_commit_receipts").fetchone()[0] == 1


def test_exact_retry_returns_byte_equal_original_result_without_new_rows(tmp_path):
    path = tmp_path / "operational.sqlite3"
    store = OperationalSnapshotStore(path, clock=lambda: NOW)
    first = store.commit("commit:one", expected_revision=0, snapshot=_snapshot())

    retry = OperationalSnapshotStore(path, clock=lambda: NOW + 999).commit(
        "commit:one", expected_revision=0, snapshot=deepcopy(_snapshot())
    )

    assert retry == first
    assert json.dumps(retry, sort_keys=True) == json.dumps(first, sort_keys=True)
    with closing(sqlite3.connect(path)) as db, db:
        assert db.execute("SELECT count(*) FROM snapshot_envelopes").fetchone()[0] == 1
        assert db.execute("SELECT count(*) FROM snapshot_commit_receipts").fetchone()[0] == 1


def test_reused_commit_id_with_changed_operation_conflicts(tmp_path):
    path = tmp_path / "operational.sqlite3"
    store = OperationalSnapshotStore(path, clock=lambda: NOW)
    store.commit("commit:one", expected_revision=0, snapshot=_snapshot())
    changed = _snapshot()
    changed["worker"]["status_text"] = "gewijzigd"

    with pytest.raises(SnapshotCommitConflict, match="andere snapshotdata"):
        store.commit("commit:one", expected_revision=0, snapshot=changed)


def test_revision_is_strictly_next_and_head_is_compare_and_swap(tmp_path):
    path = tmp_path / "operational.sqlite3"
    store = OperationalSnapshotStore(path, clock=lambda: NOW)

    with pytest.raises(SnapshotHeadConflict, match="exact"):
        store.commit("commit:two-first", expected_revision=0, snapshot=_snapshot(2))

    store.commit("commit:one", expected_revision=0, snapshot=_snapshot(1))
    with pytest.raises(SnapshotHeadConflict, match="actuele head 1"):
        store.commit("commit:stale", expected_revision=0, snapshot=_snapshot(1))

    second = _snapshot(2, status_text="tweede")
    result = store.commit("commit:two", expected_revision=1, snapshot=second)
    assert result["revision"] == 2
    assert store.head()["snapshot"]["worker"]["status_text"] == "tweede"


@pytest.mark.parametrize(
    "failure_point",
    (
        "after_snapshot_insert",
        "after_head_update",
        "after_receipt_insert",
        "before_commit",
    ),
)
def test_failure_before_sqlite_commit_rolls_back_all_three_facts(
    tmp_path, failure_point,
):
    path = tmp_path / "operational.sqlite3"

    def fail(point: str) -> None:
        if point == failure_point:
            raise RuntimeError("simulated power loss")

    store = OperationalSnapshotStore(path, clock=lambda: NOW, fault_injector=fail)
    with pytest.raises(RuntimeError, match="power loss"):
        store.commit("commit:one", expected_revision=0, snapshot=_snapshot())

    recovered = OperationalSnapshotStore(path, clock=lambda: NOW + 1)
    assert recovered.head() is None
    with closing(sqlite3.connect(path)) as db, db:
        assert db.execute("SELECT count(*) FROM snapshot_envelopes").fetchone()[0] == 0
        assert db.execute("SELECT count(*) FROM snapshot_commit_receipts").fetchone()[0] == 0
    assert recovered.commit(
        "commit:one", expected_revision=0, snapshot=_snapshot()
    )["revision"] == 1


def test_lost_result_after_commit_is_recovered_by_exact_receipt(tmp_path):
    path = tmp_path / "operational.sqlite3"

    def fail(point: str) -> None:
        if point == "after_commit":
            raise RuntimeError("result lost")

    store = OperationalSnapshotStore(path, clock=lambda: NOW, fault_injector=fail)
    with pytest.raises(RuntimeError, match="result lost"):
        store.commit("commit:one", expected_revision=0, snapshot=_snapshot())

    recovered = OperationalSnapshotStore(path, clock=lambda: NOW + 1)
    result = recovered.commit(
        "commit:one", expected_revision=0, snapshot=_snapshot()
    )
    assert result["committed_at"] == NOW
    assert recovered.head()["revision"] == 1


@pytest.mark.parametrize(
    "fault_point,committed",
    (("before_commit", False), ("after_commit", True)),
)
def test_abrupt_process_death_respects_sqlite_commit_boundary(
    tmp_path, fault_point, committed,
):
    path = tmp_path / f"hard-crash-{fault_point}.sqlite3"
    process = multiprocessing.get_context("spawn").Process(
        target=_hard_crash_commit,
        args=(str(path), fault_point),
    )
    process.start()
    process.join(timeout=20)
    if process.is_alive():
        process.terminate()
        process.join(timeout=5)
        pytest.fail("hard-crash child bleef hangen")
    assert process.exitcode == 77

    recovered = OperationalSnapshotStore(path, clock=lambda: NOW + 1)
    if committed:
        result = recovered.commit(
            "commit:hard-crash",
            expected_revision=0,
            snapshot=_snapshot(),
        )
        assert result["committed_at"] == NOW
        assert recovered.head()["revision"] == 1
    else:
        assert recovered.head() is None
        assert recovered.commit(
            "commit:hard-crash",
            expected_revision=0,
            snapshot=_snapshot(),
        )["revision"] == 1


def test_concurrent_cas_allows_only_one_revision_one_commit(tmp_path):
    path = tmp_path / "operational.sqlite3"
    OperationalSnapshotStore(path).initialize()
    barrier = __import__("threading").Barrier(2)

    def write(suffix: str):
        barrier.wait()
        return OperationalSnapshotStore(path, clock=lambda: NOW).commit(
            f"commit:{suffix}",
            expected_revision=0,
            snapshot=_snapshot(status_text=suffix),
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(write, value) for value in ("a", "b")]
    successes = []
    failures = []
    for future in futures:
        try:
            successes.append(future.result())
        except SnapshotHeadConflict as err:
            failures.append(err)
    assert len(successes) == 1
    assert len(failures) == 1
    with closing(sqlite3.connect(path)) as db, db:
        assert db.execute("SELECT count(*) FROM snapshot_envelopes").fetchone()[0] == 1
        assert db.execute("SELECT count(*) FROM snapshot_commit_receipts").fetchone()[0] == 1


def test_row_capacity_fails_closed_without_advancing_head(tmp_path):
    path = tmp_path / "operational.sqlite3"
    store = OperationalSnapshotStore(
        path, clock=lambda: NOW, max_snapshot_rows=1
    )
    store.commit("commit:one", expected_revision=0, snapshot=_snapshot(1))

    with pytest.raises(SnapshotStoreCapacityError, match="snapshotrijlimiet"):
        store.commit("commit:two", expected_revision=1, snapshot=_snapshot(2))
    assert store.head()["revision"] == 1
    assert store.commit(
        "commit:one", expected_revision=0, snapshot=_snapshot(1)
    )["revision"] == 1


def test_corrupt_active_envelope_fails_closed_on_hot_read(tmp_path):
    path = tmp_path / "operational.sqlite3"
    store = OperationalSnapshotStore(path, clock=lambda: NOW)
    store.commit("commit:one", expected_revision=0, snapshot=_snapshot())
    with closing(sqlite3.connect(path)) as db, db:
        db.execute(
            "UPDATE snapshot_envelopes SET canonical_envelope=?,envelope_bytes=2 "
            "WHERE revision=1",
            (b"{}",),
        )

    with pytest.raises(SnapshotStoreCorruption, match="lengte|hash"):
        OperationalSnapshotStore(path).head()


def test_manifest_tamper_and_unknown_table_fail_closed(tmp_path):
    path = tmp_path / "operational.sqlite3"
    OperationalSnapshotStore(path).initialize()
    with closing(sqlite3.connect(path)) as db, db:
        db.execute(
            "UPDATE manifest SET value='999' WHERE key='schema_version'"
        )
    with pytest.raises(SnapshotStoreCorruption, match="manifest"):
        OperationalSnapshotStore(path).initialize()

    other = tmp_path / "unknown-table.sqlite3"
    OperationalSnapshotStore(other).initialize()
    with closing(sqlite3.connect(other)) as db, db:
        db.execute("CREATE TABLE injected(value TEXT)")
    with pytest.raises(SnapshotStoreCorruption, match="schema"):
        OperationalSnapshotStore(other).initialize()


def test_explicit_audit_checks_all_revisions_and_receipts(tmp_path):
    path = tmp_path / "operational.sqlite3"
    store = OperationalSnapshotStore(path, clock=lambda: NOW)
    store.commit("commit:one", expected_revision=0, snapshot=_snapshot(1))
    store.commit("commit:two", expected_revision=1, snapshot=_snapshot(2))

    assert store.audit() == {
        "commit_receipts": 2,
        "database_bytes": store.audit()["database_bytes"],
        "method": store_module.METHOD,
        "snapshots": 2,
        "status": "ok",
    }


def test_worker_handlers_have_exact_payloads_and_bounded_text_head(monkeypatch, tmp_path):
    path = tmp_path / "operational.sqlite3"
    monkeypatch.setattr(store_module, "DEFAULT_STORE_PATH", path)

    class Job:
        def __init__(self, payload):
            self.payload = payload

    with pytest.raises(store_module.OperationalSnapshotStoreError, match="exact"):
        store_module.operational_snapshot_commit(Job({"snapshot": _snapshot()}))
    result = store_module.operational_snapshot_commit(Job({
        "commit_id": "commit:one",
        "expected_revision": 0,
        "snapshot": _snapshot(),
    }))
    assert result["revision"] == 1

    head = store_module.operational_snapshot_head(Job({}))
    assert head["available"] is True
    assert len(head["canonical_envelope"].encode("utf-8")) <= 64 * 1024
    assert json.loads(head["canonical_envelope"])["revision"] == 1
    with pytest.raises(store_module.OperationalSnapshotStoreError, match="leeg"):
        store_module.operational_snapshot_head(Job({"unexpected": True}))


def test_unknown_snapshot_preserves_absent_product_and_receipt_times(tmp_path):
    path = tmp_path / "unknown.sqlite3"
    store = OperationalSnapshotStore(path, clock=lambda: NOW)

    result = store.commit(
        "commit:unknown", expected_revision=0, snapshot=_unknown_snapshot()
    )

    assert result["revision"] == 1
    head = store.head()
    assert head["snapshot"]["product_valid_at"] is None
    assert head["snapshot"]["received_at"] is None
    with closing(sqlite3.connect(path)) as db, db:
        row = db.execute(
            "SELECT product_valid_at,received_at FROM snapshot_envelopes"
        ).fetchone()
    assert row == (None, None)
    assert store.audit()["status"] == "ok"


def test_hot_read_rejects_internal_head_rollback_to_valid_older_row(tmp_path):
    path = tmp_path / "rollback.sqlite3"
    store = OperationalSnapshotStore(path, clock=lambda: NOW)
    store.commit("commit:one", expected_revision=0, snapshot=_snapshot(1))
    store.commit("commit:two", expected_revision=1, snapshot=_snapshot(2))

    with closing(sqlite3.connect(path)) as db, db:
        revision, digest, committed_at = db.execute(
            "SELECT revision,envelope_sha256,committed_at "
            "FROM snapshot_envelopes WHERE revision=1"
        ).fetchone()
        db.execute(
            "UPDATE active_snapshot SET revision=?,envelope_sha256=?,activated_at=?",
            (revision, digest, committed_at),
        )

    with pytest.raises(SnapshotStoreCorruption, match="teruggezet|nieuwste"):
        store.head()


@pytest.mark.parametrize(
    "statement,parameters,match",
    (
        (
            "UPDATE snapshot_envelopes SET evaluated_at=evaluated_at+1 "
            "WHERE revision=1",
            (),
            "evaluatietijd",
        ),
        (
            "UPDATE snapshot_commit_receipts SET canonical_result=? "
            "WHERE revision=1",
            (b"{}",),
            "resultaathash",
        ),
        (
            "UPDATE snapshot_commit_receipts SET operation_sha256=? "
            "WHERE revision=1",
            ("f" * 64,),
            "operationhash",
        ),
    ),
)
def test_hot_read_checks_duplicated_times_and_full_active_receipt(
    tmp_path, statement, parameters, match,
):
    path = tmp_path / "hot-corruption.sqlite3"
    store = OperationalSnapshotStore(path, clock=lambda: NOW)
    store.commit("commit:one", expected_revision=0, snapshot=_snapshot())
    with closing(sqlite3.connect(path)) as db, db:
        db.execute(statement, parameters)

    with pytest.raises(SnapshotStoreCorruption, match=match):
        store.head()


def test_audit_detects_corrupt_non_head_history(tmp_path):
    path = tmp_path / "history.sqlite3"
    store = OperationalSnapshotStore(path, clock=lambda: NOW)
    store.commit("commit:one", expected_revision=0, snapshot=_snapshot(1))
    store.commit("commit:two", expected_revision=1, snapshot=_snapshot(2))
    with closing(sqlite3.connect(path)) as db, db:
        db.execute(
            "UPDATE snapshot_envelopes SET canonical_envelope=?,envelope_bytes=2 "
            "WHERE revision=1",
            (b"{}",),
        )

    assert store.head()["revision"] == 2
    with pytest.raises(SnapshotStoreCorruption, match="hash"):
        store.audit()


def test_hot_read_detects_a_structural_gap_in_older_receipts(tmp_path):
    path = tmp_path / "receipt-gap.sqlite3"
    store = OperationalSnapshotStore(path, clock=lambda: NOW)
    store.commit("commit:one", expected_revision=0, snapshot=_snapshot(1))
    store.commit("commit:two", expected_revision=1, snapshot=_snapshot(2))
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("DELETE FROM snapshot_commit_receipts WHERE revision=1")

    with pytest.raises(SnapshotStoreCorruption, match="receiptgat"):
        store.head()


def test_schema_contract_rejects_missing_constraints_and_extra_index(tmp_path):
    weak = tmp_path / "weak-schema.sqlite3"
    with closing(sqlite3.connect(weak)) as db, db:
        db.execute(
            "CREATE TABLE manifest(key TEXT PRIMARY KEY,value TEXT NOT NULL) STRICT"
        )
        db.executemany(
            "INSERT INTO manifest(key,value) VALUES(?,?)",
            (
                ("algorithm_version", store_module.STORE_ALGORITHM_VERSION),
                ("schema_version", str(store_module.STORE_SCHEMA_VERSION)),
            ),
        )
    with pytest.raises(SnapshotStoreCorruption, match="schema"):
        OperationalSnapshotStore(weak).initialize()

    indexed = tmp_path / "extra-index.sqlite3"
    store = OperationalSnapshotStore(indexed)
    store.initialize()
    with closing(sqlite3.connect(indexed)) as db, db:
        db.execute("CREATE INDEX injected_index ON manifest(value)")
    with pytest.raises(SnapshotStoreCorruption, match="schema"):
        store.head()

    foreign = tmp_path / "foreign.sqlite3"
    with closing(sqlite3.connect(foreign)) as db, db:
        db.execute("CREATE TABLE unrelated(value TEXT)")
        db.execute("INSERT INTO unrelated(value) VALUES('must-survive')")
    with pytest.raises(SnapshotStoreCorruption, match="geen snapshot-store"):
        OperationalSnapshotStore(foreign).initialize()
    with closing(sqlite3.connect(foreign)) as db, db:
        assert db.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
        assert db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%'"
        ).fetchall() == [("unrelated",)]
        assert db.execute("SELECT value FROM unrelated").fetchone()[0] == (
            "must-survive"
        )


def test_commit_id_is_one_way_and_snapshot_secrets_never_persist(tmp_path):
    path = tmp_path / "privacy.sqlite3"
    raw_commit_id = "eyJhbGciOiJIUzI1NiJ9.abcdefghijklmnop.signaturepart"
    store = OperationalSnapshotStore(path, clock=lambda: NOW)
    store.commit(raw_commit_id, expected_revision=0, snapshot=_snapshot())
    with closing(sqlite3.connect(path)) as db, db:
        stored_id = db.execute(
            "SELECT commit_id FROM snapshot_commit_receipts"
        ).fetchone()[0]
    assert stored_id.startswith("commit-")
    assert stored_id != raw_commit_id
    assert raw_commit_id not in path.read_bytes().decode("latin1")

    secret_path = tmp_path / "secret.sqlite3"
    secret_snapshot = _snapshot()
    secret_snapshot["worker"]["status_text"] = (
        "Authorization=Bearer-this-must-never-persist"
    )
    secret_store = OperationalSnapshotStore(secret_path, clock=lambda: NOW)
    with pytest.raises(
        store_module.OperationalSnapshotStoreError,
        match="geheim transportmateriaal",
    ):
        secret_store.commit(
            "commit:secret", expected_revision=0, snapshot=secret_snapshot
        )
    assert secret_store.head() is None
    assert b"Bearer-this-must-never-persist" not in secret_path.read_bytes()


def test_receipt_capacity_and_sqlite_page_budget_fail_closed(tmp_path):
    path = tmp_path / "capacity.sqlite3"
    store = OperationalSnapshotStore(
        path,
        clock=lambda: NOW,
        max_database_bytes=1024 * 1024,
        max_snapshot_rows=2,
        max_commit_receipts=1,
    )
    store.commit("commit:one", expected_revision=0, snapshot=_snapshot(1))
    with pytest.raises(SnapshotStoreCapacityError, match="commitreceiptlimiet"):
        store.commit("commit:two", expected_revision=1, snapshot=_snapshot(2))
    assert store.head()["revision"] == 1

    db = store._connect()
    try:
        page_size = db.execute("PRAGMA page_size").fetchone()[0]
        maximum_pages = db.execute("PRAGMA max_page_count").fetchone()[0]
    finally:
        db.close()
    assert maximum_pages == (1024 * 1024) // page_size
    assert store.audit()["database_bytes"] <= 1024 * 1024


def test_sqlite_full_is_mapped_to_capacity_error_and_rolls_back(tmp_path):
    path = tmp_path / "sqlite-full.sqlite3"

    def fail(point: str) -> None:
        if point == "after_snapshot_insert":
            raise sqlite3.OperationalError("database or disk is full")

    store = OperationalSnapshotStore(
        path,
        clock=lambda: NOW,
        fault_injector=fail,
    )
    with pytest.raises(SnapshotStoreCapacityError, match="SQLite"):
        store.commit("commit:one", expected_revision=0, snapshot=_snapshot())
    recovered = OperationalSnapshotStore(path)
    assert recovered.head() is None
    with closing(sqlite3.connect(path)) as db, db:
        assert db.execute("SELECT count(*) FROM snapshot_envelopes").fetchone()[0] == 0
        assert db.execute(
            "SELECT count(*) FROM snapshot_commit_receipts"
        ).fetchone()[0] == 0


@pytest.mark.parametrize("bad_clock", (True, float("nan"), float("inf"), -1.0))
def test_nonfinite_boolean_or_negative_store_clock_never_commits(
    tmp_path, bad_clock,
):
    path = tmp_path / "clock.sqlite3"
    store = OperationalSnapshotStore(path, clock=lambda: bad_clock)
    with pytest.raises(store_module.OperationalSnapshotStoreError, match="storeklok"):
        store.commit("commit:one", expected_revision=0, snapshot=_snapshot())
    assert store.head() is None


def test_retry_of_old_receipt_after_new_head_is_still_exact(tmp_path):
    path = tmp_path / "old-retry.sqlite3"
    store = OperationalSnapshotStore(path, clock=lambda: NOW)
    first = store.commit("commit:one", expected_revision=0, snapshot=_snapshot(1))
    store.commit("commit:two", expected_revision=1, snapshot=_snapshot(2))

    retry = OperationalSnapshotStore(path, clock=lambda: NOW + 999).commit(
        "commit:one", expected_revision=0, snapshot=_snapshot(1)
    )

    assert retry == first
    assert store.head()["revision"] == 2
