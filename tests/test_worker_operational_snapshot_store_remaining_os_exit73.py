from __future__ import annotations

import ast
from contextlib import closing
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

from storm_tracker_worker import operational_snapshot_store as subject
from storm_tracker_worker.operational_snapshot_store import OperationalSnapshotStore
from storm_tracker_worker.tracking_core.operational_snapshot import (
    encode_control_envelope,
)


NOW = 1_800_300_000.0
ROOT = Path(__file__).resolve().parents[1]
CRASH_HELPER = (
    ROOT
    / "tests"
    / "helpers"
    / "worker_operational_snapshot_store_remaining_crash_child.py"
)

_ALL_FAULT_POINTS = (
    "migration_after_receipt_prune",
    "migration_after_envelope_prune",
    "migration_after_schema",
    "migration_before_commit",
    "after_retention_receipt_prune",
    "after_retention_envelope_prune",
    "after_snapshot_insert",
    "after_head_update",
    "after_receipt_insert",
    "after_retention_update",
    "before_commit",
    "after_commit",
)
_EXISTING_PROCESS_POINTS = ("before_commit", "after_commit")
_MIGRATION_POINTS = _ALL_FAULT_POINTS[:4]
_COMMIT_POINTS = _ALL_FAULT_POINTS[4:10]
_MISSING_PROCESS_POINTS = _MIGRATION_POINTS + _COMMIT_POINTS


def _snapshot(revision: int) -> dict[str, object]:
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
        "provenance_reason_codes": ["restart_consistency"],
        "worker": {
            "state": "online",
            "queue_depth": revision,
            "failed_jobs": 0,
            "status_text": f"snapshot-{revision}",
        },
        "regions": deepcopy(empty),
        "targets": deepcopy(empty),
        "providers": deepcopy(empty),
        "ui_storms": deepcopy(empty),
        "map_artifact": {"artifact_handle": None, "etag": None},
    }


def _commit_range(
    store: OperationalSnapshotStore,
    last_revision: int,
) -> dict[int, dict[str, object]]:
    return {
        revision: store.commit(
            f"snapshot-restart:{revision}",
            expected_revision=revision - 1,
            snapshot=_snapshot(revision),
        )
        for revision in range(1, last_revision + 1)
    }


def _downgrade_to_exact_v1(path: Path) -> None:
    with closing(sqlite3.connect(path)) as database, database:
        database.execute("PRAGMA foreign_keys=ON")
        database.execute("DROP TABLE snapshot_retention_state")
        database.executemany(
            "UPDATE manifest SET value=? WHERE key=?",
            (
                (
                    subject.LEGACY_STORE_ALGORITHM_VERSION,
                    "algorithm_version",
                ),
                (
                    str(subject.LEGACY_STORE_SCHEMA_VERSION),
                    "schema_version",
                ),
            ),
        )


def _table_exists(database: sqlite3.Connection, name: str) -> bool:
    return database.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone() is not None


def _database_state(path: Path) -> dict[str, object]:
    with closing(sqlite3.connect(path)) as database:
        retention = (
            tuple(database.execute(
                "SELECT first_retained_revision,pruned_through_revision,"
                "retained_revision_count,head_revision,head_envelope_sha256,"
                "last_pruned_receipt_sha256,updated_at,state_sha256 "
                "FROM snapshot_retention_state WHERE singleton=1"
            ).fetchone() or ())
            if _table_exists(database, "snapshot_retention_state")
            else None
        )
        return {
            "foreign_keys": database.execute(
                "PRAGMA foreign_key_check"
            ).fetchall(),
            "integrity": database.execute(
                "PRAGMA integrity_check"
            ).fetchone()[0],
            "manifest": tuple(database.execute(
                "SELECT key,value FROM manifest ORDER BY key"
            ).fetchall()),
            "active": tuple(database.execute(
                "SELECT revision,envelope_sha256,activated_at "
                "FROM active_snapshot ORDER BY singleton"
            ).fetchall()),
            "envelopes": tuple(database.execute(
                "SELECT revision,snapshot_sha256,envelope_sha256,envelope_bytes,"
                "hex(canonical_envelope),product_valid_at,received_at,evaluated_at,"
                "committed_at FROM snapshot_envelopes ORDER BY revision"
            ).fetchall()),
            "receipts": tuple(database.execute(
                "SELECT commit_id,operation_sha256,revision,envelope_sha256,"
                "hex(canonical_result),result_sha256,receipt_sha256,committed_at "
                "FROM snapshot_commit_receipts ORDER BY revision"
            ).fetchall()),
            "retention": retention,
        }


def _state_sha256(state: dict[str, object]) -> str:
    payload = json.dumps(
        state,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _run_child(path: Path, operation: str, checkpoint: str) -> None:
    environment = os.environ.copy()
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    completed = subprocess.run(
        (
            sys.executable,
            "-B",
            str(CRASH_HELPER),
            str(path.resolve()),
            operation,
            checkpoint,
        ),
        cwd=ROOT,
        env=environment,
        capture_output=True,
        check=False,
        timeout=30.0,
    )
    assert completed.returncode == 73, (
        completed.returncode,
        completed.stdout,
        completed.stderr,
    )


def _product_fault_inventory() -> tuple[str, ...]:
    source_path = Path(subject.__file__)
    tree = ast.parse(
        source_path.read_text(encoding="utf-8"),
        filename=str(source_path),
    )
    store_class = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef)
        and node.name == "OperationalSnapshotStore"
    )
    calls = (
        call
        for call in ast.walk(store_class)
        if isinstance(call, ast.Call)
        and isinstance(call.func, ast.Attribute)
        and isinstance(call.func.value, ast.Name)
        and call.func.value.id == "self"
        and call.func.attr == "_fault"
        and len(call.args) == 1
        and isinstance(call.args[0], ast.Constant)
        and isinstance(call.args[0].value, str)
    )
    return tuple(
        str(call.args[0].value)
        for call in sorted(calls, key=lambda item: (item.lineno, item.col_offset))
    )


def test_snapshot_store_fault_inventory_is_existing_two_plus_missing_ten() -> None:
    discovered = _product_fault_inventory()
    assert discovered == _ALL_FAULT_POINTS
    assert len(discovered) == len(set(discovered)) == 12
    assert set(_EXISTING_PROCESS_POINTS).isdisjoint(_MISSING_PROCESS_POINTS)
    assert set(_EXISTING_PROCESS_POINTS) | set(_MISSING_PROCESS_POINTS) == set(
        discovered
    )
    assert len(_EXISTING_PROCESS_POINTS) == 2
    assert len(_MISSING_PROCESS_POINTS) == 10


@pytest.mark.parametrize("checkpoint", _MISSING_PROCESS_POINTS)
def test_remaining_snapshot_store_process_exit_reopens_and_replays_stably(
    tmp_path: Path,
    checkpoint: str,
) -> None:
    path = tmp_path / f"snapshot-{checkpoint}.sqlite3"
    migration = checkpoint in _MIGRATION_POINTS
    seed = OperationalSnapshotStore(
        path,
        clock=lambda: NOW,
        retention_rows=10 if migration else 2,
    )
    seed_results = _commit_range(seed, 3 if migration else 2)
    if migration:
        _downgrade_to_exact_v1(path)

    old_state = _database_state(path)
    old_sha256 = _state_sha256(old_state)
    assert old_state["integrity"] == "ok"
    assert old_state["foreign_keys"] == []
    assert tuple(row[0] for row in old_state["envelopes"]) == (
        (1, 2, 3) if migration else (1, 2)
    )
    if migration:
        assert old_state["retention"] is None
        assert dict(old_state["manifest"]) == {
            "algorithm_version": subject.LEGACY_STORE_ALGORITHM_VERSION,
            "schema_version": str(subject.LEGACY_STORE_SCHEMA_VERSION),
        }
    else:
        assert old_state["retention"][:4] == (1, 0, 2, 2)

    _run_child(path, "migration" if migration else "commit", checkpoint)

    interrupted_state = _database_state(path)
    assert interrupted_state == old_state
    assert _state_sha256(interrupted_state) == old_sha256

    reopened = OperationalSnapshotStore(
        path,
        clock=lambda: NOW + 2.0,
        retention_rows=2,
    )
    if migration:
        head = reopened.head()
        assert head is not None
        assert head["revision"] == 3
        assert head["canonical_envelope"] == encode_control_envelope(_snapshot(3))
        assert reopened.retention_status()["first_retained_revision"] == 2
        state = _database_state(path)
        assert tuple(row[0] for row in state["envelopes"]) == (2, 3)
        assert dict(state["manifest"]) == {
            "algorithm_version": subject.STORE_ALGORITHM_VERSION,
            "schema_version": str(subject.STORE_SCHEMA_VERSION),
        }
        replay = reopened.commit(
            "snapshot-restart:3",
            expected_revision=2,
            snapshot=_snapshot(3),
        )
        assert replay == seed_results[3]
    else:
        head = reopened.head()
        assert head is not None and head["revision"] == 2
        assert reopened.retention_status()["first_retained_revision"] == 1
        replay = reopened.commit(
            "snapshot-restart:3",
            expected_revision=2,
            snapshot=_snapshot(3),
        )
        assert replay["revision"] == 3
        assert reopened.head()["canonical_envelope"] == encode_control_envelope(
            _snapshot(3)
        )
        assert reopened.retention_status()["first_retained_revision"] == 2

    stable_state = _database_state(path)
    stable_sha256 = _state_sha256(stable_state)
    stable_head = reopened.head()
    stable_audit = reopened.audit()
    assert stable_audit == {
        "commit_receipts": 2,
        "database_bytes": stable_audit["database_bytes"],
        "method": subject.METHOD,
        "snapshots": 2,
        "status": "ok",
    }
    exact_replay = OperationalSnapshotStore(
        path,
        clock=lambda: NOW + 999.0,
        retention_rows=2,
    ).commit(
        "snapshot-restart:3",
        expected_revision=2,
        snapshot=_snapshot(3),
    )
    assert exact_replay == replay
    final = OperationalSnapshotStore(path, retention_rows=2)
    assert final.head() == stable_head
    assert final.audit() == stable_audit
    assert _database_state(path) == stable_state
    assert _state_sha256(_database_state(path)) == stable_sha256
