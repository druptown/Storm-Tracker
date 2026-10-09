"""Closed process-exit driver for the remaining snapshot-store boundaries."""
from __future__ import annotations

from copy import deepcopy
import os
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from storm_tracker_worker.operational_snapshot_store import (  # noqa: E402
    OperationalSnapshotStore,
)


NOW = 1_800_300_000.0
MIGRATION_POINTS = frozenset({
    "migration_after_receipt_prune",
    "migration_after_envelope_prune",
    "migration_after_schema",
    "migration_before_commit",
})
COMMIT_POINTS = frozenset({
    "after_retention_receipt_prune",
    "after_retention_envelope_prune",
    "after_snapshot_insert",
    "after_head_update",
    "after_receipt_insert",
    "after_retention_update",
})


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


def main() -> None:
    if len(sys.argv) != 4:
        raise SystemExit(
            "usage: worker_operational_snapshot_store_remaining_crash_child.py "
            "DATABASE OPERATION CHECKPOINT"
        )
    path = Path(sys.argv[1])
    operation = sys.argv[2]
    checkpoint = sys.argv[3]
    allowed = {
        "commit": COMMIT_POINTS,
        "migration": MIGRATION_POINTS,
    }
    if checkpoint not in allowed.get(operation, frozenset()):
        raise SystemExit("snapshot-store operation/checkpoint is not allowlisted")
    if not path.is_absolute() or not path.is_file():
        raise SystemExit("snapshot-store database must be an existing absolute file")

    def interrupt(point: str) -> None:
        if point == checkpoint:
            os._exit(73)

    store = OperationalSnapshotStore(
        path,
        clock=lambda: NOW + 1.0,
        retention_rows=2,
        fault_injector=interrupt,
    )
    if operation == "migration":
        store.initialize()
    else:
        store.commit(
            "snapshot-restart:3",
            expected_revision=2,
            snapshot=_snapshot(3),
        )
    raise SystemExit("allowlisted snapshot-store checkpoint was not reached")


if __name__ == "__main__":
    main()
