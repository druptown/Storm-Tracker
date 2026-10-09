"""One-shot, fail-closed repair for the backed-up live revision-348 tail."""
from __future__ import annotations

from contextlib import closing
import json
from pathlib import Path
import time

from storm_tracker_worker.desired_state import DesiredStateStore
from storm_tracker_worker.operational_fact_store import OperationalFactRevisionStore
from storm_tracker_worker.operational_snapshot_store import OperationalSnapshotStore


ROOT = Path("/data/storm_tracker_worker_runtime_production_v1/pipeline")
EXPECTED_ITEM_ID = "worker-only:68:9fa81cc09b01bdfd631dd94c"
EXPECTED_REVISION = 348
EXPECTED_SEQUENCE = 303
EXPECTED_SNAPSHOT_HEAD = 347


desired = DesiredStateStore(ROOT / "desired.sqlite3").load_active()
if desired is None:
    raise RuntimeError("active desired state ontbreekt")
snapshot_head = OperationalSnapshotStore(ROOT / "snapshots.sqlite3").head()
if snapshot_head is None or snapshot_head["revision"] != EXPECTED_SNAPSHOT_HEAD:
    raise RuntimeError("snapshothead is niet exact de verwachte revision 347")

store = OperationalFactRevisionStore(ROOT / "facts.sqlite3")
before = store.diagnostics()
if before["ready"] != 0 or before["head_sequence"] != EXPECTED_SEQUENCE:
    raise RuntimeError("fact-store is sinds de diagnose gewijzigd")

with store._lock, closing(store._connect()) as database:
    database.execute("BEGIN IMMEDIATE")
    try:
        state, records = store._verify_all(database)
        candidate = records[-1]
        if (
            candidate.pipeline_item_id != EXPECTED_ITEM_ID
            or candidate.sequence != EXPECTED_SEQUENCE
            or candidate.snapshot_revision != EXPECTED_REVISION
            or candidate.desired_state_sha256 != desired.state_sha256
            or candidate.authority_epoch != desired.authority_epoch
            or candidate.state != "superseded"
            or candidate.terminal_reason != "fact_revision_expired"
            or int(state["head_sequence"]) != candidate.sequence
        ):
            raise RuntimeError("verlopen tailrecord wijkt af van de diagnose")
        removed = database.execute(
            "DELETE FROM fact_revisions WHERE pipeline_item_id=? "
            "AND record_sha256=? AND state='superseded'",
            (candidate.pipeline_item_id, candidate.record_sha256),
        ).rowcount
        if removed != 1:
            raise RuntimeError("exact verlopen tailrecord is niet verwijderd")
        previous = records[-2]
        if previous.sequence != candidate.sequence - 1:
            raise RuntimeError("fact-store tail is niet aaneengesloten")
        store._write_state(
            database,
            head_sequence=previous.sequence,
            first_retained_sequence=int(state["first_retained_sequence"]),
            pruned_through_sequence=int(state["pruned_through_sequence"]),
            retained_rows=int(state["retained_rows"]) - 1,
            head_record_sha256=previous.record_sha256,
            last_pruned_record_sha256=state["last_pruned_record_sha256"],
            updated_at=time.time(),
        )
        database.commit()
    except BaseException:
        database.rollback()
        raise

audit = store.audit()
after = store.diagnostics()
if (
    after["head_sequence"] != EXPECTED_SEQUENCE - 1
    or after["retained_rows"] != before["retained_rows"] - 1
    or after["ready"] != 0
):
    raise RuntimeError("fact-store herstelcontrole faalde")

print(json.dumps({
    "audit": audit,
    "desired_state_sha256": desired.state_sha256,
    "removed_pipeline_item_id": EXPECTED_ITEM_ID,
    "removed_snapshot_revision": EXPECTED_REVISION,
    "snapshot_head_unchanged": snapshot_head["revision"],
    "store_after": after,
}, allow_nan=False, separators=(",", ":"), sort_keys=True))
