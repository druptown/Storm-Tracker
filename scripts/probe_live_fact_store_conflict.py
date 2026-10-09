"""Read-only bounded probe for a stopped worker's operational stores."""
from __future__ import annotations

import json
from pathlib import Path
import sqlite3


ROOT = Path("/data/storm_tracker_worker_runtime_production_v1/pipeline")


def _rows(database: Path, query: str) -> list[dict[str, object]]:
    uri = f"file:{database.as_posix()}?mode=ro&immutable=1"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in connection.execute(query).fetchall()]
    finally:
        connection.close()


desired_head = _rows(
    ROOT / "desired.sqlite3",
    "SELECT active.state_sha256,active.activated_at,states.authority_epoch,"
    "states.generation,states.issued_at FROM active_desired_state AS active "
    "JOIN desired_states AS states ON states.state_sha256=active.state_sha256",
)
result = {
    "desired_head": desired_head,
    "fact_state": _rows(ROOT / "facts.sqlite3", "SELECT * FROM fact_store_state"),
    "fact_tail": _rows(
        ROOT / "facts.sqlite3",
        "SELECT pipeline_item_id,sequence,revision_sha256,snapshot_sha256,"
        "desired_state_sha256,authority_epoch,snapshot_revision,state,"
        "created_at,updated_at,terminal_reason "
        "FROM fact_revisions ORDER BY sequence DESC LIMIT 16",
    ),
    "fact_ready": _rows(
        ROOT / "facts.sqlite3",
        "SELECT desired_state_sha256,authority_epoch,count(*) AS rows,"
        "min(snapshot_revision) AS min_revision,max(snapshot_revision) AS max_revision "
        "FROM fact_revisions WHERE state='ready' "
        "GROUP BY desired_state_sha256,authority_epoch ORDER BY max(sequence)",
    ),
    "fact_current_slot": _rows(
        ROOT / "facts.sqlite3",
        "SELECT pipeline_item_id,sequence,revision_sha256,snapshot_sha256,"
        "desired_state_sha256,authority_epoch,snapshot_revision,state,"
        "created_at,updated_at,terminal_reason FROM fact_revisions "
        "WHERE snapshot_revision>=347 ORDER BY sequence",
    ),
    "snapshot_head": _rows(
        ROOT / "snapshots.sqlite3",
        "SELECT singleton,revision,envelope_sha256,activated_at "
        "FROM active_snapshot",
    ),
    "snapshot_retention": _rows(
        ROOT / "snapshots.sqlite3",
        "SELECT singleton,first_retained_revision,retained_revision_count,"
        "head_revision,head_envelope_sha256,updated_at "
        "FROM snapshot_retention_state",
    ),
    "publication_head": _rows(
        ROOT / "publication.sqlite3",
        "SELECT * FROM publication_transport_head",
    ),
}
print(json.dumps(result, allow_nan=False, separators=(",", ":"), sort_keys=True))
