"""Read-only, privacy-safe live proof of worker region projection."""
from __future__ import annotations

from collections import Counter
import json
from pathlib import Path
import sqlite3

from storm_tracker_worker.desired_state import parse_desired_state
from storm_tracker_worker.worker_acquisition_plan import build_worker_acquisition_plan
from storm_tracker_worker import __version__ as worker_version


ROOT = Path("/data/storm_tracker_worker_runtime_production_v1/pipeline")


def _one(database: Path, query: str) -> sqlite3.Row:
    connection = sqlite3.connect(
        f"file:{database.as_posix()}?mode=ro",
        uri=True,
        timeout=5,
    )
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA query_only=ON")
        row = connection.execute(query).fetchone()
        if row is None:
            raise RuntimeError(f"geen rij in {database.name}")
        return row
    finally:
        connection.close()


def _many(database: Path, query: str) -> list[sqlite3.Row]:
    connection = sqlite3.connect(
        f"file:{database.as_posix()}?mode=ro",
        uri=True,
        timeout=5,
    )
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA query_only=ON")
        return list(connection.execute(query).fetchall())
    finally:
        connection.close()


desired_row = _one(
    ROOT / "desired.sqlite3",
    "SELECT states.canonical_json FROM active_desired_state AS active "
    "JOIN desired_states AS states ON states.state_sha256=active.state_sha256 "
    "WHERE active.singleton=1",
)
desired = parse_desired_state(bytes(desired_row["canonical_json"]))
plan = build_worker_acquisition_plan(desired)

snapshot_row = _one(
    ROOT / "snapshots.sqlite3",
    "SELECT envelopes.canonical_envelope FROM active_snapshot AS active "
    "JOIN snapshot_envelopes AS envelopes "
    "ON envelopes.revision=active.revision "
    "AND envelopes.envelope_sha256=active.envelope_sha256 "
    "WHERE active.singleton=1",
)
envelope = json.loads(bytes(snapshot_row["canonical_envelope"]))
snapshot = envelope["snapshot"]
regions = snapshot["regions"]["items"]
providers = snapshot["providers"]["items"]

publication_row = _one(
    ROOT / "publication.sqlite3",
    "SELECT revision,state,attempt_count FROM publication_transport_head "
    "WHERE singleton=1",
)
fact_tail = _many(
    ROOT / "facts.sqlite3",
    "SELECT snapshot_revision,state,terminal_reason,desired_state_sha256 "
    "FROM fact_revisions ORDER BY sequence DESC LIMIT 6",
)

print(json.dumps({
    "desired_generation": desired.generation,
    "desired_issued_at": desired.issued_at,
    "desired_target_count": len(desired.targets),
    "desired_available_target_count": sum(
        target.available for target in desired.targets
    ),
    "plan_region_count": len(plan.regions),
    "plan_region_sizes": sorted(len(region.target_ids) for region in plan.regions),
    "snapshot_revision": snapshot["revision"],
    "snapshot_evaluated_at": snapshot["evaluated_at"],
    "snapshot_desired_matches": (
        snapshot["desired_state_sha256"] == desired.state_sha256
    ),
    "snapshot_region_count": len(regions),
    "snapshot_region_sizes": sorted(len(region["target_ids"]) for region in regions),
    "snapshot_has_pending_region": any(
        region["region_id"] == "region:awaiting_evidence" for region in regions
    ),
    "snapshot_target_count": len(snapshot["targets"]["items"]),
    "snapshot_worker": snapshot["worker"],
    "provider_state_counts": dict(sorted(Counter(
        provider["state"] for provider in providers
    ).items())),
    "providers": [{
        "provider_id": provider["provider_id"],
        "state": provider["state"],
        "freshness": provider["freshness"],
        "reason_codes": provider["reason_codes"],
    } for provider in providers],
    "publication": {
        "revision": publication_row["revision"],
        "state": publication_row["state"],
        "attempt_count": publication_row["attempt_count"],
    },
    "fact_tail": [{
        "snapshot_revision": row["snapshot_revision"],
        "state": row["state"],
        "terminal_reason": row["terminal_reason"],
        "desired_matches": row["desired_state_sha256"] == desired.state_sha256,
    } for row in fact_tail],
    "worker_version": worker_version,
}, allow_nan=False, separators=(",", ":"), sort_keys=True))
