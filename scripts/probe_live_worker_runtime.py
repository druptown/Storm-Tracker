"""Emit privacy-safe, read-only health aggregates for a live worker runtime."""

from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import time


RUNTIME_ROOT = Path("/data/storm_tracker_worker_runtime_production_v1")


def _connect(relative_path: str) -> sqlite3.Connection:
    database = (RUNTIME_ROOT / relative_path).resolve()
    return sqlite3.connect(f"file:{database.as_posix()}?mode=ro", uri=True)


def _rows(connection: sqlite3.Connection, query: str) -> list[dict[str, object]]:
    connection.row_factory = sqlite3.Row
    return [dict(row) for row in connection.execute(query)]


def main() -> None:
    result: dict[str, object] = {}
    scheduler_workloads: dict[str, str] = {}

    with _connect("source/scheduler.sqlite3") as connection:
        result["scheduler_runtime"] = _rows(
            connection,
            """SELECT last_wall_time, active_desired_state_sha256,
                      active_plan_sha256, active_intent_count
                 FROM scheduler_runtime""",
        )
        result["scheduler_states"] = _rows(
            connection,
            """SELECT state, COUNT(*) AS count, MAX(updated_at) AS latest_update
                 FROM scheduler_outbox
                GROUP BY state
                ORDER BY state""",
        )
        scheduler_recent = _rows(
            connection,
            """SELECT job_id, job_type, state, last_error_code, scheduled_for,
                      deadline_at, updated_at, terminal_at, payload_json
                 FROM scheduler_outbox
                ORDER BY updated_at DESC
                LIMIT 40""",
        )
        scheduler_history = _rows(
            connection,
            """SELECT job_id, payload_json
                 FROM scheduler_outbox""",
        )
        for row in scheduler_history:
            payload = json.loads(bytes(row["payload_json"]).decode("utf-8"))
            scheduler_workloads[str(row["job_id"])] = str(
                payload.get("workload_id", "unknown")
            )
        for row in scheduler_recent:
            payload = json.loads(bytes(row.pop("payload_json")).decode("utf-8"))
            workload_id = str(payload.get("workload_id", "unknown"))
            row.pop("job_id")
            row["workload_id"] = workload_id
        result["scheduler_recent"] = scheduler_recent[:20]

    with _connect("source/executions.sqlite3") as connection:
        result["execution_outcomes_last_hour"] = _rows(
            connection,
            """SELECT outcome, reason_code, COUNT(*) AS count,
                      MAX(created_at) AS latest
                 FROM execution_receipts
                WHERE created_at >= CAST(strftime('%s','now') AS REAL) - 3600
                GROUP BY outcome, reason_code
                ORDER BY count DESC, outcome, reason_code""",
        )
        execution_recent = _rows(
            connection,
            """SELECT scheduler_job_id, outcome, reason_code, terminal, created_at
                 FROM execution_receipts
                ORDER BY created_at DESC
                LIMIT 20""",
        )
        for row in execution_recent:
            scheduler_job_id = str(row.pop("scheduler_job_id"))
            row["workload_id"] = scheduler_workloads.get(
                scheduler_job_id, "superseded_or_reclaimed"
            )
        result["execution_recent"] = execution_recent

    with _connect("source/raw/raw_artifacts.sqlite3") as connection:
        result["raw_captures_last_hour"] = _rows(
            connection,
            """SELECT provider, product, COUNT(*) AS count,
                      MAX(created_at) AS latest
                 FROM raw_artifact_captures
                WHERE created_at >= CAST(strftime('%s','now') AS REAL) - 3600
                GROUP BY provider, product
                ORDER BY latest DESC, provider, product""",
        )

    with _connect("pipeline/completion/acquisition_evidence.sqlite3") as connection:
        result["completion_states"] = _rows(
            connection,
            """SELECT state, COUNT(*) AS count, MAX(terminal_at) AS latest_terminal
                 FROM completion_outbox
                GROUP BY state
                ORDER BY state""",
        )

    with _connect("pipeline/snapshots.sqlite3") as connection:
        result["active_snapshot"] = _rows(
            connection,
            "SELECT revision, activated_at FROM active_snapshot",
        )

    result["observed_at"] = time.time()

    print(json.dumps(result, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
