"""Closed hard-exit driver for the scheduler boundaries not in the legacy matrix."""
from __future__ import annotations

import os
from pathlib import Path
import sys

from storm_tracker_worker.autonomous_scheduler import (
    AutonomousScheduler,
    ScheduleIntent,
)
from storm_tracker_worker.desired_state import DesiredStateStore


NOW = 1_800_000_000.0
POINT_TO_OPERATION = {
    "reconcile.after_insert": "reconcile",
    "claim.after_update": "claim",
    "retry.after_update": "retry",
    "retry.before_commit": "retry",
    "retry.after_commit": "retry",
    "complete.after_update": "complete",
    "complete.before_commit": "complete",
    "complete.after_commit": "complete",
}


def plan_builder(_state):
    return [ScheduleIntent(
        intent_id="regional-radar",
        job_type="meteorological_cycle",
        cadence_seconds=300,
        deadline_seconds=250,
        priority=20,
        payload={
            "credential_ref": "worker_environment",
            "latitude": 51.04123,
            "longitude": 4.48123,
            "marker": "plan-a",
        },
    )]


def main() -> None:
    if len(sys.argv) != 5:
        raise SystemExit(
            "usage: worker_autonomous_scheduler_missing_crash_child.py "
            "SCHEDULER DESIRED OPERATION CHECKPOINT"
        )
    scheduler_path = Path(sys.argv[1]).resolve()
    desired_path = Path(sys.argv[2]).resolve()
    operation = sys.argv[3]
    checkpoint = sys.argv[4]
    if POINT_TO_OPERATION.get(checkpoint) != operation:
        raise SystemExit("scheduler crash operation/checkpoint is not allowlisted")

    def crash(point: str) -> None:
        if point == checkpoint:
            os._exit(73)

    engine = AutonomousScheduler(
        scheduler_path,
        desired_state_store=DesiredStateStore(desired_path),
        plan_builder=plan_builder,
        lease_seconds=10.0,
        retry_base_seconds=2.0,
        fault_injector=crash,
    )
    if operation == "reconcile":
        engine.reconcile(now=NOW)
    else:
        engine.reconcile(now=NOW)
        claim = engine.claim_next("serial-worker", now=NOW + 1.0)
        if claim is None:
            raise RuntimeError("scheduler crash setup could not claim its job")
        if operation == "claim":
            raise SystemExit("allowlisted claim checkpoint was not reached")
        if operation == "retry":
            engine.retry(
                claim.job_id,
                claim.lease_token,
                "upstream_timeout",
                now=NOW + 2.0,
            )
        elif operation == "complete":
            engine.complete(
                claim.job_id,
                claim.lease_token,
                now=NOW + 2.0,
            )
        else:
            raise RuntimeError("unknown scheduler crash operation")
    raise SystemExit("allowlisted scheduler checkpoint was not reached")


if __name__ == "__main__":
    main()
