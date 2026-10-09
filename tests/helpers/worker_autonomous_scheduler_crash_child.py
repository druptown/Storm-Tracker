"""Subprocess helper that simulates an abrupt scheduler power loss."""
from __future__ import annotations

import os
from pathlib import Path
import sys

from storm_tracker_worker.autonomous_scheduler import (
    AutonomousScheduler,
    ScheduleIntent,
)
from storm_tracker_worker.desired_state import DesiredStateStore


scheduler_path = Path(sys.argv[1])
desired_path = Path(sys.argv[2])
operation = sys.argv[3]
checkpoint = sys.argv[4]
now = float(sys.argv[5])


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


def power_cut(point: str) -> None:
    if point == checkpoint:
        os._exit(79)


engine = AutonomousScheduler(
    scheduler_path,
    desired_state_store=DesiredStateStore(desired_path),
    plan_builder=plan_builder,
    lease_seconds=10.0,
    retry_base_seconds=2.0,
    fault_injector=power_cut,
)
if operation == "reconcile":
    engine.reconcile(now=now)
elif operation == "claim":
    engine.claim_next("serial-worker", now=now)
else:
    raise RuntimeError("unknown crash helper operation")
