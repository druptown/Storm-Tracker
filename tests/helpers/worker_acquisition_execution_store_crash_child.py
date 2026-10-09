"""Closed subprocess driver for execution-receipt hard-exit boundaries."""
from __future__ import annotations

import os
from pathlib import Path
import sys

from storm_tracker_worker.acquisition_execution_store import ExecutionReceiptStore


NOW = 1_800_100_000.0
CRASH_POINTS = (
    "record.after_receipt",
    "record.after_terminal_head",
    "record.before_commit",
)


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit(
            "usage: worker_acquisition_execution_store_crash_child.py "
            "DATABASE CHECKPOINT"
        )
    database = Path(sys.argv[1]).resolve()
    checkpoint = sys.argv[2]
    if checkpoint not in CRASH_POINTS:
        raise SystemExit("execution-store crash checkpoint is not allowlisted")

    def crash(point: str) -> None:
        if point == checkpoint:
            os._exit(73)

    store = ExecutionReceiptStore(
        database,
        clock=lambda: NOW,
        fault_injector=crash,
    )
    store.record(
        scheduler_job_id="autosched:" + "a" * 64,
        acquisition_job_id="acquisition:" + "b" * 64,
        binding_sha256="c" * 64,
        attempt_count=1,
        outcome="completed_shadow_raw",
        reason_code="capture_committed",
        terminal=True,
        content_sha256="d" * 64,
        raw_receipt_sha256="e" * 64,
        capture_sha256="f" * 64,
        capacity_commit_sha256="1" * 64,
    )
    raise SystemExit("allowlisted execution-store checkpoint was not reached")


if __name__ == "__main__":
    main()
