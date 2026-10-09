"""Closed hard-exit driver for the capacity-ledger boundaries not in the legacy matrix."""
from __future__ import annotations

import os
from pathlib import Path
import sys

from storm_tracker_worker.raw_capacity_ledger import (
    ProviderCapacityLimit,
    RawCapacityLedger,
    RawCapacityProfile,
)


NOW = 1_800_300_000.0
CONTENT = "1" * 64
RECEIPT = "2" * 64
CAPTURE = "3" * 64
POINT_TO_OPERATION = {
    "initialize.before_commit": "initialize",
    "initialize.after_commit": "initialize",
    "reserve.after_begin": "reserve",
    "reserve.after_reservation": "reserve",
    "reserve.before_commit": "reserve",
    "commit.after_begin": "commit",
    "commit.after_reservation": "commit",
    "commit.after_aggregate": "commit",
    "commit.before_commit": "commit",
}


def _profile() -> RawCapacityProfile:
    return RawCapacityProfile(
        global_max_bytes=1_000,
        global_max_entries=20,
        providers=(
            ProviderCapacityLimit("kmi", 800, 16),
            ProviderCapacityLimit("netatmo", 800, 16),
        ),
        max_reservations=100,
        max_catalog_bytes=4 * 1024 * 1024,
        sqlite_busy_timeout_seconds=2.0,
    )


def main() -> None:
    if len(sys.argv) != 4:
        raise SystemExit(
            "usage: worker_raw_capacity_ledger_missing_crash_child.py "
            "DATABASE OPERATION CHECKPOINT"
        )
    database = Path(sys.argv[1]).resolve()
    operation = sys.argv[2]
    checkpoint = sys.argv[3]
    if POINT_TO_OPERATION.get(checkpoint) != operation:
        raise SystemExit("capacity-ledger crash operation/checkpoint is not allowlisted")

    def crash(point: str) -> None:
        if point == checkpoint:
            os._exit(73)

    ledger = RawCapacityLedger(
        database,
        profile=_profile(),
        clock=lambda: NOW,
        fault_injector=crash,
    )
    if operation == "reserve":
        ledger.reserve(
            reservation_id="crash-reservation",
            provider="kmi",
            job_identity="crash-job",
            requested_bytes=100,
            requested_entries=2,
        )
    elif operation == "commit":
        ledger.commit(
            reservation_id="crash-reservation",
            actual_bytes=80,
            actual_entries=1,
            content_sha256=CONTENT,
            receipt_sha256=RECEIPT,
            capture_sha256=CAPTURE,
        )
    elif operation != "initialize":
        raise RuntimeError("unknown capacity-ledger crash operation")
    raise SystemExit("allowlisted capacity-ledger checkpoint was not reached")


if __name__ == "__main__":
    main()
