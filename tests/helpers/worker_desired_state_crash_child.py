"""Abrupt-exit helper for desired-state SQLite durability tests."""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys

from storm_tracker_worker.desired_state import DesiredStateStore


database_path = Path(sys.argv[1])
operation = sys.argv[2]
checkpoint = sys.argv[3]
payload_path = Path(sys.argv[4])


def crash(point: str) -> None:
    if point == checkpoint:
        os._exit(73)


store = DesiredStateStore(database_path, fault_injector=crash)
payload = json.loads(payload_path.read_text(encoding="utf-8"))
if operation == "claim":
    store.claim_producer(
        payload["producer_id"],
        payload["claim_id"],
        claimed_at=float(payload["claimed_at"]),
    )
elif operation == "apply":
    store.apply(
        payload["desired_state"],
        received_at=float(payload["received_at"]),
    )
else:
    raise SystemExit("unknown desired-state crash operation")
raise SystemExit("desired-state crash checkpoint was not reached")
