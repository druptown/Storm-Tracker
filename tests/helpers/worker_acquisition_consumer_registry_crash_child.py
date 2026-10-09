"""Closed hard-exit driver for every consumer-registry transaction boundary."""
from __future__ import annotations

import os
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
TESTS = ROOT / "tests"
for entry in (str(TESTS), str(ROOT)):
    if entry not in sys.path:
        sys.path.insert(0, entry)

from storm_tracker_worker.acquisition_consumer_registry import (  # noqa: E402
    AcquisitionConsumerRegistryAuthority,
)
from test_worker_acquisition_consumer_registry import (  # noqa: E402
    NOW,
    _archive_authorities,
)


POINT_TO_OPERATION = {
    "initialize.before_commit": "initialize",
    "initialize.after_commit": "initialize",
    "owner.before_commit": "owner",
    "owner.after_commit": "owner",
    "epoch.after_root_insert": "epoch",
    "epoch.after_epoch_insert": "epoch",
    "epoch.before_commit": "epoch",
    "epoch.after_commit": "epoch",
}


def main() -> None:
    if len(sys.argv) != 4:
        raise SystemExit(
            "usage: worker_acquisition_consumer_registry_crash_child.py "
            "DATABASE OPERATION CHECKPOINT"
        )
    database = Path(sys.argv[1]).resolve()
    operation = sys.argv[2]
    checkpoint = sys.argv[3]
    if POINT_TO_OPERATION.get(checkpoint) != operation:
        raise SystemExit(
            "consumer-registry crash operation/checkpoint is not allowlisted"
        )

    def crash(point: str) -> None:
        if point == checkpoint:
            os._exit(73)

    authority = AcquisitionConsumerRegistryAuthority(
        database,
        clock=lambda: NOW,
        token_factory=lambda: "1" * 64,
        fault_injector=crash,
    )
    if operation == "epoch":
        catalog, reproof = _archive_authorities()
        authority.ensure_epoch(
            source_catalog=catalog,
            source_reproof_authority=reproof,
        )
    authority.close()
    raise SystemExit("allowlisted consumer-registry checkpoint was not reached")


if __name__ == "__main__":
    main()
