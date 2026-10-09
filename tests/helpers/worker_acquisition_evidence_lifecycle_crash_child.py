"""Closed hard-exit driver for acquisition evidence publication boundaries."""
from __future__ import annotations

import os
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from storm_tracker_worker.acquisition_evidence_lifecycle import (  # noqa: E402
    AcquisitionEvidenceLifecycleStore,
)


NOW = 1_800_000_000.0
CHECKPOINTS = frozenset({
    "object.after_write",
    "object.after_fsync",
    "object.after_replace",
    "publish.after_hot_object",
    "publish.after_producer_insert",
    "publish.before_producer_commit",
    "publish.after_producer_commit",
    "publish.before_ready_commit",
    "publish.after_ready_commit",
})


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit(
            "usage: worker_acquisition_evidence_lifecycle_crash_child.py "
            "LIFECYCLE_ROOT CHECKPOINT"
        )
    lifecycle_root = Path(sys.argv[1]).resolve()
    checkpoint = sys.argv[2]
    if checkpoint not in CHECKPOINTS:
        raise SystemExit("acquisition evidence checkpoint is not allowlisted")

    def crash(point: str) -> None:
        if point == checkpoint:
            os._exit(73)

    store = AcquisitionEvidenceLifecycleStore(
        lifecycle_root,
        clock=lambda: NOW,
        token_factory=lambda: "a" * 64,
        fault_injector=crash,
        directory_fsync=lambda _path: None,
    )
    store.publish(
        acquisition_job_id="job-acquisition-evidence-lifecycle-crash",
        consumer_id="decoder-v1",
        stream_id="radar.test.raw",
        desired_state_sha256="1" * 64,
        authority_sha256="2" * 64,
        proof={"schema": "test-proof-v1", "sequence": "crash"},
        payload=b"exact-raw-acquisition-evidence-lifecycle",
    )
    raise SystemExit("allowlisted acquisition evidence checkpoint was not reached")


if __name__ == "__main__":
    main()
