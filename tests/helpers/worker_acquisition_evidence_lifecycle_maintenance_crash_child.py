"""Closed hard-exit driver for evidence lifecycle maintenance boundaries."""
from __future__ import annotations

import os
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from storm_tracker_worker.acquisition_evidence_lifecycle import (  # noqa: E402
    AcquisitionEvidenceLifecycleStore,
    EvidenceRetentionPolicy,
)


NOW = 1_800_000_000.0
SUPERSEDE_POINTS = frozenset({
    "supersede.before_commit",
    "supersede.after_commit",
})
ARCHIVE_POINTS = frozenset({
    "archive.after_object",
    "archive.after_segment_write",
    "archive.after_segment_fsync",
    "archive.after_segment_replace",
    "archive.after_segment_publish",
    "archive.before_manifest_commit",
    "archive.after_manifest_commit",
})
PRUNE_POINTS = frozenset({
    *(f"prune.{edge}_phase_{phase}_commit" for phase in (0, 1, 2, 3, 5)
      for edge in ("before", "after")),
    "prune.after_hot_object_unlink",
    "prune.before_file_phase_commit",
    "prune.after_file_phase_commit",
})


def publish(store: AcquisitionEvidenceLifecycleStore, sequence: int):
    payload = f"maintenance-raw-{sequence}".encode("ascii")
    return store.publish(
        acquisition_job_id=f"maintenance-job-{sequence}",
        consumer_id="decoder-v1",
        stream_id="radar.maintenance.raw",
        desired_state_sha256="1" * 64,
        authority_sha256="2" * 64,
        proof={"schema": "maintenance-proof-v1", "sequence": sequence},
        payload=payload,
    )


def consume(
    store: AcquisitionEvidenceLifecycleStore,
    receipt,
    sequence: int,
) -> None:
    claim_at = NOW + float((sequence - 1) * 2)
    lease = store.claim_next(
        "decoder-v1",
        "radar.maintenance.raw",
        f"maintenance-owner-{sequence}",
        now=claim_at,
    )
    if lease is None or lease.event_id != receipt.event_id:
        raise SystemExit("maintenance crash fixture could not claim exact event")
    store.acknowledge(
        lease.event_id,
        lease.lease_token,
        output_sha256=str(sequence + 2) * 64,
        now=claim_at + 1.0,
    )


def main() -> None:
    if len(sys.argv) != 5:
        raise SystemExit(
            "usage: worker_acquisition_evidence_lifecycle_maintenance_"
            "crash_child.py LIFECYCLE_ROOT OPERATION CHECKPOINT OCCURRENCE"
        )
    lifecycle_root = Path(sys.argv[1]).resolve()
    operation = sys.argv[2]
    checkpoint = sys.argv[3]
    try:
        occurrence = int(sys.argv[4])
    except ValueError as exc:
        raise SystemExit("maintenance checkpoint occurrence is not an integer") from exc
    allowed = {
        "supersede": SUPERSEDE_POINTS,
        "archive": ARCHIVE_POINTS,
        "prune": PRUNE_POINTS,
    }
    if checkpoint not in allowed.get(operation, frozenset()):
        raise SystemExit("maintenance operation/checkpoint is not allowlisted")
    maximum_occurrence = (
        2 if checkpoint == "prune.after_hot_object_unlink" else 1
    )
    if occurrence < 1 or occurrence > maximum_occurrence:
        raise SystemExit("maintenance checkpoint occurrence is not allowlisted")

    hits = 0

    def crash(point: str) -> None:
        nonlocal hits
        if point == checkpoint:
            hits += 1
            if hits == occurrence:
                os._exit(73)

    store = AcquisitionEvidenceLifecycleStore(
        lifecycle_root,
        clock=lambda: NOW,
        token_factory=lambda: "a" * 64,
        fault_injector=crash,
        directory_fsync=lambda _path: None,
    )
    first = publish(store, 1)
    second = publish(store, 2) if operation in {"supersede", "prune"} else None
    if operation == "supersede":
        if second is None:  # pragma: no cover - closed operation contract
            raise SystemExit("supersession fixture lacks replacement")
        store.supersede(
            first.event_id,
            expected_event_sha256=first.event_sha256,
            replacement_event_id=second.event_id,
            now=NOW + 10.0,
        )
    else:
        consume(store, first, 1)
        if second is not None:
            consume(store, second, 2)
        store.rotate_archive(
            policy=EvidenceRetentionPolicy(),
            now=NOW + 10.0,
        )
    raise SystemExit("allowlisted maintenance checkpoint was not reached")


if __name__ == "__main__":
    main()
