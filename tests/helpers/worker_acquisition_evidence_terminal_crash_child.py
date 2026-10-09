"""Closed real-process driver for lifecycle claim and terminal transactions."""
from __future__ import annotations

import os
from pathlib import Path
import sys
from typing import Sequence


REPOSITORY = Path(__file__).resolve().parents[2]
if str(REPOSITORY) not in sys.path:
    sys.path.insert(0, str(REPOSITORY))

from storm_tracker_worker.acquisition_evidence_lifecycle import (  # noqa: E402
    AcquisitionEvidenceLifecycleStore,
)


PUBLISH_NOW = 1_800_200_000.0
CLAIM_NOW = PUBLISH_NOW + 10.0
TERMINAL_NOW = PUBLISH_NOW + 20.0
LEASE_SECONDS = 120.0
LEASE_TOKEN = "closed-terminal-lease-token"
OWNER_ID = "closed-terminal-owner"
CONSUMER_ID = "closed-terminal-consumer"
STREAM_ID = "closed.terminal.stream"
OUTPUT_SHA256 = "3" * 64
ACTIVE_DESIRED_STATE_SHA256 = "4" * 64
EVIDENCE_FINGERPRINT_SHA256 = "5" * 64

_CHECKPOINT_TO_OPERATION = {
    "claim.before_commit": "claim",
    "claim.after_commit": "claim",
    "ack.before_receipt_commit": "ack",
    "ack.after_receipt_commit": "ack",
    "ack.before_cursor_commit": "ack",
    "ack.after_cursor_commit": "ack",
    "reject.before_receipt_commit": "reject",
    "reject.after_receipt_commit": "reject",
    "reject.before_cursor_commit": "reject",
    "reject.after_cursor_commit": "reject",
    "quarantine.before_receipt_commit": "quarantine",
    "quarantine.after_receipt_commit": "quarantine",
    "quarantine.before_cursor_commit": "quarantine",
    "quarantine.after_cursor_commit": "quarantine",
}


def _store(root: Path, crash) -> AcquisitionEvidenceLifecycleStore:
    return AcquisitionEvidenceLifecycleStore(
        (root / "lifecycle").resolve(),
        clock=lambda: PUBLISH_NOW,
        token_factory=lambda: LEASE_TOKEN,
        fault_injector=crash,
        directory_fsync=lambda _path: None,
    )


def _publish(store: AcquisitionEvidenceLifecycleStore):
    return store.publish(
        acquisition_job_id="closed-terminal-job",
        consumer_id=CONSUMER_ID,
        stream_id=STREAM_ID,
        desired_state_sha256="1" * 64,
        authority_sha256="2" * 64,
        proof={
            "case": "terminal-transaction-hard-exit",
            "schema": "stv3-test-evidence-terminal-proof-v1",
        },
        payload=b"closed-terminal-exact-raw",
    )


def _claim(store: AcquisitionEvidenceLifecycleStore):
    lease = store.claim_next(
        CONSUMER_ID,
        STREAM_ID,
        OWNER_ID,
        lease_seconds=LEASE_SECONDS,
        now=CLAIM_NOW,
    )
    if lease is None:
        raise RuntimeError("closed terminal fixture did not obtain its lease")
    return lease


def _run(root: Path, operation: str, checkpoint: str) -> None:
    def crash(point: str) -> None:
        if point == checkpoint:
            os._exit(73)

    store = _store(root, crash)
    producer = _publish(store)
    if operation == "claim":
        _claim(store)
        return

    lease = _claim(store)
    if lease.event_id != producer.event_id:
        raise RuntimeError("closed terminal fixture leased another event")
    if operation == "ack":
        store.acknowledge(
            lease.event_id,
            lease.lease_token,
            output_sha256=OUTPUT_SHA256,
            now=TERMINAL_NOW,
        )
    elif operation == "reject":
        store.reject_leased(
            lease.event_id,
            lease.lease_token,
            expected_event_sha256=lease.event_sha256,
            reason_code="desired_state_changed_after_processing",
            active_desired_state_sha256=ACTIVE_DESIRED_STATE_SHA256,
            now=TERMINAL_NOW,
        )
    elif operation == "quarantine":
        store.quarantine_leased(
            lease.event_id,
            lease.lease_token,
            expected_event_sha256=lease.event_sha256,
            reason_code="decoder_output_integrity_failed",
            evidence_fingerprint_sha256=EVIDENCE_FINGERPRINT_SHA256,
            now=TERMINAL_NOW,
        )
    else:  # pragma: no cover - the closed argument map owns this branch.
        raise RuntimeError("unknown closed terminal operation")


def main(arguments: Sequence[str]) -> None:
    if len(arguments) != 3:
        raise SystemExit(
            "usage: worker_acquisition_evidence_terminal_crash_child.py "
            "ROOT OPERATION CHECKPOINT"
        )
    root_value, operation, checkpoint = arguments
    if _CHECKPOINT_TO_OPERATION.get(checkpoint) != operation:
        raise SystemExit(
            "evidence terminal operation/checkpoint is not allowlisted"
        )
    root = Path(root_value)
    if not root.is_absolute() or not root.is_dir() or root.is_symlink():
        raise SystemExit(
            "evidence terminal crash root must be an existing real directory"
        )
    if any(root.iterdir()):
        raise SystemExit("evidence terminal crash root must be empty")
    _run(root.resolve(), operation, checkpoint)
    raise RuntimeError("allowlisted evidence terminal checkpoint was not reached")


if __name__ == "__main__":
    main(sys.argv[1:])
