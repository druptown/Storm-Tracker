"""Closed hard-exit driver for the five remaining raw-store checkpoints."""
from __future__ import annotations

import os
from pathlib import Path
import sys

from storm_tracker_worker.raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RawArtifactStore,
)


POINT_TO_OPERATION = {
    "store_after_begin": "store",
    "store_before_receipt": "store",
    "capture_after_begin": "capture",
    "capture_before_commit": "capture",
    "capture_after_commit": "capture",
}
PROVENANCE = {
    "representation": NATIVE_REPRESENTATION,
    "capture_stage": NATIVE_CAPTURE_STAGE,
    "collector": "raw-store-crash-test",
    "collector_version": "1",
}


def put_member(
    store: RawArtifactStore,
    *,
    product: str,
    role: str,
    payload: bytes,
):
    return store.store(
        provider="kmi",
        product=product,
        fetch_id=f"raw-store-crash:{role}",
        content_type="application/octet-stream",
        received_at=1_800_200_000.0 if role == "first" else 1_800_200_001.0,
        upstream_id=f"raw-store-crash/{role}/revision-1",
        provenance=PROVENANCE,
        payload=payload,
    )


def main() -> None:
    if len(sys.argv) != 4:
        raise SystemExit(
            "usage: worker_raw_artifacts_remaining_crash_child.py "
            "ROOT OPERATION CHECKPOINT"
        )
    root = Path(sys.argv[1]).resolve()
    operation = sys.argv[2]
    checkpoint = sys.argv[3]
    if POINT_TO_OPERATION.get(checkpoint) != operation:
        raise SystemExit("raw-store crash operation/checkpoint is not allowlisted")

    def crash(point: str) -> None:
        if point == checkpoint:
            os._exit(73)

    store = RawArtifactStore(root, fault_injector=crash)
    store.initialize()
    if operation == "store":
        put_member(
            store,
            product="first-native",
            role="first",
            payload=b"exact-first-native-bytes",
        )
    elif operation == "capture":
        first = put_member(
            store,
            product="first-native",
            role="first",
            payload=b"exact-first-native-bytes",
        )
        second = put_member(
            store,
            product="second-native",
            role="second",
            payload=b"exact-second-native-bytes",
        )
        store.commit_capture(
            provider="kmi",
            product="raw-store-crash-capture",
            fetch_id="raw-store-crash:capture",
            upstream_capture_id="raw-store-crash/capture/revision-1",
            product_timestamp=1_700_000_000.0,
            members={"first": first.receipt_id, "second": second.receipt_id},
        )
    else:
        raise RuntimeError("unknown raw-store crash operation")
    raise SystemExit("allowlisted raw-store checkpoint was not reached")


if __name__ == "__main__":
    main()
