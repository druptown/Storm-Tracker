from __future__ import annotations

import hashlib
import os
from pathlib import Path
import sys

from storm_tracker_worker.raw_artifacts import RawArtifactLimits, RawArtifactStore


store_root = Path(sys.argv[1]).resolve()
source_path = Path(sys.argv[2]).resolve()
checkpoint = sys.argv[3]
payload_size = source_path.stat().st_size
digest = hashlib.sha256(source_path.read_bytes()).hexdigest()


def crash(name: str) -> None:
    if name == checkpoint:
        os._exit(73)


store = RawArtifactStore(
    store_root,
    limits=RawArtifactLimits(
        max_object_bytes=8 * 1024 * 1024,
        max_total_bytes=32 * 1024 * 1024,
        max_object_entries=16,
        max_staging_entries=4,
        max_receipt_entries=32,
        max_capture_entries=32,
        max_capture_members=4,
        max_catalog_bytes=8 * 1024 * 1024,
    ),
    fault_injector=crash,
)
store.initialize()
store.store_file(
    provider="file-test",
    product="exact-file",
    fetch_id="crash-fetch",
    content_type="application/octet-stream",
    received_at=1_800_000_000.0,
    upstream_id=f"fixture|sha256={digest}",
    provenance={
        "representation": "upstream_native_bytes",
        "capture_stage": "before_decode",
        "collector": "file-test",
        "collector_version": "file-test-v1",
    },
    source_path=source_path,
    expected_size=payload_size,
    expected_sha256=digest,
)
raise SystemExit("fault checkpoint was not reached")
