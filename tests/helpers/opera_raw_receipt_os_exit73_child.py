"""Hard-exit after OPERA receipt durability but before spool cleanup."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from types import SimpleNamespace
import sys

from storm_tracker_worker import opera_native
from storm_tracker_worker.raw_artifacts import RawArtifactStore


PRODUCT_TIMESTAMP = 1_800_000_000.0
PAYLOAD = b"opera-durable-receipt-before-hard-exit"


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit(2)
    raw_root = Path(sys.argv[1]).resolve(strict=True)
    raw_identity = sys.argv[2]
    revision = opera_native.OperaRevisionIdentity(
        etag='"0123456789abcdef0123456789abcdef"',
        version_id=None,
        content_length=len(PAYLOAD),
        content_type="application/x-hdf5",
    )

    def download(_object_key, observed, *, deadline, destination_path):
        if observed != revision or deadline <= 0:
            raise RuntimeError("unexpected OPERA child download contract")
        path = Path(destination_path)
        path.write_bytes(PAYLOAD)
        return opera_native.OperaDownloadedFile(
            path=str(path),
            size_bytes=len(PAYLOAD),
            content_sha256=hashlib.sha256(PAYLOAD).hexdigest(),
            content_type=revision.content_type,
            received_at=PRODUCT_TIMESTAMP,
            etag=revision.etag,
            version_id=revision.version_id,
        )

    def checkpoint(point: str) -> None:
        if point != "raw_only.after_receipt_commit_before_spool_cleanup":
            raise RuntimeError("unexpected OPERA hard-exit checkpoint")
        os._exit(73)

    opera_native._head_hdf5 = lambda _object_key, *, deadline: revision
    opera_native._download_hdf5 = download
    opera_native._raw_only_checkpoint = checkpoint
    opera_native.capture_opera_dbzh_hdf5_raw_only(
        SimpleNamespace(
            job_id=opera_native._canonical_job_id(PRODUCT_TIMESTAMP),
            payload={"product_timestamp": PRODUCT_TIMESTAMP},
        ),
        raw_store=RawArtifactStore(raw_root),
        raw_identity=raw_identity,
    )
    raise RuntimeError("OPERA child missed its hard-exit checkpoint")


if __name__ == "__main__":
    main()
