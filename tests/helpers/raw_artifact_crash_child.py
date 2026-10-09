"""Abrupt subprocess used to prove RawArtifactStore commit boundaries."""
from __future__ import annotations

import os
from pathlib import Path
import sys

from storm_tracker_worker.tracking_core.engine.raw_artifact_store import (
    RawArtifactMember,
    RawArtifactStore,
)


def main() -> None:
    path = Path(sys.argv[1])
    crash_checkpoint = sys.argv[2]

    def fault(checkpoint: str) -> None:
        if checkpoint == crash_checkpoint:
            os._exit(73)

    store = RawArtifactStore(path, fault_injector=fault)
    store.initialize()
    if crash_checkpoint.startswith("store_set_"):
        store.store_set(
            provider_id="crash-provider",
            upstream_set_id="set-0001",
            members=(
                RawArtifactMember(
                    member_id="catalog",
                    role="authoritative-catalog",
                    upstream_record_id="catalog-0001",
                    payload=b'{"frame":"exact"}',
                    media_type="application/json",
                    parser_version="crash-parser-v1",
                ),
                RawArtifactMember(
                    member_id="frame",
                    role="authoritative-frame",
                    upstream_record_id="frame-0001",
                    payload=b"\x89PNG\r\n\x1a\nexact-crash-frame",
                    media_type="image/png",
                    parser_version="crash-parser-v1",
                ),
            ),
            fetched_at=1_800_000_010.0,
            nominal_at=1_800_000_000.0,
            provenance_sha256="c" * 64,
        )
    else:
        store.store(
            provider_id="crash-provider",
            upstream_record_id="frame-0001",
            payload=b"\x00exact-crash-boundary-bytes",
            fetched_at=1_800_000_010.0,
            nominal_at=1_800_000_000.0,
            media_type="application/octet-stream",
            parser_version="crash-parser-v1",
        )
    raise SystemExit("fault checkpoint werd niet bereikt")


if __name__ == "__main__":
    main()
