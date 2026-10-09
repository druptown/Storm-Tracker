"""Abrupt subprocess for ground-fact SQLite crash-boundary tests."""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys

from storm_tracker_worker.canonical_ground_evidence_store import (
    CanonicalGroundEvidenceStore,
)
from storm_tracker_worker.raw_artifacts import RawArtifactStore


def main() -> None:
    catalog_path = Path(sys.argv[1]).resolve()
    raw_root = Path(sys.argv[2]).resolve()
    mode = sys.argv[3]
    checkpoint = sys.argv[4]

    def fault(actual: str) -> None:
        if actual == checkpoint:
            os._exit(73)

    raw_store = RawArtifactStore(raw_root)
    raw_store.initialize()
    store = CanonicalGroundEvidenceStore(
        catalog_path,
        raw_artifact_reader=raw_store,
        clock=lambda: 1_800_000_080.0,
        fault_injector=fault,
    )
    if mode == "initialize":
        if checkpoint != "none":
            raise SystemExit("initialization fault checkpoint was not reached")
        return
    if mode not in {"commit", "commit_batch"}:
        raise SystemExit("unsupported helper mode")
    request = json.loads(sys.argv[5])
    if mode == "commit":
        store.commit(request)
    else:
        store.commit_batch(request)
    if checkpoint != "none":
        raise SystemExit(f"{mode} fault checkpoint was not reached")


if __name__ == "__main__":
    main()
