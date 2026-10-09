"""Hold the candidate's cross-process lock for one adversarial test."""

from __future__ import annotations

from pathlib import Path
import sys
import time

from storm_tracker_worker import goes_rrqpe_native as native
from storm_tracker_worker.raw_artifacts import RawArtifactStore


store = RawArtifactStore(Path(sys.argv[1]))
store.initialize()
with native._deadline_file_lock(store, deadline=time.monotonic() + 10):
    print("locked", flush=True)
    sys.stdin.readline()
