"""Hold the AEMET cross-process capture slot until the parent terminates us."""
from __future__ import annotations

from pathlib import Path
import sys
import time

from storm_tracker_worker import aemet_native as native
from storm_tracker_worker.raw_artifacts import RawArtifactStore


store = RawArtifactStore(Path(sys.argv[1]))
store.initialize()
with native._deadline_file_lock(store, deadline=time.monotonic() + 30.0):
    print("LOCKED", flush=True)
    time.sleep(30.0)
