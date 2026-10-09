"""Barriered subprocess used to test concurrent store initialization."""
from __future__ import annotations

from pathlib import Path
import sys
import time

from storm_tracker_worker.tracking_core.engine.raw_artifact_store import (
    RawArtifactStore,
)


def main() -> None:
    database_path = Path(sys.argv[1])
    ready_path = Path(sys.argv[2])
    start_signal = Path(sys.argv[3])
    ready_path.write_text("ready", encoding="ascii")
    deadline = time.monotonic() + 20
    while not start_signal.exists():
        if time.monotonic() >= deadline:
            raise TimeoutError("initialize start signal was not received")
        time.sleep(0.005)
    RawArtifactStore(database_path).initialize()


if __name__ == "__main__":
    main()
