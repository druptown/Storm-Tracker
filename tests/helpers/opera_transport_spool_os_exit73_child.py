"""Leave one OPERA slot-spool file behind through an uncatchable hard exit."""

from __future__ import annotations

import os
from pathlib import Path
import sys
import time

from storm_tracker_worker import opera_native
from storm_tracker_worker.raw_artifacts import RawArtifactStore


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit(2)
    raw_root = Path(sys.argv[1]).resolve(strict=True)
    raw_identity = sys.argv[2]
    store = RawArtifactStore(raw_root)
    store.initialize()
    spool_directory = opera_native._transport_spool_directory(store)
    spool_path = opera_native._slot_spool_path(store, raw_identity)
    with opera_native._deadline_file_lock(
        opera_native._transport_spool_lock_path(store),
        deadline=time.monotonic() + 30.0,
    ):
        opera_native._reconcile_transport_spool(spool_directory)
        with opera_native._open_download_file(
            temporary_directory=None,
            destination_path=spool_path,
        ) as (stream, _path, _witness):
            stream.write(b"partial-opera-hard-exit-residue")
            stream.flush()
            os.fsync(stream.fileno())
            os._exit(73)


if __name__ == "__main__":
    main()
