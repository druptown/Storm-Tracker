#!/usr/bin/env python3
"""Repository wrapper for the packaged station-storage management CLI."""

from __future__ import annotations

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from storm_tracker_worker.manage_postgis_station_storage import main  # noqa: E402


if __name__ == "__main__":
    sys.exit(main())
