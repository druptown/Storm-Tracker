"""Read-only inventory for the worker-only exact-raw calibration index."""
from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import sys


def main() -> None:
    path = Path(sys.argv[1]).resolve(strict=True)
    database = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    database.row_factory = sqlite3.Row
    try:
        manifest = dict(
            database.execute(
                "SELECT key,value FROM raw_calibration_manifest ORDER BY key"
            )
        )
        totals = dict(database.execute(
            "SELECT count(*) AS captures,"
            "count(DISTINCT source_id) AS sources,"
            "count(DISTINCT provider) AS providers,"
            "min(received_at) AS oldest_received_at,"
            "max(received_at) AS latest_received_at "
            "FROM raw_calibration_captures"
        ).fetchone())
        sources = [dict(row) for row in database.execute(
            "SELECT source_id,provider,product,count(*) AS captures,"
            "min(received_at) AS oldest_received_at,"
            "max(received_at) AS latest_received_at "
            "FROM raw_calibration_captures "
            "GROUP BY source_id,provider,product ORDER BY source_id,provider,product"
        )]
    finally:
        database.close()
    print(json.dumps({
        "path": str(path),
        "database_bytes": path.stat().st_size,
        "manifest": manifest,
        "totals": totals,
        "sources": sources,
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
