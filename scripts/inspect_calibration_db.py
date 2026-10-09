"""Read-only inventory of calibration data relevant to radar replay."""
from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import sys


def main() -> None:
    path = Path(sys.argv[1]).resolve()
    database = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    database.row_factory = sqlite3.Row
    tables = [
        row["name"]
        for row in database.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'table' ORDER BY name"
        )
    ]
    result = {"path": str(path), "tables": {}}
    for table in tables:
        columns = [
            row["name"]
            for row in database.execute(f"PRAGMA table_info({table})")
        ]
        entry = {
            "count": database.execute(
                f"SELECT COUNT(*) AS n FROM {table}"
            ).fetchone()["n"],
            "columns": columns,
        }
        source_columns = [
            column for column in columns
            if "source" in column or "provider" in column
        ]
        distributions = {}
        for column in source_columns:
            distributions[column] = [
                dict(row)
                for row in database.execute(
                    f"SELECT {column} AS value, COUNT(*) AS count "
                    f"FROM {table} GROUP BY {column} ORDER BY count DESC"
                ).fetchall()
            ]
        if distributions:
            entry["source_distributions"] = distributions
        result["tables"][table] = entry
    if {"frames", "frame_points"} <= set(tables):
        kmi_frames = [
            dict(row)
            for row in database.execute(
                """
                SELECT f.id, f.region_key, f.nominal_minute,
                       f.product_timestamp, f.grid_deg,
                       f.observation_count, f.grid_point_count,
                       COUNT(p.frame_id) AS stored_points,
                       MAX(p.max_intensity) AS peak_intensity
                FROM frames AS f
                LEFT JOIN frame_points AS p ON p.frame_id = f.id
                WHERE f.source = 'kmi'
                GROUP BY f.id
                ORDER BY f.region_key, f.nominal_minute
                """
            )
        ]
        by_region: dict[str, list[dict]] = {}
        for frame in kmi_frames:
            by_region.setdefault(frame["region_key"], []).append(frame)
        result["kmi_frame_summary"] = {
            region: {
                "frames": len(frames),
                "wet_frames": sum(
                    int(frame["stored_points"] or 0) > 0 for frame in frames
                ),
                "first_nominal_minute": frames[0]["nominal_minute"],
                "last_nominal_minute": frames[-1]["nominal_minute"],
                "max_stored_points": max(
                    int(frame["stored_points"] or 0) for frame in frames
                ),
                "max_peak_intensity": max(
                    int(frame["peak_intensity"] or 0) for frame in frames
                ),
            }
            for region, frames in by_region.items()
        }
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
