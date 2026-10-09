"""Export the longest consecutive wet KMI sequence from calibration SQLite."""
from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import sys


def main() -> None:
    source = Path(sys.argv[1]).resolve()
    destination = Path(sys.argv[2]).resolve()
    database = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
    database.row_factory = sqlite3.Row
    frames = [
        dict(row)
        for row in database.execute(
            """
            SELECT f.*, COUNT(p.frame_id) AS stored_points
            FROM frames AS f
            LEFT JOIN frame_points AS p ON p.frame_id = f.id
            WHERE f.source = 'kmi'
            GROUP BY f.id
            ORDER BY f.nominal_minute
            """
        )
    ]
    runs: list[list[dict]] = []
    current: list[dict] = []
    for frame in frames:
        wet = int(frame["stored_points"] or 0) > 0
        consecutive = (
            not current
            or 0 < int(frame["nominal_minute"])
            - int(current[-1]["nominal_minute"]) <= 10
        )
        if wet and consecutive:
            current.append(frame)
        else:
            if current:
                runs.append(current)
            current = [frame] if wet else []
    if current:
        runs.append(current)
    selected = max(runs, key=len)

    output_frames = []
    for frame in selected:
        points = database.execute(
            """
            SELECT grid_lat, grid_lon, max_intensity
            FROM frame_points WHERE frame_id = ?
            ORDER BY grid_lat DESC, grid_lon
            """,
            (frame["id"],),
        ).fetchall()
        grid = float(frame["grid_deg"])
        west = float(frame["coverage_lon_min"])
        north = float(frame["coverage_lat_max"])
        north_index = round(north / grid)
        west_index = round(west / grid)
        cells = [
            [
                north_index - int(point["grid_lat"]),
                int(point["grid_lon"]) - west_index,
                int(point["max_intensity"]),
            ]
            for point in points
        ]
        timestamp = int(frame["nominal_minute"]) * 60
        frame_payload = {
            "source": "kmi",
            "nominal_timestamp": timestamp,
            "grid_deg": grid,
            "bounds": [
                west,
                float(frame["coverage_lat_min"]),
                float(frame["coverage_lon_max"]),
                north,
            ],
            "cells": cells,
        }
        output_frames.append({
            "nominal_timestamp": timestamp,
            "frame_json": json.dumps(frame_payload, separators=(",", ":")),
        })
    payload = {
        "region_id": "region-1",
        "source_database": str(source),
        "sequence_count": len(runs),
        "selected_frame_count": len(selected),
        "frames": output_frames,
    }
    destination.write_text(json.dumps(payload), encoding="utf-8")
    print(json.dumps({
        "destination": str(destination),
        "available_wet_sequences": len(runs),
        "selected_frames": len(selected),
        "first_timestamp": output_frames[0]["nominal_timestamp"],
        "last_timestamp": output_frames[-1]["nominal_timestamp"],
        "total_points": sum(
            len(json.loads(item["frame_json"])["cells"])
            for item in output_frames
        ),
    }, indent=2))


if __name__ == "__main__":
    main()
