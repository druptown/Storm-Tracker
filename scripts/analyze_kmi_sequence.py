"""Inspect the real historical frames in the current KMI animation sequence."""
from __future__ import annotations

from datetime import datetime
import hashlib
import io
import json
from pathlib import Path
import sys
from urllib.request import Request, urlopen

from PIL import Image


BASE = "https://app.meteo.be/services/appv4/"
SECRET = "r9EnW374jkJ9acc"
USER_AGENT = "be.meteo.app"
COLORS = (
    ((144, 238, 144), 1),
    ((0, 200, 100), 2),
    ((0, 150, 255), 3),
    ((0, 0, 255), 4),
    ((255, 255, 0), 5),
    ((255, 150, 0), 6),
    ((255, 0, 0), 7),
    ((180, 0, 180), 8),
)


def fetch(url: str) -> bytes:
    request = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=20) as response:
        return response.read()


def intensity(pixel: tuple[int, int, int, int]) -> int:
    red, green, blue, alpha = pixel
    if alpha < 128:
        return 0
    best_distance, best_value = min(
        (
            (red - ref_red) ** 2
            + (green - ref_green) ** 2
            + (blue - ref_blue) ** 2,
            value,
        )
        for (ref_red, ref_green, ref_blue), value in COLORS
    )
    return best_value if best_distance < 20_000 else 0


def main() -> None:
    date_text = datetime.now().strftime("%d/%m/%Y")
    key = hashlib.md5(
        f"{SECRET};getForecasts;{date_text}".encode()
    ).hexdigest()
    metadata_url = (
        f"{BASE}?s=getForecasts&k={key}&lat=51.026&long=4.478"
    )
    payload = json.loads(fetch(metadata_url))
    now = datetime.now().astimezone()
    historical = [
        frame
        for frame in payload.get("animation", {}).get("sequence", ())
        if datetime.fromisoformat(frame["time"]) <= now
    ]
    frames = []
    replay_frames = []
    for frame in historical:
        image = Image.open(io.BytesIO(fetch(frame["uri"]))).convert("RGBA")
        counts = {level: 0 for level in range(1, 9)}
        wet_bounds = [image.width, image.height, -1, -1]
        for y in range(image.height):
            for x in range(image.width):
                level = intensity(image.getpixel((x, y)))
                if level:
                    counts[level] += 1
                    wet_bounds[0] = min(wet_bounds[0], x)
                    wet_bounds[1] = min(wet_bounds[1], y)
                    wet_bounds[2] = max(wet_bounds[2], x)
                    wet_bounds[3] = max(wet_bounds[3], y)
        frames.append({
            "time": frame["time"],
            "size": [image.width, image.height],
            "wet_pixels": sum(counts.values()),
            "intensity_counts": counts,
            "wet_bounds": wet_bounds if sum(counts.values()) else None,
        })
        timestamp = int(datetime.fromisoformat(frame["time"]).timestamp())
        cells = [
            [y, x, intensity(image.getpixel((x, y)))]
            for y in range(image.height)
            for x in range(image.width)
            if intensity(image.getpixel((x, y)))
        ]
        replay_frames.append({
            "nominal_timestamp": timestamp,
            "frame_json": json.dumps({
                "source": "kmi_raw",
                "nominal_timestamp": timestamp,
                "grid_deg": 6.5 / image.height,
                "grid_lat_deg": 6.5 / image.height,
                "grid_lon_deg": 13.0 / image.width,
                "bounds": [-2.5, 46.5, 10.5, 53.0],
                "cells": cells,
            }, separators=(",", ":")),
        })
    if len(sys.argv) > 1:
        destination = Path(sys.argv[1]).resolve()
        destination.write_text(json.dumps({
            "region_id": "region-1",
            "source": "kmi_raw",
            "frames": replay_frames,
        }), encoding="utf-8")
    print(json.dumps({
        "animation_type": payload.get("animation", {}).get("type"),
        "unit": payload.get("animation", {}).get("unit", {}).get("nl"),
        "hint": payload.get("animation", {}).get(
            "sequenceHint", {}
        ).get("nl"),
        "historical_frame_count": len(historical),
        "replay_destination": str(Path(sys.argv[1]).resolve())
        if len(sys.argv) > 1 else None,
        "frames": frames,
    }, indent=2))


if __name__ == "__main__":
    main()
