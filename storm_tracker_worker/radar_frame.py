"""Bounded regional radar-frame and display-layer construction."""
from __future__ import annotations

import hashlib
import json
from math import ceil, isfinite
from pathlib import Path
from typing import Any

import numpy as np

from .artifacts import (
    ARTIFACT_ROOT,
    MAX_ARTIFACT_BYTES,
    read_json_artifact,
    write_json_artifact,
)
from .tracking_core.engine.bounded_payload import (
    decompress_limited,
    read_limited_stream,
)

HIGH_RES_GRID_DEG = 0.005
COMPOSITE_GRID_DEG = 0.01
SATELLITE_GRID_DEG = 0.02
MAX_GRID_CELLS = 640_000
MAX_INPUT_RUNS = 25_000
MAX_OUTPUT_RUNS = 24_000

HIGH_RES_SOURCES = frozenset({
    "kmi", "knmi", "dwd_radolan", "met_office_radar",
    "meteofrance_radar", "meteolux", "dpc_radar", "aemet_radar",
})
SATELLITE_SOURCES = frozenset({
    "noaa_enterprise_rainrate", "noaa_goes_rrqpe", "hsaf_h40b",
})


def _read_input(metadata: dict[str, Any]) -> dict[str, Any]:
    raw_path = metadata.get("artifact_path")
    if not isinstance(raw_path, str):
        raise ValueError("Rastertaak bevat geen invoerartifact")
    root = ARTIFACT_ROOT.resolve()
    path = Path(raw_path).resolve()
    if root not in path.parents:
        raise ValueError("Rasterinvoer staat buiten de gedeelde map")
    try:
        expected_bytes = int(metadata.get("artifact_bytes", -1))
    except (TypeError, ValueError) as err:
        raise ValueError("Rasterinvoer heeft een ongeldige grootte") from err
    if (
        expected_bytes <= 0
        or expected_bytes > MAX_ARTIFACT_BYTES
        or path.stat().st_size != expected_bytes
    ):
        raise ValueError("Rasterinvoer faalt grootte- of hashcontrole")
    with path.open("rb") as stream:
        compressed = read_limited_stream(
            stream,
            maximum_bytes=MAX_ARTIFACT_BYTES,
            declared_bytes=expected_bytes,
            require_declared_match=True,
        )
    if (
        hashlib.sha256(compressed).hexdigest()
        != metadata.get("artifact_sha256")
    ):
        raise ValueError("Rasterinvoer faalt grootte- of hashcontrole")
    decoded = decompress_limited(
        compressed,
        codec="gzip",
        maximum_compressed_bytes=MAX_ARTIFACT_BYTES,
        maximum_uncompressed_bytes=MAX_ARTIFACT_BYTES,
    )
    value = json.loads(decoded)
    if not isinstance(value, dict) or value.get("artifact_type") != "radar_overlays":
        raise ValueError("Rasterinvoer heeft een ongeldig type")
    return value


def _source_grid_deg(source: str) -> float:
    """Gebruik brongetrouw detail zonder satellietdata kunstmatig op te blazen."""
    if source in HIGH_RES_SOURCES:
        return HIGH_RES_GRID_DEG
    if source in SATELLITE_SOURCES:
        return SATELLITE_GRID_DEG
    return COMPOSITE_GRID_DEG


def _shape(
    bounds: tuple[float, float, float, float],
    source: str,
) -> tuple[float, int, int]:
    west, south, east, north = bounds
    if not (west < east and south < north):
        raise ValueError("Ongeldige regiogrenzen")
    grid_deg = _source_grid_deg(source)
    height = ceil((north - south) / grid_deg - 1e-10)
    width = ceil((east - west) / grid_deg - 1e-10)
    if height * width > MAX_GRID_CELLS:
        while height * width > MAX_GRID_CELLS:
            grid_deg *= (
                (height * width / MAX_GRID_CELLS) ** 0.5
            ) * 1.000001
            height = ceil((north - south) / grid_deg - 1e-10)
            width = ceil((east - west) / grid_deg - 1e-10)
    return grid_deg, height, width


def _normalise(overlay: dict[str, Any], bounds: tuple[float, ...]) -> dict | None:
    source = overlay.get("source")
    try:
        timestamp = float(overlay.get("timestamp"))
    except (TypeError, ValueError):
        return None
    if not isinstance(source, str) or not source or not isfinite(timestamp):
        return None
    runs = overlay.get("runs", ())
    if not isinstance(runs, (list, tuple)) or len(runs) > MAX_INPUT_RUNS:
        raise ValueError("Te veel of ongeldige bronruns")
    west, south, east, north = bounds
    grid_deg, height, width = _shape(bounds, source)
    raster = np.zeros((height, width), dtype=np.uint8)
    valid_runs = 0
    for run in runs:
        try:
            intensity = min(255, int(run["intensity"]))
            ring = tuple(run["ring"])
            latitudes = [float(point[0]) for point in ring]
            longitudes = [float(point[1]) for point in ring]
        except (KeyError, TypeError, ValueError, IndexError):
            continue
        if len(ring) < 4 or intensity <= 0:
            continue
        run_west, run_east = max(west, min(longitudes)), min(east, max(longitudes))
        run_south, run_north = max(south, min(latitudes)), min(north, max(latitudes))
        if run_west >= run_east or run_south >= run_north:
            continue
        valid_runs += 1
        first_col = max(0, int((run_west - west) / grid_deg))
        last_col = min(width - 1, int((run_east - west) / grid_deg))
        first_row = max(0, int((north - run_north) / grid_deg))
        last_row = min(height - 1, int((north - run_south) / grid_deg))
        view = raster[first_row:last_row + 1, first_col:last_col + 1]
        np.maximum(view, intensity, out=view)

    rows, columns = np.nonzero(raster)
    values = raster[rows, columns]
    cells = [
        [int(row), int(column), int(value)]
        for row, column, value in zip(rows, columns, values)
    ]
    # Compacte kaartlaag: aaneengesloten cellen met dezelfde intensiteit
    # worden per rij één rechthoek. Hierdoor krijgt HA nooit het ruwe product.
    display_runs = []
    for row in range(height):
        active = np.flatnonzero(raster[row])
        if not active.size:
            continue
        start = previous = int(active[0])
        intensity = int(raster[row, start])
        for raw_column in active[1:].tolist() + [None]:
            column = int(raw_column) if raw_column is not None else None
            if (
                column is not None
                and column == previous + 1
                and int(raster[row, column]) == intensity
            ):
                previous = column
                continue
            run_west = west + start * grid_deg
            run_east = min(east, west + (previous + 1) * grid_deg)
            run_north = north - row * grid_deg
            run_south = max(south, north - (row + 1) * grid_deg)
            display_runs.append({
                "intensity": intensity,
                "ring": [
                    [run_south, run_west], [run_south, run_east],
                    [run_north, run_east], [run_north, run_west],
                    [run_south, run_west],
                ],
            })
            if len(display_runs) >= MAX_OUTPUT_RUNS:
                break
            if column is not None:
                start = previous = column
                intensity = int(raster[row, column])
        if len(display_runs) >= MAX_OUTPUT_RUNS:
            break
    return {
        "frame": {
            "source": source,
            "nominal_timestamp": int(timestamp // 60) * 60,
            "bounds": list(bounds),
            "grid_deg": grid_deg,
            "height": height,
            "width": width,
            "cells": cells,
            "input_runs": valid_runs,
        },
        "overlay": {
            "source": source,
            "timestamp": timestamp,
            "runs": display_runs,
            "worker_bounded": True,
            "truncated": len(display_runs) >= MAX_OUTPUT_RUNS,
        },
    }


def radar_frame_operational(job) -> dict[str, Any]:
    """Build all regional frames once and return only artifact metadata."""
    value = _read_input(job.payload)
    overlays = value.get("overlays")
    regions = value.get("regions")
    if not isinstance(overlays, dict) or not isinstance(regions, dict):
        raise ValueError("Rasterinvoer mist regio's of overlays")
    results = {}
    for engine_id, overlay in overlays.items():
        bounds = tuple(float(item) for item in regions.get(engine_id, ()))
        if len(bounds) != 4 or not isinstance(overlay, dict):
            continue
        normalised = _normalise(overlay, bounds)
        if normalised is not None:
            results[str(engine_id)] = normalised
    metadata = write_json_artifact(
        job.job_id,
        {"artifact_type": "regional_radar_frames", "regions": results},
    )
    return {
        **metadata,
        "region_count": len(results),
        "wet_cells": sum(
            len(result["frame"]["cells"]) for result in results.values()
        ),
    }


def radar_frame_from_provider_artifacts(job) -> dict[str, Any]:
    """Normalize selected provider overlays without a HA round-trip copy."""
    selections = job.payload.get("selections")
    if not isinstance(selections, dict) or len(selections) > 16:
        raise ValueError("ongeldige providerartefactselecties")
    artifact_cache = {}
    results = {}
    for engine_id, selection in selections.items():
        if not isinstance(selection, dict):
            continue
        metadata = selection.get("artifact")
        bounds = tuple(float(value) for value in selection.get("bounds", ()))
        provider = str(selection.get("provider") or "")
        if not isinstance(metadata, dict) or len(bounds) != 4 or not provider:
            continue
        cache_key = (
            metadata.get("artifact_path"), metadata.get("artifact_sha256"),
            metadata.get("artifact_bytes"),
        )
        value = artifact_cache.get(cache_key)
        if value is None:
            value = read_json_artifact(metadata)
            artifact_cache[cache_key] = value
        if value.get("provider") != provider:
            raise ValueError("providerartefact en bronselectie verschillen")
        overlay = value.get("overlay")
        if provider == "noaa_goes_rrqpe":
            satellite_overlays = value.get("overlays_by_satellite")
            if not isinstance(satellite_overlays, dict):
                continue
            center_lon = (bounds[0] + bounds[2]) / 2.0
            normalized_lon = ((center_lon + 180.0) % 360.0) - 180.0
            satellite = (
                "19" if -105.0 <= normalized_lon <= -15.0
                else "18" if normalized_lon < -105.0 or normalized_lon >= 155.0
                else None
            )
            if satellite is None:
                continue
            overlay = satellite_overlays.get(satellite)
            if (
                not isinstance(overlay, dict)
                or overlay.get("source") != provider
            ):
                continue
        if not isinstance(overlay, dict):
            continue
        normalised = _normalise(overlay, bounds)
        if normalised is not None:
            results[str(engine_id)] = normalised
    metadata = write_json_artifact(
        job.job_id,
        {"artifact_type": "regional_radar_frames", "regions": results},
    )
    return {
        **metadata,
        "region_count": len(results),
        "wet_cells": sum(len(value["frame"]["cells"]) for value in results.values()),
        "source_artifacts": len(artifact_cache),
    }
