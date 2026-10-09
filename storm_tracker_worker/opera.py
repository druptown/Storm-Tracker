"""OPERA HDF5 download and parser running outside Home Assistant Core."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import io
import json

import h5py
import numpy as np
from pyproj import CRS, Transformer

from .artifacts import write_json_artifact
from .radar import _download_bounded, _grid_polygons, _text

MIN_DBZ = 8.0
MIN_PIXELS = 5
MAX_UNSPLIT_CELL_PIXELS = 3_000
SPLIT_CORE_THRESHOLDS_DBZ = (
    12.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0,
)
SPLIT_GROWTH_PIXELS = 5
MAX_OPERA_POLYGONS_PER_FOOTPRINT = 1
MAX_OPERA_RINGS_PER_FOOTPRINT = 256
MAX_OPERA_POINTS_PER_RING = 100_000
MAX_OPERA_TOPOLOGY_POINTS_PER_ARTIFACT = 1_000_000


@dataclass(frozen=True)
class Grid:
    projdef: str
    xsize: int
    ysize: int
    xscale: float
    yscale: float


def _edge_points(bbox: tuple, samples: int = 33):
    min_lon, min_lat, max_lon, max_lat = bbox
    lon = np.linspace(min_lon, max_lon, samples)
    lat = np.linspace(min_lat, max_lat, samples)
    return (
        np.concatenate(
            (lon, lon, np.full(samples, min_lon), np.full(samples, max_lon))
        ),
        np.concatenate(
            (np.full(samples, min_lat), np.full(samples, max_lat), lat, lat)
        ),
    )


def _crop_window(grid: Grid, bbox: tuple) -> tuple[int, int, int, int]:
    forward = Transformer.from_crs(
        CRS.from_epsg(4326), CRS.from_user_input(grid.projdef), always_xy=True
    )
    lons, lats = _edge_points(bbox)
    xs, ys = forward.transform(lons, lats)
    col0 = max(0, int(np.floor(np.min(xs) / grid.xscale)))
    col1 = min(grid.xsize, int(np.ceil(np.max(xs) / grid.xscale)))
    row0 = max(0, int(np.floor(-np.max(ys) / grid.yscale)))
    row1 = min(grid.ysize, int(np.ceil(-np.min(ys) / grid.yscale)))
    if row0 >= row1 or col0 >= col1:
        raise ValueError("BBox valt buiten het OPERA-raster")
    return row0, row1, col0, col1


def _neighbors(row: int, col: int, height: int, width: int):
    for dr, dc in (
        (-1, 0), (1, 0), (0, -1), (0, 1),
        (-1, -1), (-1, 1), (1, -1), (1, 1),
    ):
        rr, cc = row + dr, col + dc
        if 0 <= rr < height and 0 <= cc < width:
            yield rr, cc


def _label_components(mask, min_pixels: int) -> list:
    height, width = mask.shape
    visited = np.zeros(mask.shape, dtype=np.bool_)
    components = []
    for start_row, start_col in np.argwhere(mask):
        sr, sc = int(start_row), int(start_col)
        if visited[sr, sc]:
            continue
        visited[sr, sc] = True
        pending = deque(((sr, sc),))
        pixels = []
        while pending:
            row, col = pending.popleft()
            pixels.append((row, col))
            for rr, cc in _neighbors(row, col, height, width):
                if mask[rr, cc] and not visited[rr, cc]:
                    visited[rr, cc] = True
                    pending.append((rr, cc))
        if len(pixels) >= min_pixels:
            components.append(np.asarray(pixels, dtype=np.int32))
    return components


def _grow_seed_components(support_mask, seeds: list, max_steps: int) -> list:
    height, width = support_mask.shape
    owner = np.full(support_mask.shape, -1, dtype=np.int32)
    distance = np.full(support_mask.shape, max_steps + 1, dtype=np.int16)
    pending = deque()
    for seed_id, seed in enumerate(seeds):
        for row, col in seed:
            rr, cc = int(row), int(col)
            owner[rr, cc] = seed_id
            distance[rr, cc] = 0
            pending.append((rr, cc))
    while pending:
        row, col = pending.popleft()
        current_distance = int(distance[row, col])
        if current_distance >= max_steps:
            continue
        for rr, cc in _neighbors(row, col, height, width):
            if not support_mask[rr, cc] or owner[rr, cc] >= 0:
                continue
            owner[rr, cc] = owner[row, col]
            distance[rr, cc] = current_distance + 1
            pending.append((rr, cc))
    owned_pixels = np.argwhere(owner >= 0).astype(np.int32)
    if not len(owned_pixels):
        return []
    owned_ids = owner[owned_pixels[:, 0], owned_pixels[:, 1]]
    return [
        owned_pixels[owned_ids == seed_id]
        for seed_id in range(len(seeds))
        if np.count_nonzero(owned_ids == seed_id) >= MIN_PIXELS
    ]


def _component_groups(mask, radar) -> list[tuple[object, list]]:
    groups = []
    for component in _label_components(mask, MIN_PIXELS):
        if len(component) <= MAX_UNSPLIT_CELL_PIXELS:
            groups.append((component, [component]))
            continue
        component_mask = np.zeros(mask.shape, dtype=np.bool_)
        component_mask[component[:, 0], component[:, 1]] = True
        seeds = []
        for threshold in SPLIT_CORE_THRESHOLDS_DBZ:
            candidates = _label_components(
                component_mask & (radar >= threshold), MIN_PIXELS
            )
            if candidates and max(map(len, candidates)) <= MAX_UNSPLIT_CELL_PIXELS:
                seeds = candidates
                break
        if not seeds:
            groups.append((component, [component]))
            continue
        groups.append(
            (
                component,
                _grow_seed_components(
                    component_mask, seeds, SPLIT_GROWTH_PIXELS
                ) or [component],
            )
        )
    return groups


def _boundary_ring(pixels, window, grid: Grid, inverse) -> tuple:
    row0, _, col0, _ = window
    edges = set()
    for local_row, local_col in pixels:
        row, col = int(local_row), int(local_col)
        for edge in (
            ((row, col), (row, col + 1)),
            ((row, col + 1), (row + 1, col + 1)),
            ((row + 1, col + 1), (row + 1, col)),
            ((row + 1, col), (row, col)),
        ):
            reverse = (edge[1], edge[0])
            if reverse in edges:
                edges.remove(reverse)
            else:
                edges.add(edge)
    unused = set(edges)
    loops = []
    while unused:
        first = min(unused)
        unused.remove(first)
        ring = [first[0], first[1]]
        while ring[-1] != ring[0]:
            candidates = sorted(edge for edge in unused if edge[0] == ring[-1])
            if not candidates:
                ring = []
                break
            edge = candidates[0]
            unused.remove(edge)
            ring.append(edge[1])
        if len(ring) >= 4:
            loops.append(ring)
    if len(loops) != 1:
        return ()
    projected = [
        ((col0 + col) * grid.xscale, -(row0 + row) * grid.yscale)
        for row, col in loops[0]
    ]
    area = abs(
        sum(
            x1 * y2 - x2 * y1
            for (x1, y1), (x2, y2) in zip(projected, projected[1:])
        )
    ) / 2.0
    expected = len(pixels) * grid.xscale * grid.yscale
    if expected <= 0 or not 0.98 <= area / expected <= 1.02:
        return ()
    xs, ys = zip(*projected)
    lons, lats = inverse.transform(xs, ys)
    return tuple(
        (round(float(lat), 5), round(float(lon), 5))
        for lat, lon in zip(lats, lons)
    )


def _boundary_polygons(pixels, window, grid: Grid, inverse) -> tuple:
    """Project every unambiguous outer/hole ring without flattening."""
    row0, _, col0, _ = window
    grid_polygons = _grid_polygons(pixels, fail_closed=True)
    if len(grid_polygons) > MAX_OPERA_POLYGONS_PER_FOOTPRINT:
        return ()
    if (
        sum(len(polygon) for polygon in grid_polygons)
        > MAX_OPERA_RINGS_PER_FOOTPRINT
    ):
        return ()
    projected_polygons = []
    for polygon in grid_polygons:
        projected_rings = []
        for ring in polygon:
            if len(ring) > MAX_OPERA_POINTS_PER_RING:
                return ()
            projected = [
                (
                    (col0 + col) * grid.xscale,
                    -(row0 + row) * grid.yscale,
                )
                for row, col in ring
            ]
            xs, ys = zip(*projected)
            lons, lats = inverse.transform(xs, ys)
            projected_rings.append(tuple(
                (round(float(lat), 5), round(float(lon), 5))
                for lat, lon in zip(lats, lons)
            ))
        projected_polygons.append(tuple(projected_rings))
    return tuple(projected_polygons)


def _cell(pixels, radar, quality, window, grid: Grid) -> dict:
    row0, _, col0, _ = window
    inverse = Transformer.from_crs(
        CRS.from_user_input(grid.projdef), CRS.from_epsg(4326), always_xy=True
    )
    rows, cols = pixels[:, 0], pixels[:, 1]
    global_row = row0 + float(rows.mean())
    global_col = col0 + float(cols.mean())
    lon, lat = inverse.transform(
        (global_col + 0.5) * grid.xscale,
        -(global_row + 0.5) * grid.yscale,
    )
    values = radar[rows, cols]
    qualities = quality[rows, cols]
    footprint_polygons = _boundary_polygons(
        pixels,
        window,
        grid,
        inverse,
    )
    return {
        "centroid_lat": round(float(lat), 5),
        "centroid_lon": round(float(lon), 5),
        "area_km2": round(
            len(pixels) * grid.xscale * grid.yscale / 1_000_000, 3
        ),
        "max_dbz": round(float(values.max()), 2),
        "mean_dbz": round(float(values.mean()), 2),
        "mean_quality": round(float(qualities.mean()), 3),
        "pixelcount": len(pixels),
        "footprint_points": _boundary_ring(
            pixels, window, grid, inverse
        ),
        "footprint_polygons": footprint_polygons,
    }


def _intensity_runs(intensity_grid, corner_to_latlon, max_run_pixels: int = 16):
    """Compact wet pixels into bounded geographic row runs."""
    height, width = intensity_grid.shape
    runs = []
    for row in range(height):
        col = 0
        while col < width:
            intensity = int(intensity_grid[row, col])
            if intensity <= 0:
                col += 1
                continue
            end = col + 1
            limit = min(width, col + max_run_pixels)
            while end < limit and int(intensity_grid[row, end]) == intensity:
                end += 1
            runs.append({
                "intensity": intensity,
                "ring": (
                    corner_to_latlon(row, col),
                    corner_to_latlon(row, end),
                    corner_to_latlon(row + 1, end),
                    corner_to_latlon(row + 1, col),
                ),
            })
            col = end
    return runs


def parse_opera_slice(payload: bytes, bbox: tuple) -> dict:
    """Apply the same cell segmentation as the in-Core OPERA parser."""
    with h5py.File(io.BytesIO(payload), "r") as dataset:
        where = dataset["where"].attrs
        grid = Grid(
            _text(where["projdef"]),
            int(where["xsize"]),
            int(where["ysize"]),
            float(where["xscale"]),
            float(where["yscale"]),
        )
        window = _crop_window(grid, bbox)
        row0, row1, col0, col1 = window
        raw_radar = dataset["dataset1/data1/data"][row0:row1, col0:col1]
        quality_group = dataset["dataset1/data1/quality1"]
        quality = quality_group["data"][row0:row1, col0:col1]
        attrs = dataset["dataset1/data1/what"].attrs
        nodata = float(attrs["nodata"])
        undetect = float(attrs["undetect"])
        gain = float(attrs["gain"])
        offset = float(attrs["offset"])
        radar = raw_radar * gain + offset
        quality_what = quality_group.get("what")
        quality_nodata = float(
            quality_what.attrs.get("nodata", -9999000.0)
            if quality_what is not None else -9999000.0
        )
        valid = (
            np.isfinite(radar)
            & np.isfinite(quality)
            & (radar != nodata * gain + offset)
            & (radar != undetect * gain + offset)
            & (quality != quality_nodata)
        )
        mask = valid & (radar >= MIN_DBZ)
        inverse = Transformer.from_crs(
            CRS.from_user_input(grid.projdef), CRS.from_epsg(4326),
            always_xy=True,
        )

        def corner_to_latlon(row, column):
            lon, lat = inverse.transform(
                (col0 + column) * grid.xscale,
                -(row0 + row) * grid.yscale,
            )
            return round(float(lat), 5), round(float(lon), 5)

        intensity_grid = np.zeros(radar.shape, dtype=np.uint8)
        intensity_grid[mask & (radar < 15)] = 1
        intensity_grid[mask & (radar >= 15) & (radar < 20)] = 2
        intensity_grid[mask & (radar >= 20) & (radar < 25)] = 3
        intensity_grid[mask & (radar >= 25) & (radar < 30)] = 4
        intensity_grid[mask & (radar >= 30) & (radar < 35)] = 5
        intensity_grid[mask & (radar >= 35) & (radar < 40)] = 6
        intensity_grid[mask & (radar >= 40) & (radar < 50)] = 7
        intensity_grid[mask & (radar >= 50)] = 8
        groups = _component_groups(mask, radar)
        cells = []
        for parent_index, (parent_pixels, child_pixels) in enumerate(groups):
            parent = _cell(
                parent_pixels, radar, quality, window, grid
            )
            for child_index, pixels in enumerate(child_pixels):
                child = _cell(pixels, radar, quality, window, grid)
                child.update(
                    {
                        "parent_component": parent_index,
                        "child_component": child_index,
                        "parent_area_km2": parent["area_km2"],
                        "parent_footprint_points": parent[
                            "footprint_points"
                        ],
                        "parent_footprint_polygons": parent[
                            "footprint_polygons"
                        ],
                    }
                )
                cells.append(child)
        cells.sort(key=lambda item: item["area_km2"], reverse=True)
        date = _text(dataset["what"].attrs.get("date", ""))
        time_value = _text(dataset["what"].attrs.get("time", ""))
        timestamp_text = (
            f"{date}T{time_value}Z" if date and time_value else ""
        )
        timestamp = (
            datetime.fromisoformat(
                timestamp_text.replace("Z", "+00:00")
            ).timestamp()
            if timestamp_text
            else datetime.now(timezone.utc).timestamp()
        )
    return {
        "provider": "opera",
        "frame_timestamp": timestamp,
        "timestamp_text": timestamp_text,
        "cells": cells,
        "overlay": {
            "source": "opera",
            "timestamp": timestamp,
            "runs": _intensity_runs(intensity_grid, corner_to_latlon),
        },
    }


def _topology_stats(cells: list[dict]) -> tuple[int, int]:
    ring_count = sum(
        len(polygon)
        for cell in cells
        for field_name in (
            "footprint_polygons",
            "parent_footprint_polygons",
        )
        for polygon in cell.get(field_name, ())
    )
    point_count = sum(
        len(ring)
        for cell in cells
        for field_name in (
            "footprint_polygons",
            "parent_footprint_polygons",
        )
        for polygon in cell.get(field_name, ())
        for ring in polygon
    )
    if point_count > MAX_OPERA_TOPOLOGY_POINTS_PER_ARTIFACT:
        raise ValueError("OPERA-topologie overschrijdt veiligheidslimiet")
    return ring_count, point_count


def opera_hdf5_fetch(job) -> dict:
    """Download and fully parse one OPERA slice outside Home Assistant."""
    payload = job.payload
    result = parse_opera_slice(
        _download_bounded(
            str(payload["url"]), int(payload.get("max_file_bytes", 40 * 1024 * 1024))
        ),
        tuple(float(value) for value in payload["bbox"]),
    )
    cell_digest = hashlib.sha256(
        json.dumps(
            result["cells"], ensure_ascii=False, separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    topology_ring_count, topology_point_count = _topology_stats(
        result["cells"]
    )
    artifact = write_json_artifact(job.job_id, result)
    return {
        **artifact,
        "provider": "opera",
        "frame_timestamp": result["frame_timestamp"],
        "cell_count": len(result["cells"]),
        "cell_digest": cell_digest,
        "topology_ring_count": topology_ring_count,
        "topology_point_count": topology_point_count,
    }


# Compatibility for a rolling integration/worker upgrade.
opera_hdf5_shadow = opera_hdf5_fetch
