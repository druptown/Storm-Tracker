"""Brononafhankelijke componenten en buitenranden voor radarrasters."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True, slots=True)
class RasterComponent:
    """Eén 4-connected neerslaggebied uit een bronraster."""

    index: int
    pixels: tuple[tuple[int, int], ...]
    centroid_row: float
    centroid_col: float
    max_intensity: int
    mean_intensity: float
    boundary: tuple[tuple[float, float], ...]


def _pixel_components(values) -> list[list[tuple[int, int]]]:
    """Label natte pixels 4-connected, zonder scipy-afhankelijkheid."""
    import numpy as np

    remaining = {
        (int(row), int(col)) for row, col in np.argwhere(values > 0)
    }
    components = []
    while remaining:
        start = min(remaining)
        remaining.remove(start)
        queue = deque((start,))
        pixels = []
        while queue:
            current_row, current_col = queue.popleft()
            pixels.append((current_row, current_col))
            for neighbour in (
                (current_row - 1, current_col),
                (current_row + 1, current_col),
                (current_row, current_col - 1),
                (current_row, current_col + 1),
            ):
                if neighbour in remaining:
                    remaining.remove(neighbour)
                    queue.append(neighbour)
        components.append(pixels)
    return components


def _grid_boundary(pixels) -> list[tuple[int, int]]:
    """Traceer de grootste gesloten buitenring langs echte pixelranden."""
    edges = set()
    for row, col in pixels:
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
            if len(ring) > len(edges) + 1:
                ring = []
                break
        if len(ring) >= 4:
            loops.append(ring)
    if not loops:
        return []

    def area(ring):
        return abs(sum(
            col1 * row2 - col2 * row1
            for (row1, col1), (row2, col2) in zip(ring, ring[1:])
        )) / 2.0

    ring = max(loops, key=area)
    # Rechte tussenpunten dragen geen vorminformatie en belasten GeoJSON alleen.
    simplified = []
    for index, current in enumerate(ring[:-1]):
        previous = ring[index - 1 if index else -2]
        following = ring[(index + 1) % (len(ring) - 1)]
        if ((previous[0] == current[0] == following[0]) or
                (previous[1] == current[1] == following[1])):
            continue
        simplified.append(current)
    if len(simplified) < 3:
        simplified = ring[:-1]
    simplified.append(simplified[0])
    return simplified


def extract_components(
    intensity_grid,
    corner_to_latlon: Callable[[float, float], tuple[float, float]],
    *,
    minimum_pixels: int = 1,
) -> list[RasterComponent]:
    """Maak geografische clusters uit een volledig intensiteitsraster."""
    components = []
    raw_components = sorted(
        _pixel_components(intensity_grid),
        key=lambda pixels: min(pixels),
    )
    for index, pixels in enumerate(raw_components):
        if len(pixels) < minimum_pixels:
            continue
        values = [int(intensity_grid[row, col]) for row, col in pixels]
        boundary = tuple(
            corner_to_latlon(float(row), float(col))
            for row, col in _grid_boundary(pixels)
        )
        components.append(RasterComponent(
            index=index,
            pixels=tuple(pixels),
            centroid_row=sum(row + 0.5 for row, _ in pixels) / len(pixels),
            centroid_col=sum(col + 0.5 for _, col in pixels) / len(pixels),
            max_intensity=max(values),
            mean_intensity=sum(values) / len(values),
            boundary=boundary,
        ))
    return components
