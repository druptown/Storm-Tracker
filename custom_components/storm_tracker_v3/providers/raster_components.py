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
    polygons: tuple[
        tuple[tuple[tuple[float, float], ...], ...], ...
    ]

    @property
    def boundary(self) -> tuple[tuple[float, float], ...]:
        """Legacy buitenring; ``polygons`` blijft de autoritatieve topologie."""
        return self.polygons[0][0] if self.polygons else ()


def extract_intensity_runs(
    intensity_grid,
    corner_to_latlon: Callable[[float, float], tuple[float, float]],
    *,
    include_point: Callable[[float, float], bool] | None = None,
    max_run_pixels: int = 16,
) -> list[dict]:
    """Comprimeer natte bronpixels tot korte, exact geprojecteerde rijruns."""
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
            center_lat, center_lon = corner_to_latlon(
                row + 0.5, (col + end) / 2.0
            )
            if include_point is None or include_point(center_lat, center_lon):
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


def _signed_grid_area(ring) -> float:
    return sum(
        col1 * row2 - col2 * row1
        for (row1, col1), (row2, col2) in zip(ring, ring[1:])
    ) / 2.0


def _simplify_grid_ring(ring) -> tuple[tuple[int, int], ...]:
    """Verwijder uitsluitend collineaire tussenpunten en behoud sluiting."""
    simplified = []
    for index, current in enumerate(ring[:-1]):
        previous = ring[index - 1 if index else -2]
        following = ring[(index + 1) % (len(ring) - 1)]
        if (
            previous[0] == current[0] == following[0]
            or previous[1] == current[1] == following[1]
        ):
            continue
        simplified.append(current)
    if len(simplified) < 3:
        simplified = list(ring[:-1])
    simplified.append(simplified[0])
    return tuple(simplified)


def _point_in_grid_ring(
    point: tuple[float, float],
    ring: tuple[tuple[int, int], ...],
) -> bool:
    row, col = point
    inside = False
    previous_row, previous_col = ring[-1]
    for current_row, current_col in ring:
        if (current_row > row) != (previous_row > row):
            crossing_col = (
                (previous_col - current_col)
                * (row - current_row)
                / (previous_row - current_row)
                + current_col
            )
            if col < crossing_col:
                inside = not inside
        previous_row, previous_col = current_row, current_col
    return inside


def _trace_grid_loops(pixels) -> list[tuple[tuple[int, int], ...]]:
    """Traceer iedere buiten- en binnenring met gevulde pixels rechts."""
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
    loops: list[tuple[tuple[int, int], ...]] = []
    direction_index = {
        (0, 1): 0,
        (1, 0): 1,
        (0, -1): 2,
        (-1, 0): 3,
    }
    turn_rank = {1: 0, 0: 1, 3: 2, 2: 3}
    while unused:
        first = min(unused)
        unused.remove(first)
        ring = [first[0], first[1]]
        while ring[-1] != ring[0]:
            candidates = [
                edge for edge in unused if edge[0] == ring[-1]
            ]
            if not candidates:
                ring = []
                break
            previous = (
                ring[-1][0] - ring[-2][0],
                ring[-1][1] - ring[-2][1],
            )
            previous_index = direction_index[previous]

            def candidate_rank(edge):
                following = (
                    edge[1][0] - edge[0][0],
                    edge[1][1] - edge[0][1],
                )
                delta = (
                    direction_index[following] - previous_index
                ) % 4
                return turn_rank[delta], edge

            edge = min(candidates, key=candidate_rank)
            unused.remove(edge)
            ring.append(edge[1])
            if len(ring) > len(edges) + 1:
                ring = []
                break
        if len(ring) >= 4:
            loops.append(_simplify_grid_ring(ring))
    return loops


def _pixel_fallback_polygons(pixels):
    """Exacte, niet-geflattenede fallback wanneer contourtracing faalt."""
    return tuple(
        ((
            (row, col),
            (row, col + 1),
            (row + 1, col + 1),
            (row + 1, col),
            (row, col),
        ),)
        for row, col in sorted(pixels)
    )


def _has_ambiguous_diagonal_contact(pixels) -> bool:
    """Detecteer 8-connected contacten die alleen één rasterhoek delen."""
    occupied = {
        (int(row), int(col)) for row, col in pixels
    }
    for row, col in occupied:
        for row_delta, col_delta in ((1, 1), (1, -1)):
            diagonal = (row + row_delta, col + col_delta)
            if diagonal not in occupied:
                continue
            bridges = (
                (row + row_delta, col),
                (row, col + col_delta),
            )
            if not any(bridge in occupied for bridge in bridges):
                return True
    return False


def _grid_polygons(pixels, *, fail_closed: bool = False):
    """Geef alle buitenringen met hun gaten als immutable MultiPolygon."""
    pixels = tuple((int(row), int(col)) for row, col in pixels)
    if fail_closed and _has_ambiguous_diagonal_contact(pixels):
        return ()
    loops = _trace_grid_loops(pixels)
    signed = [(_signed_grid_area(ring), ring) for ring in loops]
    if (
        not signed
        or abs(sum(area for area, _ in signed) - len(pixels)) > 1e-9
    ):
        return () if fail_closed else _pixel_fallback_polygons(pixels)

    outers = [
        (area, ring) for area, ring in signed if area > 0.0
    ]
    holes = [
        (area, ring) for area, ring in signed if area < 0.0
    ]
    if not outers or (fail_closed and len(outers) != 1):
        return () if fail_closed else _pixel_fallback_polygons(pixels)

    outers.sort(key=lambda item: (min(item[1]), -item[0]))
    polygons: list[list[tuple[tuple[int, int], ...]]] = [
        [ring] for _, ring in outers
    ]
    for _, hole in sorted(holes, key=lambda item: min(item[1])):
        representative = hole[0]
        containers = [
            (outer_area, index)
            for index, (outer_area, outer) in enumerate(outers)
            if _point_in_grid_ring(representative, outer)
        ]
        if not containers:
            return () if fail_closed else _pixel_fallback_polygons(pixels)
        _, container_index = min(containers)
        polygons[container_index].append(hole)
    return tuple(tuple(rings) for rings in polygons)


def grid_boundary_polygons(
    pixels,
    corner_to_latlon: Callable[[float, float], tuple[float, float]],
    *,
    fail_closed: bool = False,
) -> tuple[
    tuple[tuple[tuple[float, float], ...], ...], ...
]:
    """Projecteer alle buitenringen en gaten zonder topologieverlies."""
    return tuple(
        tuple(
            tuple(
                corner_to_latlon(float(row), float(col))
                for row, col in ring
            )
            for ring in polygon
        )
        for polygon in _grid_polygons(
            pixels,
            fail_closed=fail_closed,
        )
    )


def _grid_boundary(pixels) -> list[tuple[int, int]]:
    """Legacy grootste buitenring; nieuwe code gebruikt ``_grid_polygons``."""
    polygons = _grid_polygons(pixels)
    if not polygons:
        return []
    largest = max(
        (polygon[0] for polygon in polygons),
        key=lambda ring: abs(_signed_grid_area(ring)),
    )
    return list(largest)


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
        polygons = grid_boundary_polygons(
            pixels,
            corner_to_latlon,
        )
        components.append(RasterComponent(
            index=index,
            pixels=tuple(pixels),
            centroid_row=sum(row + 0.5 for row, _ in pixels) / len(pixels),
            centroid_col=sum(col + 0.5 for _, col in pixels) / len(pixels),
            max_intensity=max(values),
            mean_intensity=sum(values) / len(values),
            polygons=polygons,
        ))
    return components
