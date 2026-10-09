"""Veilig invoercontract voor toekomstige raster-nowcasting.

Dit module raakt nooit de StormEngine, OFE of waarschuwingen.  Het zet enkel
de geprojecteerde overlay-runs van een gekozen radarprovider om naar een klein,
regelmatig en begrensd raster per RegionEngine.  Een latere pySTEPS-worker kan
het raster zonder Home Assistant-afhankelijkheid naar een ``numpy``-array
uitpakken.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from math import ceil, isfinite
from typing import Any


DEFAULT_GRID_DEG = 0.02
MAX_GRID_CELLS = 160_000
MAX_HISTORY_FRAMES = 6


@dataclass(frozen=True, slots=True)
class RadarFrame:
    """Eén temporeel uitgelijnd, schaars regelmatig neerslagraster."""

    source: str
    nominal_timestamp: int
    bounds: tuple[float, float, float, float]  # west, zuid, oost, noord
    grid_deg: float
    height: int
    width: int
    cells: tuple[tuple[int, int, int], ...]
    input_runs: int

    @property
    def wet_cells(self) -> int:
        return len(self.cells)

    def to_numpy(self):
        """Materialiseer pas in een worker een dense ``uint8``-array."""
        import numpy as np

        values = np.zeros((self.height, self.width), dtype=np.uint8)
        for row, column, intensity in self.cells:
            values[row, column] = intensity
        return values


def _grid_shape(bounds: tuple[float, float, float, float]) -> tuple[float, int, int]:
    west, south, east, north = bounds
    if not (west < east and south < north):
        raise ValueError("ongeldige regiobounds voor radarraster")
    grid_deg = DEFAULT_GRID_DEG
    # De kleine epsilon voorkomt dat 0.2 / 0.02 als 10.000000000000002
    # een spookrij oplevert op de grens van een regio.
    height = ceil((north - south) / grid_deg - 1e-10)
    width = ceil((east - west) / grid_deg - 1e-10)
    if height * width > MAX_GRID_CELLS:
        grid_deg *= (height * width / MAX_GRID_CELLS) ** 0.5
        height = ceil((north - south) / grid_deg - 1e-10)
        width = ceil((east - west) / grid_deg - 1e-10)
    return grid_deg, height, width


def frame_from_overlay(
    overlay: dict[str, Any],
    bounds: tuple[float, float, float, float],
) -> RadarFrame | None:
    """Normaliseer een provideroverlay naar een vast regionaal raster.

    Alleen geldige vierhoekige, georeferentieerde runs tellen mee.  Elke
    doelcel krijgt de hoogste intensiteit van alle bronruns die hem raken; dit
    behoudt zware kernen bij een resolutiewissel en maakt het resultaat
    onafhankelijk van de oorspronkelijke pixelgrootte.
    """
    source = overlay.get("source")
    timestamp = overlay.get("timestamp")
    if not isinstance(source, str) or not source.strip():
        return None
    try:
        timestamp = float(timestamp)
    except (TypeError, ValueError):
        return None
    if not isfinite(timestamp) or timestamp <= 0:
        return None

    grid_deg, height, width = _grid_shape(tuple(float(value) for value in bounds))
    west, south, east, north = bounds
    raster: dict[tuple[int, int], int] = {}
    valid_runs = 0
    for run in overlay.get("runs", ()):
        try:
            intensity = int(run["intensity"])
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
        value = min(255, intensity)
        for row in range(first_row, last_row + 1):
            for column in range(first_col, last_col + 1):
                key = (row, column)
                raster[key] = max(value, raster.get(key, 0))

    return RadarFrame(
        source=source,
        nominal_timestamp=int(timestamp // 60) * 60,
        bounds=(west, south, east, north),
        grid_deg=grid_deg,
        height=height,
        width=width,
        cells=tuple((row, column, value) for (row, column), value in sorted(raster.items())),
        input_runs=valid_runs,
    )


class RadarFrameHistory:
    """Begrensde, per-regio buffer met exact uitgelijnde minutenframes."""

    def __init__(self, max_frames: int = MAX_HISTORY_FRAMES) -> None:
        self._max_frames = max(3, min(MAX_HISTORY_FRAMES, int(max_frames)))
        self._frames: dict[str, deque[RadarFrame]] = {}

    def add_overlay(
        self,
        region_id: str,
        overlay: dict[str, Any],
        bounds: tuple[float, float, float, float],
    ) -> RadarFrame | None:
        frame = frame_from_overlay(overlay, bounds)
        if frame is None:
            return None
        frames = self._frames.setdefault(region_id, deque(maxlen=self._max_frames))
        for index, existing in enumerate(frames):
            if existing.nominal_timestamp == frame.nominal_timestamp:
                frames[index] = frame
                break
        else:
            frames.append(frame)
        self._frames[region_id] = deque(
            sorted(frames, key=lambda item: item.nominal_timestamp),
            maxlen=self._max_frames,
        )
        return frame

    def frames(self, region_id: str) -> tuple[RadarFrame, ...]:
        """Geef een onveranderlijke momentopname voor een externe worker."""
        return tuple(self._frames.get(region_id, ()))

    def diagnostics(self) -> dict[str, Any]:
        regions = {}
        for region_id, frames in self._frames.items():
            latest = frames[-1] if frames else None
            regions[region_id] = {
                "frames": len(frames),
                "source": latest.source if latest else None,
                "latest_nominal_timestamp": latest.nominal_timestamp if latest else None,
                "grid_deg": latest.grid_deg if latest else None,
                "shape": [latest.height, latest.width] if latest else None,
                "wet_cells": latest.wet_cells if latest else 0,
                "input_runs": latest.input_runs if latest else 0,
            }
        return {
            "status": "collecting",
            "contract": "sparse_regular_grid_v1",
            "max_frames": self._max_frames,
            "regions": regions,
            "feeds_detection": False,
        }
