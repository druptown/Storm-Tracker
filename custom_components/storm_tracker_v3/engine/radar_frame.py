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
import time


DEFAULT_GRID_DEG = 0.02
MAX_GRID_CELLS = 160_000
MAX_HISTORY_FRAMES = 6
RECOVERY_OBJECT_MAX_GAP_SECONDS = 15 * 60


def audit_storm_object_recovery(
    frames: tuple["RadarFrame", ...],
    storms,
    *,
    max_gap_seconds: int = RECOVERY_OBJECT_MAX_GAP_SECONDS,
) -> dict[str, Any]:
    """Controleer of een recente rastercache een hersteld stormobject heeft."""
    if not frames:
        return {
            "status": "geen_recente_radarcache",
            "latest_cached_frame": None,
            "matched_storm_ids": [],
        }
    latest_cached = float(frames[-1].nominal_timestamp)
    matched = []
    for storm in storms:
        latest_storm = getattr(storm, "last_radar_timestamp", None)
        if latest_storm is None:
            continue
        if abs(float(latest_storm) - latest_cached) <= max_gap_seconds:
            matched.append(str(storm.storm_id))
    return {
        "status": "hersteld" if matched else "ontbrekend_stormobject",
        "latest_cached_frame": latest_cached,
        "matched_storm_ids": sorted(matched),
    }


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
        if self.cells:
            cells = np.asarray(self.cells, dtype=np.int32)
            values[cells[:, 0], cells[:, 1]] = cells[:, 2]
        return values

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "nominal_timestamp": self.nominal_timestamp,
            "bounds": list(self.bounds),
            "grid_deg": self.grid_deg,
            "height": self.height,
            "width": self.width,
            "cells": [list(cell) for cell in self.cells],
            "input_runs": self.input_runs,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "RadarFrame":
        return cls(
            source=str(value["source"]),
            nominal_timestamp=int(value["nominal_timestamp"]),
            bounds=tuple(float(item) for item in value["bounds"]),
            grid_deg=float(value["grid_deg"]),
            height=int(value["height"]),
            width=int(value["width"]),
            cells=tuple(
                (int(row), int(column), int(intensity))
                for row, column, intensity in value["cells"]
            ),
            input_runs=int(value.get("input_runs", 0)),
        )


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
    # Een Python-dict per doelcel houdt de GIL vast en kan bij een vol
    # RainViewer-frame Home Assistant tientallen seconden vertragen, zelfs
    # wanneer dit werk in een executor-thread draait. NumPy voert de
    # rechthoekvullingen in native code uit en geeft de GIL daarbij vrij.
    import numpy as np

    raster = np.zeros((height, width), dtype=np.uint8)
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
        view = raster[
            first_row:last_row + 1,
            first_col:last_col + 1,
        ]
        np.maximum(view, value, out=view)

    rows, columns = np.nonzero(raster)
    intensities = raster[rows, columns]
    cells = tuple(
        zip(
            rows.astype(int).tolist(),
            columns.astype(int).tolist(),
            intensities.astype(int).tolist(),
        )
    )

    return RadarFrame(
        source=source,
        nominal_timestamp=int(timestamp // 60) * 60,
        bounds=(west, south, east, north),
        grid_deg=grid_deg,
        height=height,
        width=width,
        cells=cells,
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
        return self.add_frame(region_id, frame)

    def add_frame(self, region_id: str, frame: RadarFrame) -> RadarFrame:
        """Voeg een gevalideerd frame toe en wis incompatibele oude rasters."""
        frames = self._frames.setdefault(region_id, deque(maxlen=self._max_frames))
        if frames and (
            frames[-1].source != frame.source
            or frames[-1].bounds != frame.bounds
            or frames[-1].grid_deg != frame.grid_deg
            or frames[-1].height != frame.height
            or frames[-1].width != frame.width
        ):
            frames.clear()
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

    def restore(
        self,
        serialized: dict[str, list[dict[str, Any]]],
        bounds_by_region: dict[str, tuple[float, float, float, float]],
        *,
        max_age_seconds: int = 30 * 60,
        now: float | None = None,
    ) -> int:
        """Herstel alleen recente frames die nog exact op het regiogrid passen."""
        reference = time.time() if now is None else float(now)
        restored = 0
        for region_id, values in serialized.items():
            expected_bounds = bounds_by_region.get(region_id)
            if expected_bounds is None:
                continue
            for value in values:
                try:
                    frame = RadarFrame.from_dict(value)
                except (KeyError, TypeError, ValueError):
                    continue
                if (
                    reference - frame.nominal_timestamp > max_age_seconds
                    or frame.nominal_timestamp > reference + 60
                    or frame.bounds != expected_bounds
                ):
                    continue
                self.add_frame(region_id, frame)
                restored += 1
        return restored

    def serialized(self) -> dict[str, list[dict[str, Any]]]:
        return {
            region_id: [frame.to_dict() for frame in frames]
            for region_id, frames in self._frames.items()
        }

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
            "feeds_detection": any(
                len(frames) >= 3 for frames in self._frames.values()
            ),
        }
