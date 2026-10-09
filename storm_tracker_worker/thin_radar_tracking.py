"""Turn current-cycle native radar fields into bounded CellTrack inputs."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
from typing import Any

from pyproj import Transformer

from .thin_provider_adapters import ThinRadarField
from .tracking_core.engine.analysis_grid import AnalysisGrid
from .tracking_core.engine.hierarchical_detection import NormalizedRasterFrame
from .tracking_core.engine.cell_track_engine import (
    CellTrackEngineState,
    CellTrackTransitionResult,
    advance_cell_track_engine,
    create_cell_track_engine_state,
)
from .worker_only_cell_tracking import worker_only_tracking_policy


@dataclass(frozen=True, slots=True)
class TargetRadarFrame:
    target_id: str
    target_x_m: float
    target_y_m: float
    frame: NormalizedRasterFrame


@dataclass(frozen=True, slots=True)
class RadarTrack:
    track_id: str
    provider_id: str
    observed_at: float
    latitude: float
    longitude: float
    rain_rate_mm_h: float
    velocity_east_kmh: float
    velocity_north_kmh: float


class ThinRadarTracker:
    """In-memory CellTrackEngine state per provider and tracked target."""

    def __init__(self) -> None:
        self._policy = worker_only_tracking_policy()
        self._states: dict[tuple[str, str], CellTrackEngineState] = {}

    def advance(self, field: ThinRadarField, target: Any) -> CellTrackTransitionResult:
        prepared = build_target_radar_frame(
            field, target, maximum_pixels=self._policy.maximum_frame_pixels
        )
        key = (field.provider_id, prepared.target_id)
        state = self._states.get(key)
        if state is None:
            state = create_cell_track_engine_state(
                "thin-celltrack:" + _digest({"provider": key[0], "target": key[1]}),
                self._policy,
            )
        result = advance_cell_track_engine(state, prepared.frame, self._policy)
        self._states[key] = result.next_state
        return result

    def tracks(self, field: ThinRadarField, target_id: str) -> tuple[RadarTrack, ...]:
        state = self._states.get((field.provider_id, target_id))
        if state is None or state.last_frame is None:
            return ()
        inverse = Transformer.from_crs(field.crs, "EPSG:4326", always_xy=True)
        tracks: list[RadarTrack] = []
        for track in state.ledger.tracks:
            if track.lifecycle_state == "ended":
                continue
            estimate = state.estimate(track.track_id)
            x_m, y_m = estimate.x_m, estimate.y_m
            if field.provider_id == "knmi_radar":
                x_m, y_m = x_m / 1_000.0, y_m / 1_000.0
            longitude, latitude = inverse.transform(x_m, y_m)
            hierarchy = state.hierarchy(track.track_id)
            tracks.append(RadarTrack(
                track_id=track.track_id,
                provider_id=field.provider_id,
                observed_at=estimate.valid_at,
                latitude=float(latitude), longitude=float(longitude),
                rain_rate_mm_h=float(hierarchy.maximum_value),
                velocity_east_kmh=estimate.vx_m_s * 3.6,
                velocity_north_kmh=estimate.vy_m_s * 3.6,
            ))
        return tuple(tracks)


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def build_target_radar_frame(
    field: ThinRadarField,
    target: Any,
    *,
    maximum_pixels: int = 100_000,
) -> TargetRadarFrame:
    """Crop one radar field around a target without resampling its values."""
    if maximum_pixels < 1:
        raise ValueError("maximum_pixels_must_be_positive")
    transformer = Transformer.from_crs("EPSG:4326", field.crs, always_xy=True)
    target_x_m, target_y_m = transformer.transform(
        float(target.longitude), float(target.latitude)
    )
    # KNMI's native CRS reports kilometres.  Its retained grid is converted
    # to metres at decode time, so project target coordinates need the same
    # scale conversion before applying the generic grid math.
    if field.provider_id == "knmi_radar":
        target_x_m *= 1_000.0
        target_y_m *= 1_000.0
    height, width = field.rain_rate_mm_h.shape
    radius_m = float(target.observation_radius_km) * 1_000.0
    requested_width = min(width, max(1, math.ceil(2.0 * radius_m / field.x_resolution_m)))
    requested_height = min(height, max(1, math.ceil(2.0 * radius_m / field.y_resolution_m)))
    requested_pixels = requested_width * requested_height
    scale = min(1.0, math.sqrt(maximum_pixels / requested_pixels))
    selected_width = max(1, min(requested_width, math.floor(requested_width * scale)))
    selected_height = max(1, min(requested_height, math.floor(requested_height * scale)))
    while selected_width * selected_height > maximum_pixels:
        if selected_width >= selected_height:
            selected_width -= 1
        else:
            selected_height -= 1

    fractional_column = (target_x_m - field.origin_x_m) / field.x_resolution_m - 0.5
    fractional_row = (field.origin_y_m - target_y_m) / field.y_resolution_m - 0.5
    center_column = math.floor(fractional_column + 0.5)
    center_row = math.floor(fractional_row + 0.5)
    start_column = min(max(0, center_column - selected_width // 2), width - selected_width)
    start_row = min(max(0, center_row - selected_height // 2), height - selected_height)
    end_column = start_column + selected_width
    end_row = start_row + selected_height

    grid = AnalysisGrid(
        grid_id="thin-radar-grid:" + _digest({
            "provider": field.provider_id,
            "product": field.product_id,
            "source_origin": [field.origin_x_m, field.origin_y_m],
            "start": [start_row, start_column],
            "shape": [selected_height, selected_width],
        }),
        crs=field.crs,
        origin_x_m=field.origin_x_m + start_column * field.x_resolution_m,
        origin_y_m=field.origin_y_m - start_row * field.y_resolution_m,
        x_resolution_m=field.x_resolution_m,
        y_resolution_m=field.y_resolution_m,
        width=selected_width,
        height=selected_height,
    )
    values = tuple(
        tuple(
            float(field.rain_rate_mm_h[row, column]) if field.valid_mask[row, column] else None
            for column in range(start_column, end_column)
        )
        for row in range(start_row, end_row)
    )
    frame = NormalizedRasterFrame(
        frame_id="thin-radar-frame:" + _digest({
            "provider": field.provider_id,
            "product": field.product_id,
            "valid_at": field.observed_at,
            "grid": grid.grid_id,
        }),
        valid_at=field.observed_at,
        source=field.provider_id,
        quantity="rain_rate",
        unit="mm/h",
        grid=grid,
        values=values,
    )
    return TargetRadarFrame(
        target_id=str(target.target_id),
        target_x_m=float(target_x_m),
        target_y_m=float(target_y_m),
        frame=frame,
    )


__all__ = ["RadarTrack", "TargetRadarFrame", "ThinRadarTracker", "build_target_radar_frame"]
