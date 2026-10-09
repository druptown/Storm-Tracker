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
from .tracking_core.engine.cell_track_lifecycle import CellTrackLifecyclePolicy
from .tracking_core.engine.hierarchical_detection import (
    HierarchicalDetectorConfig,
    NormalizedRasterFrame,
)
from .tracking_core.engine.cell_track_engine import (
    CellTrackEnginePolicy,
    CellTrackEngineState,
    CellTrackTransitionResult,
    advance_cell_track_engine,
    create_cell_track_engine_state,
)
from .tracking_core.engine.lucas_kanade_flow import (
    DetectionFlowAggregationConfig,
    LucasKanadeConfig,
)
from .tracking_core.engine.recursive_track_estimator import (
    RecursiveTrackEstimatorPolicy,
)
from .tracking_core.engine.motion_consensus import (
    MotionConsensusResult,
    build_transition_motion_consensus,
)
from .tracking_core.engine.weather_system_grouping import (
    WeatherSystemGroupingPolicy,
)


_ZERO4 = (
    (0.0, 0.0, 0.0, 0.0),
    (0.0, 0.0, 0.0, 0.0),
    (0.0, 0.0, 0.0, 0.0),
    (0.0, 0.0, 0.0, 0.0),
)
_INITIAL4 = (
    (1_000_000.0, 0.0, 0.0, 0.0),
    (0.0, 1_000_000.0, 0.0, 0.0),
    (0.0, 0.0, 400.0, 0.0),
    (0.0, 0.0, 0.0, 400.0),
)


def _thin_tracking_policy() -> CellTrackEnginePolicy:
    """Build the bounded tracking policy without importing legacy runtime code."""

    return CellTrackEnginePolicy(
        policy_id="thin-operational-tracking-v1",
        detector=HierarchicalDetectorConfig(
            shield_threshold=1.0,
            core_threshold=4.0,
            minimum_shield_area_m2=1_000_000.0,
            minimum_core_area_m2=1_000_000.0,
            shield_position_sigma_m=1_000.0,
            core_position_sigma_m=750.0,
            connectivity=8,
            centroid_method="geometric",
        ),
        lifecycle=CellTrackLifecyclePolicy(
            policy_id="thin-operational-lifecycle-v1",
            confirmation_observations=2,
            maximum_gap_frames=2,
        ),
        estimator=RecursiveTrackEstimatorPolicy(
            policy_id="thin-operational-estimator-v1",
            acceleration_spectral_density_m2_s3=1.0,
        ),
        lucas_kanade=LucasKanadeConfig(
            window_radius_pixels=2,
            minimum_valid_samples=6,
            minimum_structure_eigenvalue=1e-9,
            maximum_condition_number=1e10,
            regularization=0.0,
            residual_variance_floor=1e-6,
            velocity_process_noise_covariance_m2_s2=(
                (1.0, 0.0),
                (0.0, 1.0),
            ),
        ),
        flow_aggregation=DetectionFlowAggregationConfig(
            minimum_valid_pixels=1,
            minimum_valid_fraction=0.1,
            velocity_covariance_floor_m2_s2=(
                (1.0, 0.0),
                (0.0, 1.0),
            ),
            residual_position_covariance_m2=(
                (1_000_000.0, 0.0),
                (0.0, 1_000_000.0),
            ),
            method="conservative_mean",
        ),
        weather_system=WeatherSystemGroupingPolicy(
            policy_id="thin-operational-systems-v1",
            pixel_connectivity=8,
            maximum_footprint_gap_m=20_000.0,
            maximum_motion_difference_m_s=1_000_000.0,
            maximum_motion_age_s=10_000.0,
            maximum_motion_time_difference_s=10_000.0,
        ),
        initial_state_covariance=_INITIAL4,
        prior_process_noise_covariance=_ZERO4,
        association_motion_sigma_m_s=100.0,
        motion_nll_weight=1.0,
        object_kind_change_cost=50.0,
        absolute_log_area_weight=1.0,
        mean_value_difference_weight=0.1,
        maximum_value_difference_weight=0.1,
        footprint_nonoverlap_weight=1.0,
        birth_cost=15.0,
        death_cost=15.0,
        exact_solution_limit=4,
        maximum_exact_current_detections=12,
        maximum_frame_pixels=100_000,
        maximum_detected_objects=192,
        # Keep the capacity bounds consistent with the configured two-frame
        # lifecycle gap.  Otherwise one fragmented but valid regional frame
        # can fill the active set and make every later frame fail closed.
        maximum_active_tracks=576,
        maximum_candidate_edges=110_592,
        maximum_total_tracks=4_096,
        maximum_lifecycle_events=32_768,
        maximum_track_history_detections=2_048,
        multi_hypothesis_identity=None,
        scalable_one_best_fallback=True,
    )


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
    motion_ready: bool
    geometry: dict[str, Any]
    motion_status: str
    motion_confidence: str
    motion_method: str | None
    motion_reason_codes: tuple[str, ...]
    motion_supporting_methods: tuple[str, ...]
    motion_agreement_angle_deg: float | None
    mean_rain_rate_mm_h: float | None = None


def _footprint_geometry(
    pixels: tuple[tuple[int, int], ...],
    grid: AnalysisGrid,
    inverse: Transformer,
) -> dict[str, Any]:
    """Merge equal horizontal pixel runs into exact raster rectangles."""

    by_row: dict[int, list[int]] = {}
    for row, column in pixels:
        by_row.setdefault(row, []).append(column)
    row_runs: dict[int, tuple[tuple[int, int], ...]] = {}
    for row, columns in by_row.items():
        ordered = sorted(set(columns))
        runs: list[tuple[int, int]] = []
        start = previous = ordered[0]
        for column in ordered[1:]:
            if column != previous + 1:
                runs.append((start, previous + 1))
                start = column
            previous = column
        runs.append((start, previous + 1))
        row_runs[row] = tuple(runs)

    active: dict[tuple[int, int], int] = {}
    rectangles: list[tuple[int, int, int, int]] = []
    previous_row: int | None = None
    for row in sorted(row_runs):
        current = set(row_runs[row])
        if previous_row is None or row != previous_row + 1:
            rectangles.extend(
                (start_row, previous_row + 1, run[0], run[1])
                for run, start_row in active.items()
            )
            active.clear()
        for run in tuple(active):
            if run not in current:
                rectangles.append((active.pop(run), row, run[0], run[1]))
        for run in current:
            active.setdefault(run, row)
        previous_row = row
    if previous_row is not None:
        rectangles.extend(
            (start_row, previous_row + 1, run[0], run[1])
            for run, start_row in active.items()
        )

    polygons: list[list[list[list[float]]]] = []
    for first_row, last_row, first_column, last_column in rectangles:
        left = grid.origin_x_m + first_column * grid.x_resolution_m
        right = grid.origin_x_m + last_column * grid.x_resolution_m
        top = grid.origin_y_m - first_row * grid.y_resolution_m
        bottom = grid.origin_y_m - last_row * grid.y_resolution_m
        ring = []
        for x_value, y_value in (
            (left, top), (right, top), (right, bottom),
            (left, bottom), (left, top),
        ):
            longitude, latitude = inverse.transform(x_value, y_value)
            ring.append([float(longitude), float(latitude)])
        polygons.append([ring])
    return {"type": "MultiPolygon", "coordinates": polygons}


class ThinRadarTracker:
    """In-memory CellTrackEngine state per provider and tracked target."""

    def __init__(self) -> None:
        self._policy = _thin_tracking_policy()
        self._states: dict[tuple[str, str], CellTrackEngineState] = {}
        self._motion_consensus: dict[
            tuple[str, str],
            dict[str, MotionConsensusResult],
        ] = {}

    def advance(
        self,
        field: ThinRadarField,
        target: Any,
    ) -> CellTrackTransitionResult | None:
        key = (field.provider_id, str(target.target_id))
        state = self._states.get(key)
        if state is not None and state.last_frame is not None and field.observed_at <= state.last_frame.valid_at:
            # Skip before cropping: repeated processing ticks are not new frames.
            return None
        prepared = build_target_radar_frame(
            field, target, maximum_pixels=self._policy.maximum_frame_pixels
        )
        if state is None:
            state = create_cell_track_engine_state(
                "thin-celltrack:" + _digest({"provider": key[0], "target": key[1]}),
                self._policy,
            )
        result = advance_cell_track_engine(state, prepared.frame, self._policy)
        if (
            result.status == "indeterminate"
            and "analysis_grid_changed" in result.reason_codes
        ):
            state = create_cell_track_engine_state(state.engine_id, self._policy)
            result = advance_cell_track_engine(
                state, prepared.frame, self._policy
            )
        self._states[key] = result.next_state
        self._motion_consensus[key] = dict(
            build_transition_motion_consensus(result)
        )
        return result

    def tracks(self, field: ThinRadarField, target_id: str) -> tuple[RadarTrack, ...]:
        state = self._states.get((field.provider_id, target_id))
        if state is None or state.last_frame is None:
            return ()
        inverse = Transformer.from_crs(field.crs, "EPSG:4326", always_xy=True)
        consensus_by_track = self._motion_consensus.get(
            (field.provider_id, target_id), {}
        )
        tracks: list[RadarTrack] = []
        for track in state.ledger.tracks:
            if track.lifecycle_state == "ended":
                continue
            estimate = state.estimate(track.track_id)
            x_m, y_m = estimate.x_m, estimate.y_m
            longitude, latitude = inverse.transform(x_m, y_m)
            hierarchy = state.hierarchy(track.track_id)
            consensus = consensus_by_track.get(track.track_id)
            motion_ready = bool(
                consensus is not None
                and consensus.status in {
                    "ready_high", "ready_medium", "fallback", "stationary",
                }
                and consensus.confidence in {"high", "medium"}
                and consensus.vx_m_s is not None
                and consensus.vy_m_s is not None
            )
            velocity_east_kmh = (
                float(consensus.vx_m_s) * 3.6 if motion_ready else 0.0
            )
            velocity_north_kmh = (
                float(consensus.vy_m_s) * 3.6 if motion_ready else 0.0
            )
            tracks.append(RadarTrack(
                track_id=track.track_id,
                provider_id=field.provider_id,
                observed_at=estimate.valid_at,
                latitude=float(latitude), longitude=float(longitude),
                rain_rate_mm_h=float(hierarchy.maximum_value),
                mean_rain_rate_mm_h=float(hierarchy.mean_value),
                velocity_east_kmh=velocity_east_kmh,
                velocity_north_kmh=velocity_north_kmh,
                motion_ready=motion_ready,
                geometry=_footprint_geometry(
                    hierarchy.footprint_pixels,
                    state.last_frame.grid,
                    inverse,
                ),
                motion_status=(
                    "indeterminate" if consensus is None else consensus.status
                ),
                motion_confidence=(
                    "indeterminate"
                    if consensus is None
                    else consensus.confidence
                ),
                motion_method=(
                    None if consensus is None else consensus.selected_method
                ),
                motion_reason_codes=(
                    ("motion_consensus_unavailable",)
                    if consensus is None
                    else consensus.reason_codes
                ),
                motion_supporting_methods=(
                    () if consensus is None else consensus.supporting_methods
                ),
                motion_agreement_angle_deg=(
                    None if consensus is None else consensus.agreement_angle_deg
                ),
            ))
        return tuple(tracks)

    def systems(self, field: ThinRadarField, target_id: str) -> tuple[RadarTrack, ...]:
        """Expose current WeatherSystems as forecastable aggregate footprints."""
        state = self._states.get((field.provider_id, target_id))
        if (
            state is None
            or state.last_frame is None
            or state.weather_systems is None
        ):
            return ()
        inverse = Transformer.from_crs(field.crs, "EPSG:4326", always_xy=True)
        grid = state.last_frame.grid
        systems: list[RadarTrack] = []
        for system in state.weather_systems.systems:
            pixels = system.aggregate_current_footprint
            if not pixels:
                continue
            x_m = math.fsum(
                grid.origin_x_m + (column + 0.5) * grid.x_resolution_m
                for _row, column in pixels
            ) / len(pixels)
            y_m = math.fsum(
                grid.origin_y_m - (row + 0.5) * grid.y_resolution_m
                for row, _column in pixels
            ) / len(pixels)
            longitude, latitude = inverse.transform(x_m, y_m)
            values = tuple(
                float(state.last_frame.values[row][column])
                for row, column in pixels
                if state.last_frame.values[row][column] is not None
            )
            if not values:
                continue
            motion = system.descriptive_motion
            ready = motion.vx_m_s is not None
            east_kmh = 0.0 if not ready else float(motion.vx_m_s) * 3.6
            north_kmh = 0.0 if not ready else float(motion.vy_m_s) * 3.6
            supporting_methods = tuple(sorted({
                str(member.motion_method)
                for member in system.member_tracks
                if member.motion_method is not None
            }))
            systems.append(RadarTrack(
                track_id=system.system_id,
                provider_id=field.provider_id,
                observed_at=system.valid_at,
                latitude=float(latitude),
                longitude=float(longitude),
                rain_rate_mm_h=max(values),
                mean_rain_rate_mm_h=math.fsum(values) / len(values),
                velocity_east_kmh=east_kmh,
                velocity_north_kmh=north_kmh,
                motion_ready=ready,
                geometry=_footprint_geometry(pixels, grid, inverse),
                motion_status="ready_medium" if ready else "indeterminate",
                motion_confidence="medium" if ready else "indeterminate",
                motion_method=motion.method if ready else None,
                motion_reason_codes=tuple(system.system_uncertainty),
                motion_supporting_methods=supporting_methods,
                motion_agreement_angle_deg=None,
            ))
        return tuple(systems)


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
