"""Bounded radar-frame motion analysis outside Home Assistant Core."""
from __future__ import annotations

from dataclasses import dataclass
import math

import numpy as np

# Resource policy, geen meteorologische snelheidsgrens. De zoeklus onderzoekt
# maximaal (2 * N + 1)² verschuivingen rond de verplaatsing van het binaire
# neerslagmasker.
COMPUTE_SEARCH_HALF_WINDOW_CELLS = 12
MAX_FRAMES = 4
EVALUATION_FRAMES = 3
MAX_GRID_CELLS = 640_000
MAX_WET_CELLS = 640_000
NOWCAST_HORIZON_MINUTES = 60
NOWCAST_STEP_MINUTES = 5


@dataclass(frozen=True)
class Frame:
    source: str
    nominal_timestamp: int
    bounds: tuple[float, float, float, float]
    grid_deg: float
    height: int
    width: int
    cells: tuple[tuple[int, int, int], ...]

    @classmethod
    def from_dict(cls, value: dict) -> "Frame":
        height, width = int(value["height"]), int(value["width"])
        if height <= 0 or width <= 0 or height * width > MAX_GRID_CELLS:
            raise ValueError("ongeldige of te grote rastervorm")
        raw_cells = value.get("cells")
        if not isinstance(raw_cells, list) or len(raw_cells) > MAX_WET_CELLS:
            raise ValueError("ongeldige of te grote cellijst")
        cells = tuple(
            (int(row), int(column), int(intensity))
            for row, column, intensity in raw_cells
        )
        if any(
            row < 0 or row >= height or column < 0 or column >= width
            or intensity <= 0 or intensity > 255
            for row, column, intensity in cells
        ):
            raise ValueError("rastercel valt buiten het contract")
        bounds = tuple(float(item) for item in value["bounds"])
        if len(bounds) != 4 or not (
            bounds[0] < bounds[2] and bounds[1] < bounds[3]
        ):
            raise ValueError("ongeldige rastergrenzen")
        grid_deg = float(value["grid_deg"])
        if not 0 < grid_deg <= 1:
            raise ValueError("ongeldige rasterresolutie")
        return cls(
            source=str(value["source"]),
            nominal_timestamp=int(value["nominal_timestamp"]),
            bounds=bounds,
            grid_deg=grid_deg,
            height=height,
            width=width,
            cells=cells,
        )


def _compatible(frames: tuple[Frame, ...]) -> bool:
    first = frames[0]
    return all(
        frame.source == first.source
        and frame.bounds == first.bounds
        and frame.height == first.height
        and frame.width == first.width
        and frame.grid_deg == first.grid_deg
        for frame in frames[1:]
    )


def _weighted_centroid(frame: Frame):
    total = sum(intensity for _, _, intensity in frame.cells)
    if total <= 0:
        return None
    return (
        sum(row * intensity for row, _, intensity in frame.cells) / total,
        sum(column * intensity for _, column, intensity in frame.cells) / total,
    )


def _support_centroid(frame: Frame):
    """Geometrisch centrum van natte cellen, onafhankelijk van intensiteit."""
    if not frame.cells:
        return None
    count = len(frame.cells)
    return (
        sum(row for row, _, _ in frame.cells) / count,
        sum(column for _, column, _ in frame.cells) / count,
    )


def _axis_search_window(center: int, maximum_shift: int) -> tuple[int, int]:
    return (
        max(-maximum_shift, center - COMPUTE_SEARCH_HALF_WINDOW_CELLS),
        min(maximum_shift, center + COMPUTE_SEARCH_HALF_WINDOW_CELLS),
    )


def _motion_values(frame: Frame, rows_per_minute: float, cols_per_minute: float):
    center_lat = (frame.bounds[1] + frame.bounds[3]) / 2.0
    north = -rows_per_minute * frame.grid_deg * 110.574 * 60.0
    east = (
        cols_per_minute * frame.grid_deg * 111.320
        * max(0.1, abs(math.cos(math.radians(center_lat)))) * 60.0
    )
    speed = math.hypot(east, north)
    heading = (
        0.0 if speed < 1e-9
        else (math.degrees(math.atan2(east, north)) + 360.0) % 360.0
    )
    return speed, heading


def _centroid_motion(origin: Frame, observed: Frame):
    start, end = _weighted_centroid(origin), _weighted_centroid(observed)
    minutes = (observed.nominal_timestamp - origin.nominal_timestamp) / 60.0
    if start is None or end is None or minutes <= 0:
        return None
    return _motion_values(
        origin, (end[0] - start[0]) / minutes, (end[1] - start[1]) / minutes
    )


def _match(origin: Frame, observed: Frame):
    """Zoek rond een onbeperkte centroidverplaatsing met een vaste werkbudget."""
    minutes = (observed.nominal_timestamp - origin.nominal_timestamp) / 60.0
    if minutes <= 0 or not origin.cells or not observed.cells:
        return None
    origin_center = _support_centroid(origin)
    observed_center = _support_centroid(observed)
    if origin_center is None or observed_center is None:
        return None
    center_row_shift = round(observed_center[0] - origin_center[0])
    center_column_shift = round(observed_center[1] - origin_center[1])
    max_row_shift = origin.height - 1
    max_column_shift = origin.width - 1
    row_min, row_max = _axis_search_window(
        center_row_shift, max_row_shift
    )
    column_min, column_max = _axis_search_window(
        center_column_shift, max_column_shift
    )
    search_window_exhaustive = (
        row_min == -max_row_shift
        and row_max == max_row_shift
        and column_min == -max_column_shift
        and column_max == max_column_shift
    )
    origin_mask = np.zeros((origin.height, origin.width), dtype=np.bool_)
    observed_mask = np.zeros((observed.height, observed.width), dtype=np.bool_)
    origin_rows, origin_columns = zip(*(
        (row, column) for row, column, _ in origin.cells
    ))
    observed_rows, observed_columns = zip(*(
        (row, column) for row, column, _ in observed.cells
    ))
    origin_mask[origin_rows, origin_columns] = True
    observed_mask[observed_rows, observed_columns] = True
    actual_count = int(np.count_nonzero(observed_mask))
    best = None
    for row_shift in range(row_min, row_max + 1):
        for column_shift in range(column_min, column_max + 1):
            row_start, row_stop = max(0, -row_shift), min(
                origin.height, origin.height - row_shift
            )
            col_start, col_stop = max(0, -column_shift), min(
                origin.width, origin.width - column_shift
            )
            expected = origin_mask[row_start:row_stop, col_start:col_stop]
            actual = observed_mask[
                row_start + row_shift:row_stop + row_shift,
                col_start + column_shift:col_stop + column_shift,
            ]
            expected_count = int(np.count_nonzero(expected))
            overlap = int(np.count_nonzero(np.logical_and(expected, actual)))
            union = expected_count + actual_count - overlap
            iou = overlap / union if union else 0.0
            candidate = (
                iou,
                overlap,
                -(
                    (row_shift - center_row_shift) ** 2
                    + (column_shift - center_column_shift) ** 2
                ),
                row_shift, column_shift,
            )
            if best is None or candidate[:3] > best[:3]:
                best = candidate
    if best is None:
        return None
    iou, overlap, _, row_shift, column_shift = best
    speed, heading = _motion_values(
        origin, row_shift / minutes, column_shift / minutes
    )
    artificial_boundary_hit = (
        (row_shift == row_min and row_min > -max_row_shift)
        or (row_shift == row_max and row_max < max_row_shift)
        or (
            column_shift == column_min
            and column_min > -max_column_shift
        )
        or (
            column_shift == column_max
            and column_max < max_column_shift
        )
    )
    translation_censored = artificial_boundary_hit or (
        overlap == 0 and not search_window_exhaustive
    )
    censor_reason = None
    if artificial_boundary_hit:
        censor_reason = "beste_match_op_rand_computationeel_zoekvenster"
    elif overlap == 0 and not search_window_exhaustive:
        censor_reason = "geen_overlap_binnen_computationeel_zoekvenster"
    return {
        "row_per_minute": row_shift / minutes,
        "column_per_minute": column_shift / minutes,
        "speed_kmh": speed,
        "heading_deg": heading,
        "match_iou": iou,
        "overlap_cells": overlap,
        "search_center_row_shift": center_row_shift,
        "search_center_column_shift": center_column_shift,
        "search_window_row_min": row_min,
        "search_window_row_max": row_max,
        "search_window_column_min": column_min,
        "search_window_column_max": column_max,
        "search_window_candidates": (
            (row_max - row_min + 1) * (column_max - column_min + 1)
        ),
        "search_window_exhaustive": search_window_exhaustive,
        "translation_censored": translation_censored,
        "censor_reason": censor_reason,
    }


def _shifted_iou(origin: Frame, observed: Frame, row_shift: int, col_shift: int):
    actual = {(row, column) for row, column, _ in observed.cells}
    expected_count = overlap = 0
    for row, column, _ in origin.cells:
        shifted = (row + row_shift, column + col_shift)
        if 0 <= shifted[0] < origin.height and 0 <= shifted[1] < origin.width:
            expected_count += 1
            overlap += shifted in actual
    union = expected_count + len(actual) - overlap
    return overlap / union if union else 0.0


def evaluate(frames: tuple[Frame, ...]) -> dict:
    frames = frames[-EVALUATION_FRAMES:]
    base = {
        "status": "insufficient_frames", "experimental": True,
        "feeds_detection": False, "frames_used": len(frames),
        "method": "bounded_grid_match_v1",
        "search_policy": "centroid_guided_fixed_cell_window_v2",
        "compute_search_half_window_cells": COMPUTE_SEARCH_HALF_WINDOW_CELLS,
        "confidence": "insufficient",
        "translation_km_per_hour": None, "heading_deg": None,
        "verification_iou": None, "motion_match_iou": None,
        "vector_consistency_deg": None, "wet_cell_ratio": None,
        "centroid_translation_km_per_hour": None,
        "centroid_heading_deg": None,
        "search_window_candidates": None,
        "search_window_exhaustive": None,
        "translation_censored": None,
        "translation_uncertainty": None,
        "reason": None,
    }
    if len(frames) < 2:
        return base
    if not _compatible(frames):
        return {**base, "status": "incompatible_frames",
                "reason": "bron_of_raster_gewijzigd"}
    if any(not frame.cells for frame in frames):
        return {**base, "status": "dry_or_empty",
                "reason": "geen_neerslag_in_vergelijkbare_frames"}
    if any(
        newer.nominal_timestamp <= older.nominal_timestamp
        for older, newer in zip(frames, frames[1:])
    ):
        return {**base, "status": "invalid_timestamps",
                "reason": "geen_oplopende_nominale_minuten"}
    latest = _match(frames[-2], frames[-1])
    if latest is None:
        return {**base, "status": "no_motion_match",
                "reason": "geen_rastermatch"}
    centroid = _centroid_motion(frames[-2], frames[-1])
    wet_ratio = len(frames[-1].cells) / max(1, len(frames[-2].cells))
    result = {
        **base, "status": "warming_up_vectors",
        "translation_km_per_hour": round(latest["speed_kmh"], 1),
        "heading_deg": round(latest["heading_deg"], 0),
        "motion_match_iou": round(latest["match_iou"], 3),
        "wet_cell_ratio": round(wet_ratio, 3),
        "centroid_translation_km_per_hour": (
            round(centroid[0], 1) if centroid else None
        ),
        "centroid_heading_deg": round(centroid[1], 0) if centroid else None,
        "search_window_candidates": latest["search_window_candidates"],
        "search_window_exhaustive": latest["search_window_exhaustive"],
        "translation_censored": latest["translation_censored"],
        "translation_uncertainty": (
            "open_ended_outside_compute_window"
            if latest["translation_censored"]
            else "resolved_inside_compute_window"
        ),
        "reason": "tweede_opeenvolgende_vector_nodig",
    }
    if latest["translation_censored"]:
        return {
            **result,
            "status": "motion_indeterminate",
            "confidence": "indeterminate",
            "reason": latest["censor_reason"],
        }
    if latest["overlap_cells"] <= 0:
        return {
            **result,
            "status": "no_motion_match",
            "confidence": "insufficient",
            "reason": "geen_rasteroverlap",
        }
    if len(frames) < 3:
        return result
    previous = _match(frames[0], frames[1])
    if previous is None:
        return {**result, "status": "no_motion_match",
                "reason": "geen_vorige_rastermatch"}
    if previous["translation_censored"]:
        return {
            **result,
            "status": "motion_indeterminate",
            "confidence": "indeterminate",
            "reason": "vorige_vector_buiten_computationeel_zoekvenster",
        }
    if previous["overlap_cells"] <= 0:
        return {
            **result,
            "status": "no_motion_match",
            "confidence": "insufficient",
            "reason": "geen_vorige_rasteroverlap",
        }
    minutes = (frames[2].nominal_timestamp - frames[1].nominal_timestamp) / 60.0
    verification = _shifted_iou(
        frames[1], frames[2],
        round(previous["row_per_minute"] * minutes),
        round(previous["column_per_minute"] * minutes),
    )
    angle = abs(
        (latest["heading_deg"] - previous["heading_deg"] + 180.0) % 360.0
        - 180.0
    )
    max_speed = max(previous["speed_kmh"], latest["speed_kmh"])
    speed_ratio = (
        1.0 if max_speed <= 5.0
        else min(previous["speed_kmh"], latest["speed_kmh"]) / max_speed
    )
    if verification >= 0.35 and angle <= 30 and speed_ratio >= 0.5:
        confidence, status, reason = "high", "ready", None
    elif verification >= 0.15 and angle <= 60 and speed_ratio >= 0.35:
        confidence, status, reason = "medium", "ready", None
    else:
        confidence, status, reason = (
            "low", "unstable_motion", "lage_overlap_of_inconsistente_vector"
        )
    return {
        **result, "status": status, "confidence": confidence,
        "feeds_detection": status == "ready",
        "verification_iou": round(verification, 3),
        "vector_consistency_deg": round(angle, 1), "reason": reason,
    }


def _shift_cells(
    frame: Frame, row_shift: int, column_shift: int
) -> tuple[tuple[int, int, int], ...]:
    """Advecteer een schaars raster zonder grote MQTT-rasterpayload."""
    shifted: dict[tuple[int, int], int] = {}
    for row, column, intensity in frame.cells:
        target = (row + row_shift, column + column_shift)
        if 0 <= target[0] < frame.height and 0 <= target[1] < frame.width:
            shifted[target] = max(intensity, shifted.get(target, 0))
    return tuple(
        (row, column, intensity)
        for (row, column), intensity in sorted(shifted.items())
    )


def _target_cell(frame: Frame, latitude: float, longitude: float):
    west, south, east, north = frame.bounds
    if not west <= longitude <= east or not south <= latitude <= north:
        return None
    return (
        min(frame.height - 1, max(0, int((north - latitude) / frame.grid_deg))),
        min(frame.width - 1, max(0, int((longitude - west) / frame.grid_deg))),
    )


def _adaptive_motion_model(frames: tuple[Frame, ...], latest_match: dict) -> dict:
    """Kies versnelling alleen na een echte één-frame-hindcast.

    Vier frames leveren drie opeenvolgende intervalvectoren. De eerste twee
    schatten een versnelling; daarmee voorspellen we het vierde frame. Alleen
    wanneer die voorspelling duidelijk beter overlapt dan constante snelheid,
    mag de meest recente versnelling de operationele nowcast sturen.
    """
    linear = {
        "motion_model": "linear",
        "row_velocity_per_minute": latest_match["row_per_minute"],
        "column_velocity_per_minute": latest_match["column_per_minute"],
        "row_acceleration_per_minute2": 0.0,
        "column_acceleration_per_minute2": 0.0,
        "acceleration_kmh2": 0.0,
        "hindcast_linear_iou": None,
        "hindcast_adaptive_iou": None,
        "model_gain": 0.0,
        "projected_speed_kmh": round(latest_match["speed_kmh"], 1),
        "model_selection_basis": "hindcast_skill_only",
    }
    if len(frames) < 4:
        return linear
    first, second, third, latest = frames[-4:]
    matches = (
        _match(first, second),
        _match(second, third),
        _match(third, latest),
    )
    if any(
        match is None
        or match["translation_censored"]
        or match["overlap_cells"] <= 0
        for match in matches
    ):
        return {
            **linear,
            "model_selection_basis": "indeterminate_hindcast_vector",
        }
    first_match, second_match, third_match = matches
    first_midpoint = (
        first.nominal_timestamp + second.nominal_timestamp
    ) / 120.0
    second_midpoint = (
        second.nominal_timestamp + third.nominal_timestamp
    ) / 120.0
    third_midpoint = (
        third.nominal_timestamp + latest.nominal_timestamp
    ) / 120.0
    previous_delta = second_midpoint - first_midpoint
    current_delta = third_midpoint - second_midpoint
    interval_minutes = (
        latest.nominal_timestamp - third.nominal_timestamp
    ) / 60.0
    if min(previous_delta, current_delta, interval_minutes) <= 0:
        return linear

    previous_row_acceleration = (
        second_match["row_per_minute"] - first_match["row_per_minute"]
    ) / previous_delta
    previous_column_acceleration = (
        second_match["column_per_minute"] - first_match["column_per_minute"]
    ) / previous_delta
    predicted_row_rate = (
        second_match["row_per_minute"]
        + previous_row_acceleration * current_delta
    )
    predicted_column_rate = (
        second_match["column_per_minute"]
        + previous_column_acceleration * current_delta
    )
    adaptive_iou = _shifted_iou(
        third,
        latest,
        round(predicted_row_rate * interval_minutes),
        round(predicted_column_rate * interval_minutes),
    )
    linear_iou = _shifted_iou(
        third,
        latest,
        round(second_match["row_per_minute"] * interval_minutes),
        round(second_match["column_per_minute"] * interval_minutes),
    )

    row_acceleration = (
        third_match["row_per_minute"] - second_match["row_per_minute"]
    ) / current_delta
    column_acceleration = (
        third_match["column_per_minute"] - second_match["column_per_minute"]
    ) / current_delta
    acceleration_kmh2 = _motion_values(
        latest,
        row_acceleration * 60.0,
        column_acceleration * 60.0,
    )[0]
    row_velocity_now = (
        third_match["row_per_minute"]
        + 0.5 * row_acceleration * interval_minutes
    )
    column_velocity_now = (
        third_match["column_per_minute"]
        + 0.5 * column_acceleration * interval_minutes
    )
    future_speed = _motion_values(
        latest,
        row_velocity_now
        + row_acceleration * NOWCAST_HORIZON_MINUTES,
        column_velocity_now
        + column_acceleration * NOWCAST_HORIZON_MINUTES,
    )[0]
    gain = adaptive_iou - linear_iou
    if not (
        adaptive_iou >= 0.20
        and gain >= 0.08
    ):
        return {
            **linear,
            "acceleration_kmh2": round(acceleration_kmh2, 1),
            "hindcast_linear_iou": round(linear_iou, 3),
            "hindcast_adaptive_iou": round(adaptive_iou, 3),
            "model_gain": round(gain, 3),
            "candidate_accelerated_projected_speed_kmh": round(
                future_speed, 1
            ),
            "model_selection_basis": "hindcast_skill_only",
        }
    return {
        "motion_model": "constant_acceleration",
        "row_velocity_per_minute": row_velocity_now,
        "column_velocity_per_minute": column_velocity_now,
        "row_acceleration_per_minute2": row_acceleration,
        "column_acceleration_per_minute2": column_acceleration,
        "acceleration_kmh2": round(acceleration_kmh2, 1),
        "hindcast_linear_iou": round(linear_iou, 3),
        "hindcast_adaptive_iou": round(adaptive_iou, 3),
        "model_gain": round(gain, 3),
        "projected_speed_kmh": round(future_speed, 1),
        "model_selection_basis": "hindcast_skill_only",
    }


def deterministic_nowcast(
    frames: tuple[Frame, ...], motion: dict, targets: list[dict] | None = None
) -> dict:
    """Maak een begrensde 0–60 min advectienowcast in de worker."""
    base = {
        "method": "adaptive_sparse_advection_v2",
        "status": "insufficient_motion",
        "horizon_minutes": NOWCAST_HORIZON_MINUTES,
        "step_minutes": NOWCAST_STEP_MINUTES,
        "steps": [],
        "targets": {},
        "reason": None,
    }
    if not frames:
        return base
    if motion.get("status") != "ready":
        return {
            **base,
            "status": (
                "motion_indeterminate"
                if motion.get("status") == "motion_indeterminate"
                else "insufficient_motion"
            ),
            "reason": motion.get("reason"),
        }
    latest_match = _match(frames[-2], frames[-1])
    if latest_match is None:
        return base
    latest = frames[-1]
    model = _adaptive_motion_model(frames, latest_match)
    target_values = {
        str(item["target_id"]): {
            "eta_minutes": None,
            "peak_intensity": 0,
            "first_step_intensity": None,
        }
        for item in (targets or [])
        if isinstance(item, dict) and item.get("target_id") is not None
    }
    steps = []
    for minute in range(0, NOWCAST_HORIZON_MINUTES + 1, NOWCAST_STEP_MINUTES):
        cells = _shift_cells(
            latest,
            round(
                model["row_velocity_per_minute"] * minute
                + 0.5
                * model["row_acceleration_per_minute2"]
                * minute * minute
            ),
            round(
                model["column_velocity_per_minute"] * minute
                + 0.5
                * model["column_acceleration_per_minute2"]
                * minute * minute
            ),
        )
        intensity_by_cell = {(row, column): value for row, column, value in cells}
        rows = [row for row, _, _ in cells]
        columns = [column for _, column, _ in cells]
        steps.append({
            "lead_minutes": minute,
            "valid_timestamp": latest.nominal_timestamp + minute * 60,
            "wet_cells": len(cells),
            "peak_intensity": max((value for _, _, value in cells), default=0),
            "cell_bbox": (
                [min(rows), min(columns), max(rows), max(columns)]
                if cells else None
            ),
        })
        for target in targets or []:
            target_id = str(target.get("target_id"))
            if target_id not in target_values:
                continue
            cell = _target_cell(
                latest, float(target["latitude"]), float(target["longitude"])
            )
            if cell is None:
                continue
            # Eén cel rondom het punt vangt afronding en de contourrand op.
            intensity = max(
                (
                    intensity_by_cell.get((cell[0] + dr, cell[1] + dc), 0)
                    for dr in (-1, 0, 1)
                    for dc in (-1, 0, 1)
                ),
                default=0,
            )
            item = target_values[target_id]
            item["peak_intensity"] = max(item["peak_intensity"], intensity)
            if intensity > 0 and item["eta_minutes"] is None:
                item["eta_minutes"] = minute
                item["first_step_intensity"] = intensity
    return {
        **base,
        "status": "ready",
        "source": latest.source,
        "base_timestamp": latest.nominal_timestamp,
        "confidence": motion.get("confidence"),
        "motion_model": model["motion_model"],
        "acceleration_kmh2": model["acceleration_kmh2"],
        "hindcast_linear_iou": model["hindcast_linear_iou"],
        "hindcast_adaptive_iou": model["hindcast_adaptive_iou"],
        "model_gain": model["model_gain"],
        "projected_speed_kmh": model["projected_speed_kmh"],
        "model_selection_basis": model["model_selection_basis"],
        "steps": steps,
        "targets": target_values,
    }


def motion_preflight(job) -> dict:
    raw_frames = job.payload.get("frames")
    if not isinstance(raw_frames, list) or len(raw_frames) > MAX_FRAMES:
        raise ValueError("ongeldige framecollectie")
    frames = tuple(Frame.from_dict(value) for value in raw_frames)
    result = evaluate(frames)
    result["nowcast"] = deterministic_nowcast(
        frames, result, job.payload.get("targets")
    )
    return {"result": result, "region_id": job.payload.get("region_id")}


motion_preflight_shadow = motion_preflight
