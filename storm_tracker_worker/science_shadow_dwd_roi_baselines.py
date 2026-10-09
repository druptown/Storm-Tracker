"""Independent raster-translation references for the immutable DWD ROI.

These deterministic offline references never inspect tracker outputs and are
never used to tune identity costs.  Missing values (including nodata,
undetect and mixed 2-km support) are excluded from every calculation; they are
not filled, interpolated or reclassified as dry.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import math
from typing import Any

import numpy as np

from .hard_decoder_isolation import _canonical_json_bytes
from .science_shadow_dwd_roi_replay import (
    DwdRoiScienceReplayExport,
    DwdScienceReplayError,
    EXPECTED_ANALYSIS_PIXELS,
    EXPECTED_FRAME_COUNT,
    load_dwd_roi_science_evidence,
)


DWD_ROI_BASELINE_CONTRACT = "stv3-dwd-roi-independent-raster-reference-v1"


@dataclass(frozen=True, slots=True)
class DwdRoiBaselineConfig:
    support_threshold_mm_h: float = 1.0
    minimum_overlap_pixels: int = 64
    minimum_positive_pixels_each: int = 4
    variance_epsilon: float = 1e-12
    tie_tolerance: float = 1e-10
    max_frame_pixels: int = 250_000
    max_correlation_elements: int = 1_000_000
    max_estimated_array_bytes: int = 256 * 1024 * 1024

    def __post_init__(self) -> None:
        if self.support_threshold_mm_h != 1.0:
            raise ValueError("support threshold is fixed before replay")
        for label, value, upper in (
            ("minimum overlap", self.minimum_overlap_pixels, 1_000_000),
            ("minimum positive", self.minimum_positive_pixels_each, 1_000_000),
            ("max frame pixels", self.max_frame_pixels, 4_000_000),
            ("max correlation elements", self.max_correlation_elements, 8_000_000),
            ("max array bytes", self.max_estimated_array_bytes, 2 * 1024**3),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= upper:
                raise ValueError(f"{label} is outside its hard bound")
        for label, value in (
            ("variance epsilon", self.variance_epsilon),
            ("tie tolerance", self.tie_tolerance),
        ):
            if not math.isfinite(value) or value <= 0.0:
                raise ValueError(f"{label} is invalid")

    def to_mapping(self) -> dict[str, int | float | str]:
        return {
            "algorithm": "masked_full_domain_normalized_cross_correlation_fft_v1",
            "max_correlation_elements": self.max_correlation_elements,
            "max_estimated_array_bytes": self.max_estimated_array_bytes,
            "max_frame_pixels": self.max_frame_pixels,
            "minimum_overlap_pixels": self.minimum_overlap_pixels,
            "minimum_positive_pixels_each": self.minimum_positive_pixels_each,
            "support_threshold_mm_h": self.support_threshold_mm_h,
            "tie_tolerance": self.tie_tolerance,
            "variance_epsilon": self.variance_epsilon,
        }

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(_canonical_json_bytes(self.to_mapping())).hexdigest()


def _round(value: float) -> float:
    return float(format(float(value), ".12g"))


def _fft_convolve(first: np.ndarray, second: np.ndarray) -> np.ndarray:
    shape = (
        first.shape[0] + second.shape[0] - 1,
        first.shape[1] + second.shape[1] - 1,
    )
    return np.fft.irfftn(
        np.fft.rfftn(first, shape, axes=(-2, -1))
        * np.fft.rfftn(second, shape, axes=(-2, -1)),
        shape,
        axes=(-2, -1),
    )


def _correlate(current: np.ndarray, previous: np.ndarray) -> np.ndarray:
    return _fft_convolve(current, previous[::-1, ::-1])


def masked_translation_reference(
    previous: np.ndarray,
    current: np.ndarray,
    previous_valid: np.ndarray,
    current_valid: np.ndarray,
    *,
    x_resolution_m: float,
    y_resolution_m: float,
    elapsed_seconds: float,
    config: DwdRoiBaselineConfig,
) -> dict[str, Any]:
    """Search every raster shift using overlap-normalized physical rates."""

    arrays = (previous, current, previous_valid, current_valid)
    if (
        any(not isinstance(item, np.ndarray) or item.ndim != 2 for item in arrays)
        or len({item.shape for item in arrays}) != 1
        or previous.dtype != np.dtype("float64")
        or current.dtype != np.dtype("float64")
        or previous_valid.dtype != np.dtype("bool")
        or current_valid.dtype != np.dtype("bool")
    ):
        raise DwdScienceReplayError("baseline raster contract differs")
    height, width = previous.shape
    pixels = height * width
    correlation_elements = (2 * height - 1) * (2 * width - 1)
    estimated_bytes = correlation_elements * 8 * 16 + pixels * 8 * 8
    if (
        pixels > config.max_frame_pixels
        or correlation_elements > config.max_correlation_elements
        or estimated_bytes > config.max_estimated_array_bytes
    ):
        raise DwdScienceReplayError("baseline array capacity exceeded")
    if (
        not math.isfinite(x_resolution_m)
        or x_resolution_m <= 0.0
        or not math.isfinite(y_resolution_m)
        or y_resolution_m <= 0.0
        or not math.isfinite(elapsed_seconds)
        or elapsed_seconds <= 0.0
    ):
        raise DwdScienceReplayError("baseline metric geometry is invalid")
    if (
        np.any(~np.isfinite(previous[previous_valid]))
        or np.any(~np.isfinite(current[current_valid]))
        or np.any(previous[previous_valid] < 0.0)
        or np.any(current[current_valid] < 0.0)
    ):
        raise DwdScienceReplayError("baseline physical rates are invalid")

    pm = previous_valid.astype(np.float64)
    cm = current_valid.astype(np.float64)
    pv = np.where(previous_valid, previous, 0.0)
    cv = np.where(current_valid, current, 0.0)
    count = np.rint(_correlate(cm, pm))
    sum_current = _correlate(cv, pm)
    sum_previous = _correlate(cm, pv)
    sum_current_sq = _correlate(cv * cv, pm)
    sum_previous_sq = _correlate(cm, pv * pv)
    sum_product = _correlate(cv, pv)
    positive_current = np.rint(_correlate(
        np.logical_and(current_valid, current > 0.0).astype(np.float64), pm
    ))
    positive_previous = np.rint(_correlate(
        cm, np.logical_and(previous_valid, previous > 0.0).astype(np.float64)
    ))

    eligible = (
        (count >= config.minimum_overlap_pixels)
        & (positive_current >= config.minimum_positive_pixels_each)
        & (positive_previous >= config.minimum_positive_pixels_each)
    )
    safe_count = np.where(eligible, count, 1.0)
    numerator = sum_product - sum_current * sum_previous / safe_count
    variance_current = sum_current_sq - sum_current * sum_current / safe_count
    variance_previous = sum_previous_sq - sum_previous * sum_previous / safe_count
    variance_current = np.maximum(variance_current, 0.0)
    variance_previous = np.maximum(variance_previous, 0.0)
    denominator = np.sqrt(variance_current * variance_previous)
    eligible &= denominator > config.variance_epsilon
    scores = np.full(count.shape, -np.inf, dtype=np.float64)
    np.divide(numerator, denominator, out=scores, where=eligible)
    scores[eligible] = np.clip(scores[eligible], -1.0, 1.0)
    finite_positions = np.argwhere(np.isfinite(scores))
    if finite_positions.size == 0:
        return {
            "candidate_shifts": 0,
            "indeterminate_reason": "insufficient_masked_overlap_or_variance",
            "status": "indeterminate",
        }
    best = float(np.max(scores))
    tied = np.argwhere(np.logical_and(
        np.isfinite(scores), np.abs(scores - best) <= config.tie_tolerance
    ))
    # Ties remain explicit; this ordering is only a deterministic receipt rule.
    ranked_ties = sorted(
        ((int(row), int(column)) for row, column in tied),
        key=lambda item: (
            (item[0] - (height - 1)) ** 2 + (item[1] - (width - 1)) ** 2,
            item[0], item[1],
        ),
    )
    row_index, column_index = ranked_ties[0]
    shift_row = row_index - (height - 1)
    shift_column = column_index - (width - 1)
    all_scores = np.sort(scores[np.isfinite(scores)].reshape(-1))
    second = float(all_scores[-2]) if all_scores.size > 1 else None
    dx_m = shift_column * x_resolution_m
    dy_m = -shift_row * y_resolution_m
    distance_m = math.hypot(dx_m, dy_m)
    smaller_valid_support = min(
        int(np.count_nonzero(previous_valid)),
        int(np.count_nonzero(current_valid)),
    )
    status = "resolved_unique_peak" if len(ranked_ties) == 1 else "indeterminate"
    return {
        "best_score": _round(best),
        "candidate_shifts": int(finite_positions.shape[0]),
        "indeterminate_reason": (
            None if status == "resolved_unique_peak" else "multiple_equal_best_shifts"
        ),
        "masked_overlap_pixels": int(count[row_index, column_index]),
        "positive_current_pixels": int(positive_current[row_index, column_index]),
        "positive_previous_pixels": int(positive_previous[row_index, column_index]),
        "overlap_fraction_of_smaller_valid_support": _round(
            float(count[row_index, column_index]) / smaller_valid_support
        ),
        "score_margin_to_second": (
            None if second is None else _round(best - second)
        ),
        "second_score": None if second is None else _round(second),
        "shift_column_pixels": shift_column,
        "shift_row_pixels": shift_row,
        "speed_km_h": _round(distance_m / elapsed_seconds * 3.6),
        "status": status,
        "tied_best_shifts": len(ranked_ties),
        "translation_distance_m": _round(distance_m),
        "translation_x_m": _round(dx_m),
        "translation_y_m": _round(dy_m),
    }


def _frame_arrays(evidence: Any) -> tuple[np.ndarray, np.ndarray]:
    values = evidence.normalized_frame.values
    array = np.asarray([
        [np.nan if value is None else float(value) for value in row]
        for row in values
    ], dtype=np.float64)
    valid = np.isfinite(array)
    return np.where(valid, array, 0.0), valid


def _support_centroid(evidence: Any, threshold: float) -> dict[str, Any]:
    array, valid = _frame_arrays(evidence)
    support = np.logical_and(valid, array >= threshold)
    rows, columns = np.nonzero(support)
    if rows.size == 0:
        return {"status": "indeterminate", "reason": "no_support_pixels"}
    grid = evidence.normalized_frame.grid
    x = grid.origin_x_m + (columns.astype(np.float64) + 0.5) * grid.x_resolution_m
    y = grid.origin_y_m - (rows.astype(np.float64) + 0.5) * grid.y_resolution_m
    rates = array[support]
    return {
        "binary_centroid_x_m": _round(float(np.mean(x))),
        "binary_centroid_y_m": _round(float(np.mean(y))),
        "rate_weighted_centroid_x_m": _round(float(np.average(x, weights=rates))),
        "rate_weighted_centroid_y_m": _round(float(np.average(y, weights=rates))),
        "status": "resolved",
        "support_area_km2": int(rows.size * grid.pixel_area_m2 / 1_000_000.0),
        "support_pixels": int(rows.size),
    }


@dataclass(frozen=True, slots=True)
class DwdRoiBaselineReceipt:
    receipt_sha256: str
    document: dict[str, Any]


def compute_dwd_roi_sanity_baselines(
    export: DwdRoiScienceReplayExport,
    *,
    config: DwdRoiBaselineConfig | None = None,
) -> DwdRoiBaselineReceipt:
    """Compute fixed independent references without observing tracker output."""

    if not isinstance(export, DwdRoiScienceReplayExport):
        raise TypeError("export must be DwdRoiScienceReplayExport")
    selected = config or DwdRoiBaselineConfig()
    if len(export.entries) != EXPECTED_FRAME_COUNT:
        raise DwdScienceReplayError("baseline frame count differs")
    first_evidence = load_dwd_roi_science_evidence(export, export.entries[0])
    previous_evidence = first_evidence
    previous, previous_valid = _frame_arrays(previous_evidence)
    frame_accounting: list[dict[str, Any]] = []
    translations: list[dict[str, Any]] = []
    first_support = _support_centroid(
        first_evidence, selected.support_threshold_mm_h
    )
    for index, entry in enumerate(export.entries):
        evidence = first_evidence if index == 0 else load_dwd_roi_science_evidence(
            export, entry
        )
        array, valid = _frame_arrays(evidence)
        frame_accounting.append({
            "evidence_content_sha256": evidence.evidence_content_sha256,
            "finite_pixels": int(np.count_nonzero(valid)),
            "frame_number": index,
            "missing_pixels": int(valid.size - np.count_nonzero(valid)),
            "run_id": entry.canonical_run_id,
            "support_pixels_ge_1_mm_h": int(np.count_nonzero(
                np.logical_and(valid, array >= selected.support_threshold_mm_h)
            )),
            "valid_at": entry.valid_at,
        })
        if index:
            translations.append({
                "current_evidence_content_sha256": evidence.evidence_content_sha256,
                "current_frame_number": index,
                "current_run_id": entry.canonical_run_id,
                "elapsed_seconds": entry.valid_at - export.entries[index - 1].valid_at,
                "previous_evidence_content_sha256": (
                    previous_evidence.evidence_content_sha256
                ),
                "previous_frame_number": index - 1,
                "previous_run_id": export.entries[index - 1].canonical_run_id,
                "translation": masked_translation_reference(
                    previous,
                    array,
                    previous_valid,
                    valid,
                    x_resolution_m=evidence.normalized_frame.grid.x_resolution_m,
                    y_resolution_m=evidence.normalized_frame.grid.y_resolution_m,
                    elapsed_seconds=entry.valid_at - export.entries[index - 1].valid_at,
                    config=selected,
                ),
            })
        previous_evidence = evidence
        previous, previous_valid = array, valid
    last_evidence = previous_evidence
    last_support = _support_centroid(
        last_evidence, selected.support_threshold_mm_h
    )
    elapsed = export.entries[-1].valid_at - export.entries[0].valid_at
    first_last: dict[str, Any] = {
        "elapsed_seconds": elapsed,
        "first": first_support,
        "first_evidence_content_sha256": first_evidence.evidence_content_sha256,
        "last": last_support,
        "last_evidence_content_sha256": last_evidence.evidence_content_sha256,
    }
    if first_support["status"] == last_support["status"] == "resolved":
        dx = last_support["binary_centroid_x_m"] - first_support["binary_centroid_x_m"]
        dy = last_support["binary_centroid_y_m"] - first_support["binary_centroid_y_m"]
        distance = math.hypot(dx, dy)
        first_last.update({
            "binary_displacement_distance_m": _round(distance),
            "binary_displacement_x_m": _round(dx),
            "binary_displacement_y_m": _round(dy),
            "binary_reference_speed_km_h": _round(distance / elapsed * 3.6),
            "status": "resolved_reference_not_truth",
        })
    else:
        first_last.update({
            "reason": "first_or_last_support_absent",
            "status": "indeterminate",
        })
    document = {
        "admission_granted": False,
        "config": selected.to_mapping(),
        "config_sha256": selected.fingerprint,
        "contract": DWD_ROI_BASELINE_CONTRACT,
        "first_last_support_reference": first_last,
        "forecast_leads_used": False,
        "frame_accounting": frame_accounting,
        "interval_count": len(translations),
        "masked_interval_translation_references": translations,
        "missing_values_treated_as_dry": False,
        "sequence_id": export.sequence_id,
        "sequence_manifest_sha256": export.sequence_manifest_sha256,
        "source_manifest_sha256": export.source_manifest_sha256,
        "target_grid_sha256": export.target_grid_sha256,
        "tracker_outputs_consulted": False,
        "tracker_parameters_tuned": False,
        "truth_claimed": False,
    }
    body = _canonical_json_bytes(document)
    if len(body) > 1024 * 1024:
        raise DwdScienceReplayError("baseline receipt exceeds byte cap")
    return DwdRoiBaselineReceipt(
        receipt_sha256=hashlib.sha256(body).hexdigest(),
        document=document,
    )


__all__ = [
    "DWD_ROI_BASELINE_CONTRACT",
    "DwdRoiBaselineConfig",
    "DwdRoiBaselineReceipt",
    "compute_dwd_roi_sanity_baselines",
    "masked_translation_reference",
]
