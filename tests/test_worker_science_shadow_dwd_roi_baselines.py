from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from storm_tracker_worker.science_shadow_dwd_roi_baselines import (
    DwdRoiBaselineConfig,
    compute_dwd_roi_sanity_baselines,
    masked_translation_reference,
)
from storm_tracker_worker.science_shadow_dwd_roi_replay import (
    EXPECTED_SEQUENCE_MANIFEST_SHA256,
    load_dwd_roi_science_replay_export,
)


REPOSITORY = Path(__file__).resolve().parents[1]
EXPORT_ROOT = REPOSITORY / ".analysis" / "dwd-rv-germany-roi-2km-36-export-v1"
SEQUENCE_MANIFEST = (
    EXPORT_ROOT / "manifests" / f"{EXPECTED_SEQUENCE_MANIFEST_SHA256}.json"
)


def _synthetic_config() -> DwdRoiBaselineConfig:
    return DwdRoiBaselineConfig(
        minimum_overlap_pixels=64,
        minimum_positive_pixels_each=4,
        max_frame_pixels=1_000,
        max_correlation_elements=2_000,
    )


def test_masked_translation_recovers_known_shift_without_distance_gate() -> None:
    previous = np.zeros((20, 20), dtype=np.float64)
    previous[5:10, 6:11] = np.arange(25, dtype=np.float64).reshape(5, 5) + 1
    current = np.zeros_like(previous)
    current[7:12, 9:14] = previous[5:10, 6:11]
    valid = np.ones(previous.shape, dtype=np.bool_)

    result = masked_translation_reference(
        previous,
        current,
        valid,
        valid,
        x_resolution_m=2_000.0,
        y_resolution_m=2_000.0,
        elapsed_seconds=300.0,
        config=_synthetic_config(),
    )

    assert result["status"] == "resolved_unique_peak"
    assert result["shift_column_pixels"] == 3
    assert result["shift_row_pixels"] == 2
    assert result["translation_x_m"] == 6_000.0
    assert result["translation_y_m"] == -4_000.0
    assert result["best_score"] == 1.0


def test_invalid_and_missing_values_are_excluded_not_zero_filled() -> None:
    previous = np.zeros((20, 20), dtype=np.float64)
    current = np.zeros_like(previous)
    previous[5:10, 5:10] = np.arange(25).reshape(5, 5) + 1
    current[5:10, 6:11] = previous[5:10, 5:10]
    previous_valid = np.ones(previous.shape, dtype=np.bool_)
    current_valid = np.ones(current.shape, dtype=np.bool_)
    previous_valid[0:4, :] = False
    current_valid[0:4, :] = False
    previous[~previous_valid] = 1e9
    current[~current_valid] = -1e9

    result = masked_translation_reference(
        previous,
        current,
        previous_valid,
        current_valid,
        x_resolution_m=2_000.0,
        y_resolution_m=2_000.0,
        elapsed_seconds=300.0,
        config=_synthetic_config(),
    )
    assert result["status"] == "resolved_unique_peak"
    assert result["shift_column_pixels"] == 1
    assert result["shift_row_pixels"] == 0


def test_constant_or_absent_signal_is_explicitly_indeterminate() -> None:
    values = np.ones((20, 20), dtype=np.float64)
    valid = np.ones(values.shape, dtype=np.bool_)
    result = masked_translation_reference(
        values,
        values,
        valid,
        valid,
        x_resolution_m=2_000.0,
        y_resolution_m=2_000.0,
        elapsed_seconds=300.0,
        config=_synthetic_config(),
    )
    assert result == {
        "candidate_shifts": 0,
        "indeterminate_reason": "insufficient_masked_overlap_or_variance",
        "status": "indeterminate",
    }


def test_exact_roi_reference_is_deterministic_and_never_truth() -> None:
    if not SEQUENCE_MANIFEST.is_file():
        pytest.skip("immutable DWD Germany ROI export is not present")
    export = load_dwd_roi_science_replay_export(
        EXPORT_ROOT.resolve(), SEQUENCE_MANIFEST.resolve()
    )
    first = compute_dwd_roi_sanity_baselines(export)
    second = compute_dwd_roi_sanity_baselines(export)

    assert first == second
    document = first.document
    assert document["truth_claimed"] is False
    assert document["admission_granted"] is False
    assert document["tracker_outputs_consulted"] is False
    assert document["tracker_parameters_tuned"] is False
    assert document["missing_values_treated_as_dry"] is False
    assert document["interval_count"] == 35
    reference = document["first_last_support_reference"]
    assert reference["elapsed_seconds"] == 10_500.0
    assert reference["first"]["support_pixels"] == 229
    assert reference["last"]["support_pixels"] == 70
    assert reference["binary_displacement_x_m"] == pytest.approx(-228_080.723643)
    assert reference["binary_displacement_y_m"] == pytest.approx(359_323.518403)
    intervals = document["masked_interval_translation_references"]
    assert len(intervals) == 35
    assert all(item["translation"]["status"] == "resolved_unique_peak" for item in intervals)
    assert sum(
        item["translation"]["shift_column_pixels"] == 1
        and item["translation"]["shift_row_pixels"] == 0
        for item in intervals
    ) == 31
    assert min(
        item["translation"]["overlap_fraction_of_smaller_valid_support"]
        for item in intervals
    ) < 0.05
    assert max(item["translation"]["speed_km_h"] for item in intervals) > 2_600

