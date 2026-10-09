from __future__ import annotations

import ast
import math
from pathlib import Path
import random

import pytest

from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid
from storm_tracker_worker.tracking_core.engine.shared_system_translation import (
    BinaryRainShieldFootprintFrame,
    HARD_MAX_GRID_PIXELS,
    HARD_MAX_NONZERO_SHIFT_CANDIDATES,
    HARD_MAX_PAIR_CONTRIBUTIONS,
    HARD_MAX_SHIFT_CANDIDATES,
    HARD_MAX_SUPPORT_PIXELS_PER_FRAME,
    SharedSystemTranslationConfig,
    estimate_shared_system_translation,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def _grid(
    *,
    width: int = 9,
    height: int = 8,
    x_resolution_m: float = 1_000.0,
    y_resolution_m: float = 2_000.0,
    grid_id: str = "translation-test-grid",
) -> AnalysisGrid:
    return AnalysisGrid(
        grid_id=grid_id,
        crs="EPSG:3035",
        origin_x_m=100_000.0,
        origin_y_m=900_000.0,
        x_resolution_m=x_resolution_m,
        y_resolution_m=y_resolution_m,
        width=width,
        height=height,
    )


def _frame(
    frame_id: str,
    valid_at: float,
    footprints: tuple[tuple[tuple[int, int], ...], ...],
    *,
    grid: AnalysisGrid | None = None,
) -> BinaryRainShieldFootprintFrame:
    return BinaryRainShieldFootprintFrame(
        frame_id=frame_id,
        valid_at=valid_at,
        grid=grid or _grid(),
        rain_shield_footprints=footprints,
    )


def _support(frame: BinaryRainShieldFootprintFrame) -> set[tuple[int, int]]:
    return set(frame.support_pixels)


def _brute_overlap(
    previous: set[tuple[int, int]],
    current: set[tuple[int, int]],
    *,
    row_shift: int,
    column_shift: int,
) -> int:
    shifted = {
        (row + row_shift, column + column_shift)
        for row, column in previous
    }
    return len(shifted & current)


def test_known_shared_translation_is_assignment_independent_and_exact() -> None:
    previous = _frame(
        "f0",
        1_000.0,
        (
            ((1, 1), (1, 2), (2, 1)),
            ((4, 5), (5, 5)),
        ),
    )
    current = _frame(
        "f1",
        1_300.0,
        (
            ((2, 3), (2, 4), (3, 3)),
            ((5, 7), (6, 7)),
        ),
    )

    result = estimate_shared_system_translation(previous, current)

    assert result.status == "resolved_unique_peak"
    assert result.unique_peak is True
    assert result.peak_tie_count == 1
    assert result.uncertainty_reason_codes == ()
    assert result.indeterminate_reason_codes == ()
    assert result.selected_candidate is not None
    selected = result.selected_candidate
    assert (selected.row_shift_pixels, selected.column_shift_pixels) == (1, 2)
    assert selected.displacement_x_m == 2_000.0
    assert selected.displacement_y_m == -2_000.0
    assert selected.displacement_distance_m == pytest.approx(math.sqrt(8_000_000.0))
    assert selected.interval_speed_kmh == pytest.approx(
        math.sqrt(8_000_000.0) / 300.0 * 3.6
    )
    assert selected.overlap_pixels == 5
    assert selected.comparison_window_pixels == 49
    assert selected.previous_support_inside_grid == 5
    assert selected.current_support_inside_comparison_window == 5
    assert selected.previous_only_inside_window == 0
    assert selected.current_only_inside_window == 0
    assert selected.both_dry_inside_window == 44
    assert selected.full_support_union_pixels == 5
    assert selected.jaccard_full_support == 1.0
    assert result.runner_up_candidate is not None
    assert result.peak_margin_pixels == (
        selected.overlap_pixels - result.runner_up_candidate.overlap_pixels
    )

    accounting = result.accounting
    assert accounting.grid_pixels == 72
    assert accounting.previous_footprint_objects == 2
    assert accounting.current_footprint_objects == 2
    assert accounting.previous_support_pixels == 5
    assert accounting.current_support_pixels == 5
    assert accounting.previous_background_pixels == 67
    assert accounting.current_background_pixels == 67
    assert accounting.full_shift_candidates == 255
    assert accounting.shift_candidates_evaluated == 255
    assert accounting.pair_contributions_expected == 25
    assert accounting.pair_contributions_evaluated == 25
    assert accounting.overlap_histogram_mass == 25
    assert accounting.search_complete is True
    assert result.assignment_inputs_used is False
    assert result.speed_or_distance_gate_applied is False
    assert result.meteorological_admission_granted is False
    assert result.operational_snapshot_claim_generated is False


def test_input_order_is_canonical_and_result_is_deterministic() -> None:
    first = _frame(
        "f0",
        10.0,
        (((4, 5), (5, 5)), ((2, 1), (1, 2), (1, 1))),
    )
    first_reordered = _frame(
        "f0",
        10.0,
        (((1, 1), (1, 2), (2, 1)), ((5, 5), (4, 5))),
    )
    second = _frame(
        "f1",
        310.0,
        (((6, 7), (5, 7)), ((3, 3), (2, 4), (2, 3))),
    )
    second_reordered = _frame(
        "f1",
        310.0,
        (((2, 3), (2, 4), (3, 3)), ((5, 7), (6, 7))),
    )

    left = estimate_shared_system_translation(first, second)
    right = estimate_shared_system_translation(first_reordered, second_reordered)

    assert first.fingerprint == first_reordered.fingerprint
    assert second.fingerprint == second_reordered.fingerprint
    assert left == right
    assert left.to_mapping() == right.to_mapping()
    assert left.result_sha256 == right.result_sha256


def test_non_circular_boundary_shift_preserves_clipped_support_accounting() -> None:
    grid = _grid(width=5, height=3, x_resolution_m=1_000.0, y_resolution_m=1_000.0)
    previous = _frame(
        "edge-0",
        0.0,
        (((0, 0), (1, 0), (2, 1)),),
        grid=grid,
    )
    current = _frame(
        "edge-1",
        300.0,
        (((0, 4), (1, 4)),),
        grid=grid,
    )

    result = estimate_shared_system_translation(previous, current)

    assert result.status == "estimated_with_uncertainty"
    assert result.uncertainty_reason_codes == ("peak_on_full_search_boundary",)
    assert result.unique_peak is True
    assert result.selected_candidate is not None
    selected = result.selected_candidate
    assert (selected.row_shift_pixels, selected.column_shift_pixels) == (0, 4)
    assert selected.overlap_pixels == 2
    assert selected.previous_support_inside_grid == 2
    assert selected.previous_support_outside_grid == 1
    assert selected.current_support_inside_comparison_window == 2
    assert selected.current_support_outside_comparison_window == 0
    assert selected.full_support_union_pixels == 3
    assert selected.jaccard_full_support == pytest.approx(2.0 / 3.0)
    assert selected.column_shift_pixels != -1  # no circular wraparound


def test_equal_peaks_keep_bounded_deterministic_receipt_candidates() -> None:
    grid = _grid(width=5, height=5, x_resolution_m=1_000.0, y_resolution_m=1_000.0)
    previous = _frame("tie-0", 0.0, (((2, 2),),), grid=grid)
    current = _frame("tie-1", 60.0, (((2, 1), (2, 3)),), grid=grid)

    result = estimate_shared_system_translation(previous, current)

    assert result.status == "estimated_with_uncertainty"
    assert result.unique_peak is False
    assert result.peak_tie_count == 2
    assert result.peak_margin_pixels == 0
    assert result.uncertainty_reason_codes == ("peak_not_unique",)
    assert result.indeterminate_reason_codes == ()
    assert result.selected_candidate is not None
    assert result.runner_up_candidate is not None
    assert (
        result.selected_candidate.row_shift_pixels,
        result.selected_candidate.column_shift_pixels,
    ) == (0, -1)
    assert (
        result.runner_up_candidate.row_shift_pixels,
        result.runner_up_candidate.column_shift_pixels,
    ) == (0, 1)


def test_low_overlap_keeps_the_unique_point_shift_with_uncertainty() -> None:
    grid = _grid(width=7, height=7, x_resolution_m=1_000.0, y_resolution_m=1_000.0)
    previous = _frame(
        "deform-0",
        0.0,
        (((1, 1), (1, 2), (2, 1), (3, 3)),),
        grid=grid,
    )
    current = _frame(
        "deform-1",
        300.0,
        (((2, 2), (2, 3), (0, 6), (6, 0)),),
        grid=grid,
    )
    config = SharedSystemTranslationConfig(
        low_overlap_uncertainty_fraction=0.75
    )

    result = estimate_shared_system_translation(previous, current, config)

    assert result.status == "estimated_with_uncertainty"
    assert result.unique_peak is True
    assert result.selected_candidate is not None
    assert result.selected_candidate.overlap_pixels == 2
    assert result.selected_candidate.overlap_fraction_smaller_support == 0.5
    assert result.uncertainty_reason_codes == ("low_overlap_support",)
    assert result.indeterminate_reason_codes == ()


def test_absent_support_is_explicitly_indeterminate() -> None:
    grid = _grid(width=4, height=3)
    previous = _frame("empty-0", 0.0, (), grid=grid)
    current = _frame("empty-1", 300.0, (), grid=grid)

    result = estimate_shared_system_translation(previous, current)

    assert result.status == "indeterminate"
    assert result.selected_candidate is None
    assert result.runner_up_candidate is None
    assert result.uncertainty_reason_codes == ()
    assert set(result.indeterminate_reason_codes) == {
        "previous_rain_shield_support_absent",
        "current_rain_shield_support_absent",
    }
    assert result.accounting.previous_support_pixels == 0
    assert result.accounting.current_support_pixels == 0
    assert result.accounting.previous_background_pixels == 12
    assert result.accounting.current_background_pixels == 12
    assert result.accounting.search_complete is False


@pytest.mark.parametrize(
    ("config", "expected_reason"),
    (
        (SharedSystemTranslationConfig(max_grid_pixels=19), "grid_pixel_capacity_exceeded"),
        (
            SharedSystemTranslationConfig(max_support_pixels_per_frame=1),
            "support_pixel_capacity_exceeded",
        ),
        (
            SharedSystemTranslationConfig(max_shift_candidates=62),
            "full_shift_domain_capacity_exceeded",
        ),
        (
            SharedSystemTranslationConfig(max_pair_contributions=3),
            "pair_contribution_capacity_exceeded",
        ),
    ),
)
def test_static_capacity_is_fail_closed_without_a_partial_peak(
    config: SharedSystemTranslationConfig,
    expected_reason: str,
) -> None:
    grid = _grid(width=5, height=4)
    previous = _frame("cap-0", 0.0, (((0, 0), (1, 1)),), grid=grid)
    current = _frame("cap-1", 60.0, (((2, 2), (3, 3)),), grid=grid)

    result = estimate_shared_system_translation(previous, current, config)

    assert result.status == "indeterminate"
    assert expected_reason in result.indeterminate_reason_codes
    assert result.selected_candidate is None
    assert result.runner_up_candidate is None
    assert result.accounting.pair_contributions_evaluated == 0
    assert result.accounting.overlap_histogram_mass == 0
    assert result.accounting.shift_candidates_evaluated == 0
    assert result.accounting.search_complete is False


def test_dynamic_histogram_capacity_is_fail_closed_and_accounted() -> None:
    grid = _grid(width=5, height=4)
    previous = _frame("dyn-0", 0.0, (((0, 0), (1, 1)),), grid=grid)
    current = _frame("dyn-1", 60.0, (((2, 2), (3, 4)),), grid=grid)
    config = SharedSystemTranslationConfig(max_nonzero_shift_candidates=1)

    result = estimate_shared_system_translation(previous, current, config)

    assert result.status == "indeterminate"
    assert result.indeterminate_reason_codes == (
        "nonzero_shift_candidate_capacity_exceeded",
    )
    assert result.selected_candidate is None
    assert result.accounting.pair_contributions_expected == 4
    assert result.accounting.pair_contributions_evaluated == 1
    assert result.accounting.overlap_histogram_mass == 1
    assert result.accounting.nonzero_shift_candidates_discovered == 1
    assert result.accounting.search_complete is False


def test_extreme_finite_speed_is_reported_without_a_gate() -> None:
    grid = _grid(width=101, height=2, x_resolution_m=1_000.0, y_resolution_m=1_000.0)
    previous = _frame("fast-0", 0.0, (((0, 0),),), grid=grid)
    current = _frame("fast-1", 1.0, (((0, 100),),), grid=grid)

    result = estimate_shared_system_translation(previous, current)

    assert result.selected_candidate is not None
    assert result.selected_candidate.column_shift_pixels == 100
    assert result.selected_candidate.interval_speed_kmh == 360_000.0
    assert result.speed_or_distance_gate_applied is False
    assert result.status == "estimated_with_uncertainty"
    assert "peak_on_full_search_boundary" in result.uncertainty_reason_codes


def test_measured_dwd_roi_support_envelope_fits_default_compute_caps() -> None:
    """Guard the measured 36-frame ROI envelope, not a live admission claim.

    Source: sealed sequence manifest
    aaa9045acb6407c3ca714563c6293f5a66f9dd284d1fb00b508be4d1d991e7dc.
    Its 35 adjacent intervals contain at most 56,640 ordered support pairs.
    """

    config = SharedSystemTranslationConfig()
    dwd_roi_grid_pixels = 425 * 350
    dwd_roi_full_shift_domain = (2 * 425 - 1) * (2 * 350 - 1)
    measured_max_support_pixels = 240
    measured_max_adjacent_pair_contributions = 56_640
    measured_max_distinct_nonzero_shifts = 7_553

    assert dwd_roi_grid_pixels == 148_750
    assert dwd_roi_full_shift_domain == 593_451
    assert dwd_roi_grid_pixels <= config.max_grid_pixels
    assert dwd_roi_full_shift_domain <= config.max_shift_candidates
    assert measured_max_support_pixels <= config.max_support_pixels_per_frame
    assert measured_max_adjacent_pair_contributions <= config.max_pair_contributions
    assert (
        measured_max_distinct_nonzero_shifts
        <= config.max_nonzero_shift_candidates
    )


@pytest.mark.parametrize(
    ("name", "maximum"),
    (
        ("max_grid_pixels", HARD_MAX_GRID_PIXELS),
        ("max_support_pixels_per_frame", HARD_MAX_SUPPORT_PIXELS_PER_FRAME),
        ("max_pair_contributions", HARD_MAX_PAIR_CONTRIBUTIONS),
        ("max_shift_candidates", HARD_MAX_SHIFT_CANDIDATES),
        ("max_nonzero_shift_candidates", HARD_MAX_NONZERO_SHIFT_CANDIDATES),
    ),
)
def test_config_hard_capacity_boundaries(name: str, maximum: int) -> None:
    assert getattr(SharedSystemTranslationConfig(**{name: maximum}), name) == maximum
    with pytest.raises(ValueError):
        SharedSystemTranslationConfig(**{name: maximum + 1})


def test_frame_and_pair_contract_validation_is_fail_closed() -> None:
    grid = _grid(width=5, height=4)
    with pytest.raises(ValueError, match="duplicate"):
        _frame("bad", 0.0, (((0, 0), (0, 0)),), grid=grid)
    with pytest.raises(ValueError, match="overlap"):
        _frame("bad", 0.0, (((0, 0),), ((0, 0),)), grid=grid)
    with pytest.raises(ValueError, match="outside"):
        _frame("bad", 0.0, (((4, 0),),), grid=grid)

    previous = _frame("same", 10.0, (((0, 0),),), grid=grid)
    with pytest.raises(ValueError, match="advance"):
        estimate_shared_system_translation(
            previous,
            _frame("later", 10.0, (((0, 1),),), grid=grid),
        )
    with pytest.raises(ValueError, match="unique IDs"):
        estimate_shared_system_translation(
            previous,
            _frame("same", 20.0, (((0, 1),),), grid=grid),
        )
    changed_geometry = _grid(width=6, height=4, grid_id=grid.grid_id)
    with pytest.raises(ValueError):
        estimate_shared_system_translation(
            previous,
            _frame("later", 20.0, (((0, 1),),), grid=changed_geometry),
        )


def test_random_small_grids_match_independent_full_domain_oracle() -> None:
    rng = random.Random(0x5A17)
    for case in range(40):
        height = rng.randint(2, 5)
        width = rng.randint(2, 6)
        grid = _grid(
            width=width,
            height=height,
            x_resolution_m=750.0,
            y_resolution_m=1_250.0,
            grid_id=f"random-{case}",
        )
        all_pixels = [(row, column) for row in range(height) for column in range(width)]
        previous_pixels = tuple(sorted(rng.sample(all_pixels, rng.randint(1, min(6, len(all_pixels))))))
        current_pixels = tuple(sorted(rng.sample(all_pixels, rng.randint(1, min(6, len(all_pixels))))))
        previous = _frame(f"r{case}-0", 0.0, (previous_pixels,), grid=grid)
        current = _frame(f"r{case}-1", 300.0, (current_pixels,), grid=grid)

        result = estimate_shared_system_translation(previous, current)
        oracle = []
        for row_shift in range(-(height - 1), height):
            for column_shift in range(-(width - 1), width):
                overlap = _brute_overlap(
                    set(previous_pixels),
                    set(current_pixels),
                    row_shift=row_shift,
                    column_shift=column_shift,
                )
                distance_squared = (
                    (column_shift * grid.x_resolution_m) ** 2
                    + (row_shift * grid.y_resolution_m) ** 2
                )
                oracle.append(
                    ((-overlap, distance_squared, row_shift, column_shift), row_shift, column_shift, overlap)
                )
        oracle.sort()
        peak = max(item[3] for item in oracle)
        ties = sum(item[3] == peak for item in oracle)

        assert result.selected_candidate is not None
        assert (
            result.selected_candidate.row_shift_pixels,
            result.selected_candidate.column_shift_pixels,
            result.selected_candidate.overlap_pixels,
        ) == (oracle[0][1], oracle[0][2], oracle[0][3])
        assert result.peak_tie_count == ties
        assert result.unique_peak == (ties == 1)
        assert result.accounting.full_shift_candidates == len(oracle)
        assert result.accounting.shift_candidates_evaluated == len(oracle)
        assert result.accounting.overlap_histogram_mass == (
            len(previous_pixels) * len(current_pixels)
        )
        assert sum(item[3] for item in oracle) == (
            len(previous_pixels) * len(current_pixels)
        )


def test_module_is_pure_and_wired_only_to_shadow_celltrack_code() -> None:
    module_path = (
        REPO_ROOT
        / "storm_tracker_worker"
        / "tracking_core"
        / "engine"
        / "shared_system_translation.py"
    )
    source = module_path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        (node.module or "")
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    }
    assert imports <= {
        "__future__",
        "dataclasses",
        "hashlib",
        "json",
        "math",
        "typing",
        "analysis_grid",
    }
    forbidden_tokens = (
        "cell_track_engine",
        "association_features",
        "custom_components",
        "mqtt",
        "requests",
        "service",
        "socket",
        "urllib",
        "weathersystem",
    )
    lowered = source.lower()
    assert all(token not in lowered for token in forbidden_tokens)

    unwired_files = [
        REPO_ROOT / "storm_tracker_worker" / "service.py",
        REPO_ROOT
        / "storm_tracker_worker"
        / "tracking_core"
        / "engine"
        / "association_features.py",
    ]
    unwired_files.extend(
        (REPO_ROOT / "custom_components" / "storm_tracker_v3").rglob("*.py")
    )
    for path in unwired_files:
        assert "shared_system_translation" not in path.read_text(
            encoding="utf-8"
        )
    assert "shared_system_translation" in (
        REPO_ROOT
        / "storm_tracker_worker"
        / "tracking_core"
        / "engine"
        / "cell_track_engine.py"
    ).read_text(encoding="utf-8")
