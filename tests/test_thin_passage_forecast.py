from __future__ import annotations

import numpy as np

from storm_tracker_worker.thin_passage_forecast import (
    AdvectionMotion,
    PassageForecastPolicy,
    PassageTarget,
    advect_labelled_qpe,
)
from storm_tracker_worker.thin_provider_adapters import RadarFieldIdentity, ThinRadarField


def _field(
    labelled: tuple[tuple[int, int], ...],
    *,
    quantity: str = "precipitation_rate",
    unit: str = "mm/h",
    invalid: tuple[tuple[int, int], ...] = (),
) -> ThinRadarField:
    values = np.zeros((3, 7), dtype=float)
    valid = np.ones((3, 7), dtype=bool)
    for row, column in labelled:
        values[row, column] = 7.0
    for row, column in invalid:
        valid[row, column] = False
    return ThinRadarField(
        "radar", "product", 1_000.0, "EPSG:3857",
        -3_500.0, 1_500.0, 1_000.0, 1_000.0, values, valid,
        identity=RadarFieldIdentity(
            quantity=quantity,
            unit=unit,
            quality_kind="source_valid_mask",
            processing_version="test-v1",
            source_composition=("radar",),
            canonical_valid_at=1_000.0,
            received_at=1_001.0,
            available_at=1_002.0,
            grid_id="test-grid",
            content_fingerprint="d" * 64,
        ),
    )


POLICY = PassageForecastPolicy(
    horizon_seconds=400.0,
    step_seconds=100.0,
    wet_threshold_mm_h=0.1,
)
TARGET = PassageTarget("home", 0.0, 0.0, 0.0)
EAST = AdvectionMotion(10.0, 0.0, "candidate_policy_v1")


def test_straight_qpe_motion_has_hand_derived_entry_and_exit_brackets() -> None:
    result = advect_labelled_qpe(
        _field(((1, 1),)), ((1, 1),), EAST, TARGET,
        evaluated_at=1_000.0, policy=POLICY,
    )

    assert len(result) == 1
    passage = result[0]
    assert (passage.entry_bracket.lower_at, passage.entry_bracket.upper_at) == (1_100.0, 1_200.0)
    assert (passage.exit_bracket.lower_at, passage.exit_bracket.upper_at) == (1_200.0, 1_300.0)
    assert passage.left_censored is False
    assert passage.right_censored is False
    assert passage.mean_intensity_mm_h == 7.0
    assert passage.peak_intensity_mm_h == 7.0
    assert passage.method == "advected_labelled_qpe"


def test_delayed_issue_keeps_arrival_at_the_original_radar_clock() -> None:
    result = advect_labelled_qpe(
        _field(((1, 1),)), ((1, 1),), EAST, TARGET,
        evaluated_at=1_100.0, policy=POLICY,
    )

    assert len(result) == 1
    assert (result[0].entry_bracket.lower_at, result[0].entry_bracket.upper_at) == (1_100.0, 1_200.0)
    assert (result[0].exit_bracket.lower_at, result[0].exit_bracket.upper_at) == (1_200.0, 1_300.0)


def test_issue_during_radar_advected_rain_keeps_only_the_remaining_interval() -> None:
    result = advect_labelled_qpe(
        _field(((1, 1),)), ((1, 1),), EAST, TARGET,
        evaluated_at=1_200.0, policy=POLICY,
    )

    assert len(result) == 1
    assert result[0].left_censored is True
    assert result[0].entry_bracket is None
    assert (result[0].exit_bracket.lower_at, result[0].exit_bracket.upper_at) == (1_200.0, 1_300.0)
    assert result[0].mean_intensity_mm_h == 7.0


def test_already_passed_radar_label_does_not_reappear_as_an_upcoming_event() -> None:
    result = advect_labelled_qpe(
        _field(((1, 1),)), ((1, 1),), EAST, TARGET,
        evaluated_at=1_400.0, policy=POLICY,
    )

    assert result == ()


def test_radar_from_after_issue_time_cannot_supply_a_forecast() -> None:
    result = advect_labelled_qpe(
        _field(((1, 1),)), ((1, 1),), EAST, TARGET,
        evaluated_at=900.0, policy=POLICY,
    )

    assert result == ()


def test_initially_wet_stationary_qpe_is_left_and_right_censored() -> None:
    result = advect_labelled_qpe(
        _field(((1, 3),)), ((1, 3),),
        AdvectionMotion(0.0, 0.0, "candidate_policy_v1"), TARGET,
        evaluated_at=1_000.0, policy=POLICY,
    )

    assert len(result) == 1
    assert result[0].left_censored is True
    assert result[0].entry_bracket is None
    assert result[0].right_censored is True
    assert result[0].exit_bracket is None


def test_dry_gap_produces_two_separate_passages() -> None:
    result = advect_labelled_qpe(
        _field(((1, 2), (1, 0))), ((1, 0), (1, 2)), EAST, TARGET,
        evaluated_at=1_000.0, policy=POLICY,
    )

    assert len(result) == 2
    assert result[0].interval_ordinal == 0
    assert result[1].interval_ordinal == 1
    assert result[0].exit_bracket is not None
    assert result[1].entry_bracket is not None


def test_dbzh_echo_cannot_publish_local_rain_rate() -> None:
    result = advect_labelled_qpe(
        _field(((1, 1),), quantity="reflectivity", unit="dBZ"),
        ((1, 1),), EAST, TARGET, evaluated_at=1_000.0, policy=POLICY,
    )

    assert result == ()


def test_nodata_target_support_is_not_treated_as_dry_counterevidence() -> None:
    result = advect_labelled_qpe(
        _field(((1, 1),), invalid=((1, 2),)), ((1, 1),), EAST, TARGET,
        evaluated_at=1_000.0, policy=POLICY,
    )

    assert result == ()
