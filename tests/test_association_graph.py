"""Tests for the complete, non-gating CellDetection candidate graph."""
from __future__ import annotations

import math

import pytest

from storm_tracker_worker.tracking_core.engine.association_graph import (
    build_detection_association_graph,
)
from storm_tracker_worker.tracking_core.engine.cell_track import CellDetection


def _cell(
    detection_id: str,
    valid_at: float,
    x_m: float,
    y_m: float,
    *,
    sigma_m: float = 1_000.0,
    grid_id: str = "metric-grid-v1",
    source: str = "test-radar",
) -> CellDetection:
    return CellDetection(
        detection_id=detection_id,
        valid_at=valid_at,
        analysis_grid_id=grid_id,
        x_m=x_m,
        y_m=y_m,
        position_sigma_m=sigma_m,
        source=source,
    )


def test_graph_retains_every_pair_including_extreme_displacement():
    previous = (
        _cell("old-a", 1_000.0, 0.0, 0.0),
        _cell("old-b", 1_000.0, 5_000.0, 0.0),
    )
    current = (
        _cell("new-a", 1_060.0, 1_000.0, 0.0),
        _cell("new-extreme", 1_060.0, 111_000.0, 0.0),
    )

    graph = build_detection_association_graph(
        previous,
        current,
        motion_sigma_m_s=20.0,
    )

    assert graph.is_complete is True
    assert graph.expected_candidate_count == 4
    assert len(graph.candidates) == 4
    extreme = next(
        candidate
        for candidate in graph.candidates
        if candidate.previous_detection_id == "old-a"
        and candidate.current_detection_id == "new-extreme"
    )
    assert extreme.interval_speed_kmh > 6_000.0
    assert extreme.gaussian_geometric_nll > 0.0


def test_graph_ranks_but_does_not_select_or_delete_alternatives():
    previous = (_cell("old", 1_000.0, 0.0, 0.0),)
    current = (
        _cell("far", 1_600.0, 50_000.0, 0.0),
        _cell("near", 1_600.0, 2_000.0, 0.0),
    )

    graph = build_detection_association_graph(
        previous,
        current,
        motion_sigma_m_s=10.0,
    )

    by_id = {
        candidate.current_detection_id: candidate
        for candidate in graph.candidates
    }
    assert set(by_id) == {"near", "far"}
    assert by_id["near"].previous_rank == 1
    assert by_id["far"].previous_rank == 2
    assert by_id["near"].gaussian_geometric_nll < (
        by_id["far"].gaussian_geometric_nll
    )


def test_interval_motion_and_combined_uncertainty_are_exact():
    previous = (_cell("old", 1_000.0, 0.0, 0.0, sigma_m=1_000.0),)
    current = (_cell("new", 1_100.0, 3_000.0, 4_000.0, sigma_m=2_000.0),)

    graph = build_detection_association_graph(
        previous,
        current,
        motion_sigma_m_s=10.0,
    )
    candidate = graph.candidates[0]

    assert candidate.displacement_x_m == 3_000.0
    assert candidate.displacement_y_m == 4_000.0
    assert candidate.displacement_m == 5_000.0
    assert candidate.interval_speed_kmh == 180.0
    assert candidate.combined_position_sigma_m == pytest.approx(
        math.sqrt(1_000.0**2 + 2_000.0**2 + 1_000.0**2)
    )
    assert candidate.standardized_displacement == pytest.approx(
        candidate.displacement_m / candidate.combined_position_sigma_m
    )


def test_graph_is_deterministic_independent_of_input_order():
    previous = (
        _cell("old-b", 1_000.0, 4_000.0, 0.0),
        _cell("old-a", 1_000.0, 0.0, 0.0),
    )
    current = (
        _cell("new-b", 1_600.0, 5_000.0, 0.0),
        _cell("new-a", 1_600.0, 1_000.0, 0.0),
    )

    first = build_detection_association_graph(
        previous,
        current,
        motion_sigma_m_s=10.0,
    )
    second = build_detection_association_graph(
        reversed(previous),
        reversed(current),
        motion_sigma_m_s=10.0,
    )

    assert first == second
    assert first.previous_detection_ids == ("old-a", "old-b")
    assert first.current_detection_ids == ("new-a", "new-b")


def test_provider_handoff_is_retained_when_grid_contract_matches():
    previous = (
        _cell(
            "provider-a:old",
            1_000.0,
            0.0,
            0.0,
            source="provider-a",
        ),
    )
    current = (
        _cell(
            "provider-b:new",
            1_600.0,
            2_000.0,
            0.0,
            sigma_m=5_000.0,
            source="provider-b",
        ),
    )

    graph = build_detection_association_graph(
        previous,
        current,
        motion_sigma_m_s=10.0,
    )

    assert len(graph.candidates) == 1
    assert graph.candidates[0].previous_source == "provider-a"
    assert graph.candidates[0].current_source == "provider-b"


def test_cross_grid_frames_fail_explicitly_instead_of_fake_motion():
    previous = (_cell("old", 1_000.0, 0.0, 0.0, grid_id="grid-a"),)
    current = (_cell("new", 1_600.0, 1_000.0, 0.0, grid_id="grid-b"),)

    with pytest.raises(ValueError, match="hetzelfde analysis grid"):
        build_detection_association_graph(
            previous,
            current,
            motion_sigma_m_s=10.0,
        )


@pytest.mark.parametrize(
    "motion_sigma_m_s",
    (-1.0, float("nan"), float("inf")),
)
def test_motion_uncertainty_must_be_explicit_and_valid(motion_sigma_m_s):
    previous = (_cell("old", 1_000.0, 0.0, 0.0),)
    current = (_cell("new", 1_600.0, 1_000.0, 0.0),)

    with pytest.raises(ValueError, match="motion_sigma_m_s"):
        build_detection_association_graph(
            previous,
            current,
            motion_sigma_m_s=motion_sigma_m_s,
        )


@pytest.mark.parametrize("which", ("previous", "current"))
def test_empty_frame_fails_without_inventing_birth_or_death_prior(which):
    previous = [_cell("old", 1_000.0, 0.0, 0.0)]
    current = [_cell("new", 1_600.0, 1_000.0, 0.0)]
    if which == "previous":
        previous = []
    else:
        current = []

    with pytest.raises(ValueError, match="mag niet leeg"):
        build_detection_association_graph(
            previous,
            current,
            motion_sigma_m_s=10.0,
        )


def test_duplicate_detection_identity_within_frame_fails_closed():
    previous = (
        _cell("duplicate", 1_000.0, 0.0, 0.0),
        _cell("duplicate", 1_000.0, 1_000.0, 0.0),
    )
    current = (_cell("new", 1_600.0, 1_000.0, 0.0),)

    with pytest.raises(ValueError, match="dubbele detection_id"):
        build_detection_association_graph(
            previous,
            current,
            motion_sigma_m_s=10.0,
        )


def test_mixed_timestamps_are_not_silently_treated_as_one_frame():
    previous = (
        _cell("old-a", 1_000.0, 0.0, 0.0),
        _cell("old-b", 1_001.0, 1_000.0, 0.0),
    )
    current = (_cell("new", 1_600.0, 1_000.0, 0.0),)

    with pytest.raises(ValueError, match="meerdere geldigheidstijden"):
        build_detection_association_graph(
            previous,
            current,
            motion_sigma_m_s=10.0,
        )


def test_current_frame_must_be_meteorologically_later():
    previous = (_cell("old", 1_000.0, 0.0, 0.0),)
    current = (_cell("new", 1_000.0, 1_000.0, 0.0),)

    with pytest.raises(ValueError, match="later"):
        build_detection_association_graph(
            previous,
            current,
            motion_sigma_m_s=10.0,
        )
