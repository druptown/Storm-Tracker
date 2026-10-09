import pytest

from storm_tracker_worker.replay import (
    run_celltrack_shadow_replay,
    run_phase3_replay,
)


def test_phase3_replay_is_isolated_and_passes_all_checks():
    result = run_phase3_replay(base_timestamp=1_800_000_000)

    assert result["mode"] == "isolated_replay"
    assert result["operational_state_mutated"] is False
    assert result["passed"] is True
    assert all(result["checks"].values())
    assert result["tracking"]["frame_count"] == 3
    assert result["probe"]["confirmed_count"] == 1
    assert result["calibration"]["comparisons"] == 1


def test_celltrack_shadow_replay_uses_more_than_twenty_frames():
    result = run_celltrack_shadow_replay(frame_count=24)

    assert result["mode"] == "celltrack_shadow_contract_replay"
    assert result["fixture_kind"] == "synthetic_known_identity"
    assert result["operational_state_mutated"] is False
    assert result["global_assignment_validated"] is False
    assert result["real_meteorological_case"] is False
    assert result["frame_count"] == 24
    assert result["candidate_count"] == 23 * 4
    assert result["passed"] is True
    assert all(result["checks"].values())
    assert result["track_speeds_kmh"]["track-a"]["minimum"] == 12.0
    assert result["track_speeds_kmh"]["track-b"]["maximum"] == 6.0
    assert result["max_candidate_speed_kmh"] > 1_000.0


def test_celltrack_shadow_replay_rejects_short_qualification_sequence():
    with pytest.raises(ValueError, match="minstens twintig"):
        run_celltrack_shadow_replay(frame_count=19)
