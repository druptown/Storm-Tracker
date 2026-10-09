"""Contract tests for the bounded real Met Office replay command."""
from __future__ import annotations

from scripts import replay_worker_only_met_office_sequence as replay
from storm_tracker_worker import met_office_native as native
from storm_tracker_worker.worker_only_cell_tracking import MAX_ROI_PIXELS


def test_replay_tiles_cover_the_exact_native_grid_once():
    tiles = replay._tiles()

    assert len(tiles) == 45
    assert sum(tile.grid.width * tile.grid.height for tile in tiles) == (
        native.GRID_ROWS * native.GRID_COLUMNS
    )
    assert max(tile.grid.width * tile.grid.height for tile in tiles) <= (
        MAX_ROI_PIXELS
    )
    assert len({tile.name for tile in tiles}) == len(tiles)
    assert len({tile.region_lineage_id for tile in tiles}) == len(tiles)
    assert all(len(tile.targets) == 9 for tile in tiles)


def test_replay_contract_never_claims_operational_output():
    assert replay.MINIMUM_SEQUENCE_FRAMES >= 2
    assert replay.MAXIMUM_SEQUENCE_FRAMES <= 8
    assert replay.WORKLOAD_ID == "radar.metoffice.uk_rate"
    assert "real-sequence" in replay.CAPTURE_CONTRACT
    assert "replay-report" in replay.REPORT_CONTRACT
