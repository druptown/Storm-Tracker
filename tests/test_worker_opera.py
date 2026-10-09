"""Parity tests for the external OPERA HDF5 shadow parser."""
from __future__ import annotations

from pathlib import Path

import pytest

from storm_tracker_worker import opera as worker_opera
from storm_tracker_worker.opera import parse_opera_slice


def test_worker_opera_cells_match_integration_parser(
    opera_module, opera_fixture_file
):
    payload = Path(opera_fixture_file["path"]).read_bytes()
    bbox = (3.70, 50.50, 5.30, 51.55)

    local_cells, _timestamp = opera_module._parse_hdf5_slice(payload, bbox)
    worker = parse_opera_slice(payload, bbox)

    assert worker["cells"] == [
        opera_module._serialized_cell(cell) for cell in local_cells
    ]
    donut = [
        cell for cell in worker["cells"]
        if cell["max_dbz"] == 32.0
    ]
    assert len(donut) == 1
    assert donut[0]["footprint_points"] == ()
    assert len(donut[0]["footprint_polygons"]) == 1
    assert len(donut[0]["footprint_polygons"][0]) == 2
    assert worker_opera._topology_stats(
        worker["cells"]
    ) == opera_module._raw_topology_stats(worker["cells"])


def test_worker_opera_topology_limit_fails_closed(monkeypatch):
    polygons = (((
        (50.9, 3.9), (50.9, 4.1), (51.1, 4.1),
        (50.9, 3.9),
    ),),)
    cells = [{
        "footprint_polygons": polygons,
        "parent_footprint_polygons": polygons,
    }]
    monkeypatch.setattr(
        worker_opera,
        "MAX_OPERA_TOPOLOGY_POINTS_PER_ARTIFACT",
        7,
    )

    with pytest.raises(ValueError, match="veiligheidslimiet"):
        worker_opera._topology_stats(cells)
