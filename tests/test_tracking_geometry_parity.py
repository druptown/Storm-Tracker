"""Bewaak identieke footprintgeometrie in HA en de externe worker."""

from pathlib import Path

import pytest


def test_worker_polygon_helper_is_byte_identical():
    repository = Path(__file__).resolve().parent.parent
    integration = (
        repository
        / "custom_components"
        / "storm_tracker_v3"
        / "geometry"
        / "polygon.py"
    )
    worker = (
        repository
        / "storm_tracker_worker"
        / "tracking_core"
        / "geometry"
        / "polygon.py"
    )

    assert worker.read_bytes() == integration.read_bytes()


@pytest.mark.parametrize(
    "relative_path",
    (
        "engine/observation.py",
        "engine/storm.py",
        "engine/storm_engine.py",
    ),
)
def test_worker_tracking_geometry_contract_is_byte_identical(relative_path):
    repository = Path(__file__).resolve().parent.parent
    integration = (
        repository
        / "custom_components"
        / "storm_tracker_v3"
        / relative_path
    )
    worker = (
        repository
        / "storm_tracker_worker"
        / "tracking_core"
        / relative_path
    )

    assert worker.read_bytes() == integration.read_bytes()


def test_observation_materializes_immutable_structured_geometry(
    observation_module,
):
    raw = [[[
        [50.9, 3.9], [50.9, 4.3], [51.2, 4.3],
        [51.2, 3.9], [50.9, 3.9],
    ]]]
    observation = observation_module.Observation(
        obs_type=observation_module.ObservationType.RADAR,
        lat=51.0,
        lon=4.1,
        timestamp=1.0,
        footprint_polygons=raw,
    )
    raw[0][0][0][0] = 0.0

    assert isinstance(observation.footprint_polygons, tuple)
    assert isinstance(observation.footprint_polygons[0], tuple)
    assert isinstance(observation.footprint_polygons[0][0], tuple)
    assert observation.footprint_polygons[0][0][0] == (50.9, 3.9)
    assert observation.footprint_points == observation.footprint_polygons[0][0]


def test_worker_and_integration_use_polygon_edges(storm_module):
    from storm_tracker_worker.tracking_core.engine import storm as worker_storm

    footprint = (
        (50.8, 4.1),
        (51.2, 4.1),
        (51.2, 4.2),
        (50.8, 4.2),
    )

    def build(module):
        storm = module.Storm(centroid_lat=51.0, centroid_lon=4.15)
        storm.radar_cells["cell"] = module.RadarCellSnapshot(
            cell_id="cell",
            timestamp=1_000.0,
            lat=51.0,
            lon=4.15,
            intensity=3,
            area_km2=20.0,
            footprint_points=footprint,
        )
        return storm

    integration_result = build(storm_module).closest_radar_point(51.0, 4.0)
    worker_result = build(worker_storm).closest_radar_point(51.0, 4.0)

    assert worker_result == pytest.approx(integration_result)
    assert integration_result[0] == pytest.approx(7.0, abs=0.2)
    assert integration_result[1:] == pytest.approx(
        (51.00004268, 4.1),
        abs=1e-7,
    )
