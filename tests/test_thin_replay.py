from __future__ import annotations

import subprocess
import sys
from dataclasses import replace

import pytest

from storm_tracker_worker.thin_footprint_forecast import forecast_target_passage
from storm_tracker_worker.thin_ground_validation import validate_passages
from storm_tracker_worker.thin_nowcast import StormCell
from storm_tracker_worker.thin_observation import (
    NormalizedObservation,
    ObservationKind,
    ObservationQuality,
)
from storm_tracker_worker.thin_replay import (
    ReplayCell,
    ReplayFrame,
    ReplayRepository,
    run_replay,
    verify_radar_forecasts,
)


def _cell(observed_at: float = 100.0):
    return StormCell(
        cell_id="cell-1", provider_id="dwd_radolan", observed_at=observed_at,
        latitude=0.0, longitude=0.0,
        geometry={"type": "Polygon", "coordinates": [[
            [-0.01, -0.01], [0.01, -0.01], [0.01, 0.01],
            [-0.01, 0.01], [-0.01, -0.01],
        ]]},
        precipitation_rate_mm_h=4.0, reflectivity_dbz=None, radar_level=None,
        velocity_east_kmh=60.0, velocity_north_kmh=0.0,
        speed_kmh=60.0, heading_deg=90.0, motion_ready=True,
    )


def _rain(observed_at: float, value: float = 3.0):
    return NormalizedObservation.create(
        provider_id="netatmo", product_id="ground_observation",
        target_id="home", kind=ObservationKind.RAIN_RATE,
        value=value, unit="mm/h", observed_at=observed_at,
        received_at=observed_at + 1.0, latitude=0.0, longitude=0.10,
        quality=ObservationQuality.GOOD,
        metadata={"station_id": "station-1"},
        maximum_age_seconds=20_000.0,
    )


def _frame():
    return ReplayFrame(
        evaluated_at=102.0,
        target_id="home",
        target_latitude=0.0,
        target_longitude=0.10,
        target_accuracy_km=0.0,
        cells=(_cell(),),
        ground_observations=(_rain(700.0),),
    )


def test_replay_uses_exact_live_passage_and_validation_functions():
    frame = _frame()
    live_passage = forecast_target_passage(
        cell=frame.cells[0], target_id=frame.target_id,
        target_latitude=frame.target_latitude,
        target_longitude=frame.target_longitude,
        evaluated_at=frame.evaluated_at,
        accuracy_buffer_km=frame.target_accuracy_km,
    )
    live_validation = validate_passages(
        (live_passage,), frame.ground_observations, evaluated_at=700.0,
    )

    report = run_replay((frame,), algorithm_version="fixture-v1")

    assert report.passages == (live_passage,)
    assert report.validations == live_validation


def test_replay_order_and_metrics_are_deterministic():
    second = ReplayFrame(
        evaluated_at=50.0, target_id="other", target_latitude=1.0,
        target_longitude=1.0, target_accuracy_km=0.0,
        cells=(), ground_observations=(_rain(50.0),),
    )

    first_run = run_replay((_frame(), second), algorithm_version="fixture-v1")
    second_run = run_replay((second, _frame()), algorithm_version="fixture-v1")

    assert first_run.replay_id == second_run.replay_id
    assert first_run.metrics == second_run.metrics
    metrics = {item.metric_name: item.metric_value for item in first_run.metrics}
    assert metrics["frames_total"] == 2.0
    assert metrics["hit_count"] == 1.0
    assert metrics["miss_count"] == 1.0
    assert "entry_error_p50_seconds" in metrics


def test_one_bad_frame_isolated_from_healthy_replay():
    broken = ReplayFrame(
        evaluated_at=1.0, target_id="broken", target_latitude=0.0,
        target_longitude=0.0, target_accuracy_km=0.0,
        cells=(object(),), ground_observations=(),
    )

    report = run_replay((broken, _frame()), algorithm_version="fixture-v1")

    metrics = {item.metric_name: item.metric_value for item in report.metrics}
    assert metrics["failed_frames"] == 1.0
    assert report.passages


def test_importing_replay_does_not_import_provider_network_adapters():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import storm_tracker_worker.thin_replay; "
            "assert 'storm_tracker_worker.thin_provider_adapters' not in sys.modules",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_repository_applies_exact_date_target_and_provider_bounds():
    class Cursor:
        def __init__(self):
            self.calls = []
            self.responses = iter((
                ((100.0, 51.05, 4.42, 0.005),),
                (),
                (),
            ))

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, parameters):
            self.calls.append((" ".join(sql.split()), parameters))

        def fetchall(self):
            return next(self.responses)

    class Connection:
        def __init__(self):
            self.value = Cursor()

        def cursor(self):
            return self.value

        def close(self):
            pass

    connection = Connection()
    repository = ReplayRepository(lambda **kwargs: connection, {})

    frames = repository.load_frames(
        from_at=100.0, until_at=200.0, target_id="home",
        provider_id="dwd_radolan",
    )

    assert len(frames) == 1
    assert connection.value.calls[0][1] == ("home", 100.0, 200.0)
    assert connection.value.calls[1][1] == (
        -800.0, 200.0, "dwd_radolan", "dwd_radolan",
    )
    assert connection.value.calls[2][1] == (
        "home", 100.0, 200.0, -7100.0, 7400.0, 20_000.0,
    )


def test_replay_loads_ground_data_by_space_and_time_without_target_copies():
    class Cursor:
        def __init__(self):
            self.calls = []

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, parameters):
            self.calls.append((" ".join(sql.split()), parameters))

        def fetchall(self):
            sql = self.calls[-1][0]
            if sql.startswith("SELECT EXTRACT(EPOCH FROM observed_at)"):
                return ((150.0, 51.05, 4.42, 0.005),)
            if sql.startswith("SELECT cell.cell_id"):
                return ()
            if "FROM stv3_station_measurement" in sql:
                return ((
                    "netatmo", "getpublicdata", 2.5, "mm/h",
                    150.0, 151.0, 51.05, 4.42, "good",
                    {"station_id": "station-1", "measurement": "rain_rate"},
                    '{"type":"Point","coordinates":[4.42,51.05]}',
                ),)
            return ((
                "netatmo", "getpublicdata", "home", 2.5, "mm/h",
                150.0, 151.0, 51.05, 4.42, "good",
                {"station_id": "station-1", "measurement": "rain_rate"},
                '{"type":"Point","coordinates":[4.42,51.05]}',
            ),)

    class Connection:
        def __init__(self):
            self.value = Cursor()

        def cursor(self):
            return self.value

        def close(self):
            pass

    connection = Connection()
    repository = ReplayRepository(lambda **kwargs: connection, {})

    frames = repository.load_frames(
        from_at=100.0,
        until_at=200.0,
        target_id="home",
    )

    assert frames[0].ground_observations[0].target_id == "home"
    ground_sql, _ = connection.value.calls[2]
    assert "FROM stv3_station_measurement" in ground_sql
    assert "JOIN stv3_ground_station" in ground_sql
    assert "measurement.target_id" not in ground_sql


def test_repository_radar_cells_accepts_an_unfiltered_provider_query():
    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, parameters):
            normalized = " ".join(sql.split())
            if parameters[2] is None and "%s::text IS NULL" not in normalized:
                raise RuntimeError("postgres_would_raise_indeterminate_datatype")

        def fetchall(self):
            return ()

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            pass

    repository = ReplayRepository(lambda **kwargs: Connection(), {})

    assert repository.load_radar_cells(
        from_at=100.0, until_at=200.0, provider_id=None,
    ) == ()


def _replay_cell(
    *,
    observed_at: float,
    west: float,
    east: float,
    longitude: float,
    velocity_east_kmh: float,
) -> ReplayCell:
    return ReplayCell(
        cell_id="stable-track-1",
        provider_id="opera_radar",
        observed_at=observed_at,
        latitude=51.0,
        longitude=longitude,
        geometry={
            "type": "Polygon",
            "coordinates": [[
                [west, 50.99], [east, 50.99], [east, 51.01],
                [west, 51.01], [west, 50.99],
            ]],
        },
        precipitation_rate_mm_h=4.0,
        reflectivity_dbz=None,
        radar_level=None,
        velocity_east_kmh=velocity_east_kmh,
        velocity_north_kmh=0.0,
        speed_kmh=abs(velocity_east_kmh),
        heading_deg=90.0,
    )


def test_radar_hindcast_compares_translated_footprint_with_later_actual_shape():
    # In ten minutes a 60 km/h cell moves exactly 10 km east. At latitude 51
    # that is 0.142752 degrees longitude (hand-calculated).
    anchor = _replay_cell(
        observed_at=100.0,
        west=4.000000,
        east=4.020000,
        longitude=4.010000,
        velocity_east_kmh=60.0,
    )
    actual = _replay_cell(
        observed_at=700.0,
        west=4.142752,
        east=4.162752,
        longitude=4.152752,
        velocity_east_kmh=60.0,
    )

    results = verify_radar_forecasts(
        (actual, anchor), lead_seconds=(600.0,), match_tolerance_seconds=1.0,
    )

    assert len(results) == 1
    assert results[0].lead_seconds == 600.0
    assert results[0].footprint_iou > 0.999
    assert results[0].centroid_error_km < 0.01


def test_radar_hindcast_scores_projected_convex_pixel_polygons():
    anchor = _replay_cell(
        observed_at=100.0,
        west=4.00,
        east=4.02,
        longitude=4.01,
        velocity_east_kmh=0.0,
    )
    projected_geometry = {
        "type": "Polygon",
        "coordinates": [[
            [4.000, 51.000], [4.010, 51.012], [4.022, 51.002],
            [4.012, 50.990], [4.000, 51.000],
        ]],
    }
    anchor = ReplayCell(
        **{
            field: getattr(anchor, field)
            for field in anchor.__dataclass_fields__
            if field != "geometry"
        },
        geometry=projected_geometry,
    )
    actual = ReplayCell(
        **{
            field: getattr(anchor, field)
            for field in anchor.__dataclass_fields__
            if field not in {"geometry", "observed_at"}
        },
        geometry=projected_geometry,
        observed_at=400.0,
    )

    results = verify_radar_forecasts(
        (anchor, actual), lead_seconds=(300.0,), match_tolerance_seconds=1.0,
    )

    assert len(results) == 1
    assert results[0].footprint_iou == 1.0


def test_radar_hindcast_does_not_compare_a_frame_outside_lead_tolerance():
    anchor = _replay_cell(
        observed_at=100.0,
        west=4.00,
        east=4.02,
        longitude=4.01,
        velocity_east_kmh=0.0,
    )
    too_late = _replay_cell(
        observed_at=711.0,
        west=4.00,
        east=4.02,
        longitude=4.01,
        velocity_east_kmh=0.0,
    )

    assert verify_radar_forecasts(
        (anchor, too_late),
        lead_seconds=(600.0,),
        match_tolerance_seconds=10.0,
    ) == ()


def test_radar_hindcast_matches_the_later_shape_when_the_tracker_id_changes():
    anchor = _replay_cell(
        observed_at=100.0,
        west=4.00,
        east=4.02,
        longitude=4.01,
        velocity_east_kmh=0.0,
    )
    actual = ReplayCell(
        cell_id="new-tracker-id",
        provider_id=anchor.provider_id,
        observed_at=400.0,
        latitude=anchor.latitude,
        longitude=anchor.longitude,
        geometry=anchor.geometry,
        precipitation_rate_mm_h=anchor.precipitation_rate_mm_h,
        reflectivity_dbz=anchor.reflectivity_dbz,
        radar_level=anchor.radar_level,
        velocity_east_kmh=anchor.velocity_east_kmh,
        velocity_north_kmh=anchor.velocity_north_kmh,
        speed_kmh=anchor.speed_kmh,
        heading_deg=anchor.heading_deg,
    )

    results = verify_radar_forecasts(
        (anchor, actual), lead_seconds=(300.0,), match_tolerance_seconds=1.0,
    )

    assert len(results) == 1
    assert results[0].cell_id == anchor.cell_id
    assert results[0].actual_cell_id == actual.cell_id
    assert results[0].footprint_iou == 1.0


def test_replay_reports_radar_accuracy_per_forecast_lead():
    anchor = _replay_cell(
        observed_at=100.0,
        west=4.00,
        east=4.02,
        longitude=4.01,
        velocity_east_kmh=0.0,
    )
    actual = _replay_cell(
        observed_at=400.0,
        west=4.00,
        east=4.02,
        longitude=4.01,
        velocity_east_kmh=0.0,
    )

    report = run_replay(
        (),
        algorithm_version="fixture-v2",
        radar_cells=(actual, anchor),
        radar_lead_seconds=(300.0,),
        radar_match_tolerance_seconds=1.0,
    )

    assert len(report.radar_verifications) == 1
    metrics = {
        (item.metric_name, item.dimension.get("lead_seconds")): item.metric_value
        for item in report.metrics
    }
    assert metrics[("radar_matched_count", 300.0)] == 1.0
    assert metrics[("radar_footprint_iou_mean", 300.0)] == 1.0
    assert metrics[("radar_centroid_error_p90_km", 300.0)] == 0.0


def test_radar_replay_handles_holes_and_concave_shapes():
    cell = _replay_cell(observed_at=100, west=4, east=4.02,
                        longitude=4.01, velocity_east_kmh=0)
    for geometry in (
        {"type": "Polygon", "coordinates": [
            [[4, 51], [4.04, 51], [4.04, 51.04], [4, 51.04], [4, 51]],
            [[4.01, 51.01], [4.01, 51.03], [4.03, 51.03],
             [4.03, 51.01], [4.01, 51.01]],
        ]},
        {"type": "Polygon", "coordinates": [[
            [4, 51], [4.04, 51], [4.04, 51.02], [4.02, 51.02],
            [4.02, 51.04], [4, 51.04], [4, 51],
        ]]},
    ):
        anchor = replace(cell, geometry=geometry)
        actual = replace(anchor, observed_at=400)
        results = verify_radar_forecasts((anchor, actual), lead_seconds=(300,))
        assert len(results) == 1
        assert results[0].footprint_iou == pytest.approx(1)


def test_radar_replay_counts_unmatched_and_missing_frames_without_inflating_score():
    anchor = _replay_cell(observed_at=100, west=4, east=4.02,
                        longitude=4.01, velocity_east_kmh=0)
    actual = replace(anchor, observed_at=400, longitude=6.01,
                     geometry={"type": "Polygon", "coordinates": [[
                         [6, 50.99], [6.02, 50.99], [6.02, 51.01],
                         [6, 51.01], [6, 50.99],
                     ]]})
    report = run_replay((), algorithm_version="test",
                        radar_cells=(anchor, actual), radar_lead_seconds=(300,))
    metrics = {item.metric_name: item.metric_value for item in report.metrics
               if item.dimension.get("provider_id") == "opera_radar"}
    assert metrics["radar_forecast_cell_count"] == 1
    assert metrics["radar_unmatched_forecast_count"] == 1
    assert metrics["radar_unmatched_actual_count"] == 1
    assert metrics["radar_missing_future_frame_count"] == 1
    assert metrics["radar_frame_iou_mean"] == 0
    assert metrics["radar_persistence_frame_iou_mean"] == 0


def test_radar_replay_measures_advection_against_stationary_baseline():
    anchor = _replay_cell(observed_at=100, west=4, east=4.02,
                        longitude=4.01, velocity_east_kmh=60)
    actual = _replay_cell(observed_at=700, west=4.1427430587, east=4.1627430587,
                        longitude=4.1527430587, velocity_east_kmh=-500)
    # The actual frame's later velocity must never affect the earlier forecast.
    report = run_replay((), algorithm_version="test",
                        radar_cells=(anchor, actual), radar_lead_seconds=(600,))
    metrics = {item.metric_name: item.metric_value for item in report.metrics
               if item.dimension.get("provider_id") == "opera_radar"}
    assert metrics["radar_frame_iou_mean"] > 0.999
    assert metrics["radar_persistence_frame_iou_mean"] == 0


def test_radar_replay_counts_invalid_geometry_and_excludes_frame_score():
    anchor = _replay_cell(observed_at=100, west=4, east=4.02,
                        longitude=4.01, velocity_east_kmh=0)
    invalid = replace(anchor, cell_id="broken", geometry={"type": "Polygon",
        "coordinates": [[[4, 51], [4.02, 51.02], [4, 51.02], [4.02, 51], [4, 51]]]})
    actual = replace(anchor, observed_at=400)
    report = run_replay((), algorithm_version="test", radar_lead_seconds=(300,),
                        radar_cells=(invalid, anchor, actual))
    metrics = {item.metric_name: item.metric_value for item in report.metrics
               if item.dimension.get("provider_id") == "opera_radar"}
    assert metrics["radar_invalid_forecast_count"] == 1
    assert metrics["radar_unscored_frame_pair_count"] == 1
    assert "radar_frame_iou_mean" not in metrics
    assert len(report.radar_verifications) == 1


def test_radar_replay_run_identity_distinguishes_unmatched_data():
    cell = _replay_cell(observed_at=100, west=4, east=4.02,
                      longitude=4.01, velocity_east_kmh=0)
    first = run_replay((), algorithm_version="test", radar_cells=(cell,))
    second = run_replay((), algorithm_version="test",
                        radar_cells=(replace(cell, observed_at=200),))
    assert first.replay_id != second.replay_id


def test_radar_replay_matches_one_to_one_and_never_across_providers():
    cell = _replay_cell(observed_at=100, west=4, east=4.02,
                       longitude=4.01, velocity_east_kmh=0)
    duplicate_candidate = replace(cell, cell_id="second-candidate")
    truth = replace(cell, observed_at=400, cell_id="truth")
    other_provider = replace(truth, provider_id="knmi_radar")
    results = verify_radar_forecasts(
        (other_provider, truth, duplicate_candidate, cell), lead_seconds=(300,),
    )
    assert len(results) == 1
    assert results[0].provider_id == "opera_radar"
    assert results[0].actual_cell_id == "truth"


def test_radar_replay_identity_and_metrics_ignore_input_order():
    cell = _replay_cell(observed_at=100, west=4, east=4.02,
                       longitude=4.01, velocity_east_kmh=0)
    truth = replace(cell, observed_at=400)
    first = run_replay((), algorithm_version="test", radar_cells=(cell, truth))
    second = run_replay((), algorithm_version="test", radar_cells=(truth, cell))
    assert first == second
    changed = run_replay((), algorithm_version="test", radar_cells=(cell, truth),
                         radar_maximum_match_distance_km=5)
    assert first.replay_id != changed.replay_id


def test_radar_replay_unions_touching_pixel_runs_before_comparing_footprints():
    cell = _replay_cell(observed_at=100, west=4, east=4.02,
                       longitude=4.01, velocity_east_kmh=0)
    first = replace(cell, geometry={"type": "MultiPolygon", "coordinates": [
        [[[4, 51], [4.01, 51], [4.01, 51.01], [4, 51.01], [4, 51]]],
        [[[4.01, 51], [4.02, 51], [4.02, 51.01], [4.01, 51.01], [4.01, 51]]],
    ]})
    truth = replace(cell, observed_at=400, geometry={"type": "Polygon", "coordinates": [
        [[4, 51], [4.02, 51], [4.02, 51.01], [4, 51.01], [4, 51]],
    ]})
    results = verify_radar_forecasts((first, truth), lead_seconds=(300,))
    assert len(results) == 1
    assert results[0].footprint_iou == pytest.approx(1)
