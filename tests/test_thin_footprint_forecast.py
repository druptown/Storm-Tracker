from __future__ import annotations

from types import SimpleNamespace

import pytest
import numpy as np

from storm_tracker_worker.thin_footprint_forecast import (
    forecast_target_passage,
    forecast_target_passages,
)
from storm_tracker_worker.thin_nowcast import StormCell
from storm_tracker_worker.thin_provider_adapters import ThinRadarField


EVALUATED_AT = 10_000.0


def _polygon(*points):
    ring = [list(point) for point in points]
    if ring[0] != ring[-1]:
        ring.append(ring[0])
    return {"type": "Polygon", "coordinates": [ring]}


def _multipolygon(*polygons):
    coordinates = []
    for points in polygons:
        ring = [list(point) for point in points]
        if ring[0] != ring[-1]:
            ring.append(ring[0])
        coordinates.append([ring])
    return {"type": "MultiPolygon", "coordinates": coordinates}


def _cell(*, geometry, longitude=0.0, latitude=0.0, east=0.0, north=0.0, observed_at=9_990.0):
    return StormCell(
        cell_id="cell-1",
        provider_id="fixture",
        observed_at=observed_at,
        latitude=latitude,
        longitude=longitude,
        geometry=geometry,
        precipitation_rate_mm_h=4.0,
        reflectivity_dbz=None,
        radar_level=None,
        velocity_east_kmh=east,
        velocity_north_kmh=north,
        speed_kmh=(east * east + north * north) ** 0.5,
        heading_deg=None,
        motion_ready=True,
    )


def test_current_polygon_intersection_has_immediate_entry_and_ordered_exit():
    cell = _cell(
        geometry=_polygon((-0.01, -0.01), (0.01, -0.01), (0.01, 0.01), (-0.01, 0.01)),
        east=30.0,
    )

    passage = forecast_target_passage(
        cell=cell,
        target_id="home",
        target_latitude=0.0,
        target_longitude=0.0,
        evaluated_at=EVALUATED_AT,
    )

    assert passage is not None
    assert passage.entry_at == EVALUATED_AT
    assert passage.exit_at > passage.entry_at
    assert passage.method == "translated_footprint"


@pytest.mark.parametrize(
    ("target_latitude", "target_longitude", "east", "north"),
    ((0.0, 0.10, 60.0, 0.0), (0.0, -0.10, -60.0, 0.0),
     (0.10, 0.0, 0.0, 60.0), (-0.10, 0.0, 0.0, -60.0)),
)
def test_polygon_forecast_supports_all_cardinal_motion_directions(
    target_latitude, target_longitude, east, north,
):
    cell = _cell(
        geometry=_polygon((-0.01, -0.01), (0.01, -0.01), (0.01, 0.01), (-0.01, 0.01)),
        east=east,
        north=north,
    )

    passage = forecast_target_passage(
        cell=cell,
        target_id="target",
        target_latitude=target_latitude,
        target_longitude=target_longitude,
        evaluated_at=EVALUATED_AT,
    )

    assert passage is not None
    assert 8.0 * 60.0 < passage.entry_at - EVALUATED_AT < 12.0 * 60.0
    assert passage.exit_at > passage.entry_at


def test_irregular_polygon_rejects_target_inside_bbox_but_outside_footprint():
    cell = _cell(geometry=_polygon(
        (0.0, 0.0), (0.03, 0.0), (0.03, 0.01),
        (0.01, 0.01), (0.01, 0.03), (0.0, 0.03),
    ))

    passage = forecast_target_passage(
        cell=cell,
        target_id="gap",
        target_latitude=0.02,
        target_longitude=0.02,
        evaluated_at=EVALUATED_AT,
        accuracy_buffer_km=0.0,
    )

    assert passage is None


def test_fragmented_footprint_exit_covers_the_last_translated_fragment():
    """A raster footprint may contain separated runs from one rain shield.

    The passage envelope must not stop at the first dry gap and discard the
    trailing part of that same detected footprint.
    """
    cell = _cell(
        geometry=_multipolygon(
            ((-0.01, -0.01), (0.01, -0.01), (0.01, 0.01), (-0.01, 0.01)),
            ((-0.05, -0.01), (-0.03, -0.01), (-0.03, 0.01), (-0.05, 0.01)),
        ),
        east=60.0,
        observed_at=EVALUATED_AT,
    )

    passage = forecast_target_passage(
        cell=cell,
        target_id="target",
        target_latitude=0.0,
        target_longitude=0.10,
        evaluated_at=EVALUATED_AT,
    )

    assert passage is not None
    assert 8.0 * 60.0 < passage.entry_at - EVALUATED_AT < 12.0 * 60.0
    assert 13.0 * 60.0 < passage.exit_at - EVALUATED_AT < 17.0 * 60.0


def test_passage_intensity_uses_footprint_mean_instead_of_remote_cell_peak():
    original = _cell(
        geometry=_polygon((-0.01, -0.01), (0.01, -0.01), (0.01, 0.01), (-0.01, 0.01)),
        east=30.0,
    )
    values = original.as_dict()
    values["mean_precipitation_rate_mm_h"] = 1.5
    cell = SimpleNamespace(**values)

    passage = forecast_target_passage(
        cell=cell,
        target_id="home",
        target_latitude=0.0,
        target_longitude=0.0,
        evaluated_at=EVALUATED_AT,
    )

    assert passage is not None
    assert passage.predicted_intensity_mm_h == 1.5


def test_target_accuracy_buffer_can_turn_a_near_miss_into_a_passage():
    cell = _cell(
        geometry=_polygon((-0.01, -0.01), (0.01, -0.01), (0.01, 0.01), (-0.01, 0.01)),
        east=60.0,
    )

    miss = forecast_target_passage(
        cell=cell, target_id="target", target_latitude=0.015,
        target_longitude=0.10, evaluated_at=EVALUATED_AT,
        accuracy_buffer_km=0.0,
    )
    buffered = forecast_target_passage(
        cell=cell, target_id="target", target_latitude=0.015,
        target_longitude=0.10, evaluated_at=EVALUATED_AT,
        accuracy_buffer_km=0.7,
    )

    assert miss is None
    assert buffered is not None


def test_stationary_and_horizon_clipping_are_explicit():
    cell = _cell(
        geometry=_polygon((-0.01, -0.01), (0.01, -0.01), (0.01, 0.01), (-0.01, 0.01)),
    )

    passage = forecast_target_passage(
        cell=cell, target_id="home", target_latitude=0.0,
        target_longitude=0.0, evaluated_at=EVALUATED_AT,
        horizon_seconds=900.0,
    )

    assert passage is not None
    assert passage.entry_at == EVALUATED_AT
    assert passage.exit_at == EVALUATED_AT + 900.0
    assert passage.horizon_clipped is True


def test_old_cell_frame_is_rejected():
    cell = _cell(
        geometry=_polygon((-0.01, -0.01), (0.01, -0.01), (0.01, 0.01), (-0.01, 0.01)),
        east=30.0,
        observed_at=EVALUATED_AT - 901.0,
    )

    assert forecast_target_passage(
        cell=cell, target_id="home", target_latitude=0.0,
        target_longitude=0.0, evaluated_at=EVALUATED_AT,
    ) is None


@pytest.mark.parametrize("polygon", [True, False])
def test_cached_frame_does_not_postpone_absolute_arrival(polygon):
    cell = _cell(
        geometry=(_polygon((-0.01, -0.01), (0.01, -0.01), (0.01, 0.01), (-0.01, 0.01))
                  if polygon else {"type": "Point", "coordinates": [0.0, 0.0]}),
        east=60.0, observed_at=EVALUATED_AT,
    )
    def predict(at):
        return forecast_target_passage(
            cell=cell, target_id="home", target_latitude=0.0,
            target_longitude=0.10, evaluated_at=at,
        )
    first, cached = predict(EVALUATED_AT), predict(EVALUATED_AT + 60.0)
    assert first is not None and cached is not None
    assert cached.entry_at == pytest.approx(first.entry_at, abs=0.1)
    assert cached.exit_at == pytest.approx(first.exit_at, abs=0.1)


def test_cached_footprint_that_has_passed_is_no_longer_a_future_passage():
    cell = _cell(
        geometry=_polygon((-0.01, -0.01), (0.01, -0.01), (0.01, 0.01), (-0.01, 0.01)),
        east=60.0, observed_at=EVALUATED_AT,
    )
    assert forecast_target_passage(
        cell=cell, target_id="home", target_latitude=0.0,
        target_longitude=0.0, evaluated_at=EVALUATED_AT + 300.0,
    ) is None


def test_point_geometry_uses_named_centerline_fallback():
    cell = _cell(
        geometry={"type": "Point", "coordinates": [0.0, 0.0]},
        east=60.0,
    )

    passage = forecast_target_passage(
        cell=cell, target_id="target", target_latitude=0.0,
        target_longitude=0.10, evaluated_at=EVALUATED_AT,
    )

    assert passage is not None
    assert passage.method == "centerline_fallback"
    assert passage.fallback_reason == "polygon_unavailable"


def test_live_forecast_prefers_labelled_qpe_and_keeps_reentry_intervals() -> None:
    values = np.zeros((3, 7), dtype=float)
    values[1, 2] = 3.0
    values[1, 0] = 8.0
    field = ThinRadarField(
        "radar", "qpe", EVALUATED_AT, "EPSG:3857",
        -3_500.0, 1_500.0, 1_000.0, 1_000.0,
        values, np.ones(values.shape, dtype=bool),
    )
    cell = _cell(
        geometry=_polygon((-0.04, -0.01), (0.0, -0.01), (0.0, 0.01), (-0.04, 0.01)),
        east=36.0,
        observed_at=EVALUATED_AT,
    )
    cell = SimpleNamespace(
        **cell.as_dict(),
        radar_field=field,
        footprint_pixels=((1, 0), (1, 2)),
    )

    passages = forecast_target_passages(
        cell=cell,
        target_id="home",
        target_latitude=0.0,
        target_longitude=0.0,
        evaluated_at=EVALUATED_AT,
        horizon_seconds=400.0,
        forecast_step_seconds=100.0,
    )

    assert len(passages) == 2
    assert [item.interval_ordinal for item in passages] == [0, 1]
    assert [item.predicted_intensity_mm_h for item in passages] == [3.0, 8.0]
    assert all(item.method == "advected_labelled_qpe" for item in passages)
