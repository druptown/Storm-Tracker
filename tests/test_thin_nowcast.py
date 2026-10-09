from __future__ import annotations

import pytest
from dataclasses import replace
import numpy as np

from storm_tracker_worker.thin_fusion import fuse_observations
from storm_tracker_worker.thin_nowcast import StormCell, ThinNowcastRuntime
from storm_tracker_worker.thin_observation import (
    NormalizedObservation,
    ObservationKind,
    ObservationQuality,
)
from storm_tracker_worker.thin_provider_runtime import ProviderResult
from storm_tracker_worker.thin_provider_adapters import ThinRadarField


def _cycle(
    *,
    valid_at: float,
    longitude: float,
    target_id: str | None,
    rain_rate: float = 4.0,
    include_spatial: bool = False,
):
    observation = NormalizedObservation.create(
        provider_id="opera_radar",
        product_id="composite",
        target_id=target_id,
        kind=ObservationKind.RAIN_RATE,
        value=rain_rate,
        unit="mm/h",
        observed_at=valid_at,
        received_at=valid_at + 1.0,
        latitude=50.85,
        longitude=longitude,
        quality=ObservationQuality.GOOD,
        geometry={"type": "Point", "coordinates": [longitude, 50.85]},
        metadata={"cell_id": "fixture-cell"},
    )
    observations = [observation]
    if include_spatial:
        observations.append(NormalizedObservation.create(
            provider_id="opera_radar",
            product_id="composite",
            target_id=None,
            kind=ObservationKind.RAIN_RATE,
            value=rain_rate,
            unit="mm/h",
            observed_at=valid_at,
            received_at=valid_at + 1.0,
            latitude=50.85,
            longitude=longitude,
            quality=ObservationQuality.GOOD,
            geometry={"type": "Point", "coordinates": [longitude, 50.85]},
            metadata={"cell_id": "fixture-cell"},
        ))
    result = ProviderResult(
        provider_id="opera_radar",
        ready=True,
        status="operational",
        reason_code=None,
        observations=tuple(observations),
        attempted_at=valid_at,
        completed_at=valid_at + 1.0,
    )
    return fuse_observations(
        {"opera_radar": result},
        evaluated_at=valid_at + 2.0,
        target_ids=("home",),
    )


def test_current_rain_is_on_map_without_future_arrival():
    runtime = ThinNowcastRuntime(targets={"home": (50.85, 4.35)})

    nowcast = runtime.advance(
        _cycle(
            valid_at=100.0,
            longitude=4.35,
            target_id="home",
            include_spatial=True,
        )
    )

    assert nowcast.target_results["home"].current_state == "regen"
    assert nowcast.target_results["home"].eta_minutes == 0.0
    assert len(nowcast.storm_cells) >= 1
    assert nowcast.storm_cells[0].geometry["type"] == "Point"


def test_target_sample_is_not_duplicated_as_a_spatial_storm_cell():
    runtime = ThinNowcastRuntime(targets={"home": (50.85, 4.35)})

    nowcast = runtime.advance(
        _cycle(valid_at=100.0, longitude=4.35, target_id="home")
    )

    assert nowcast.target_results["home"].current_state == "regen"
    assert nowcast.storm_cells == ()


def test_explicit_zero_rain_rate_overrides_positive_echo_at_target():
    """A quantitative dry sample must not become rain through coarse echo."""
    valid_at = 100.0
    dry_rate = NormalizedObservation.create(
        provider_id="quantitative_radar",
        product_id="rain_rate",
        target_id="home",
        kind=ObservationKind.RAIN_RATE,
        value=0.0,
        unit="mm/h",
        observed_at=valid_at,
        received_at=valid_at + 1.0,
        latitude=50.85,
        longitude=4.35,
        quality=ObservationQuality.GOOD,
        geometry={"type": "Point", "coordinates": [4.35, 50.85]},
    )
    coarse_echo = NormalizedObservation.create(
        provider_id="coarse_echo_radar",
        product_id="reflectivity",
        target_id="home",
        kind=ObservationKind.REFLECTIVITY,
        value=20.0,
        unit="dBZ",
        observed_at=valid_at,
        received_at=valid_at + 1.0,
        latitude=50.85,
        longitude=4.35,
        quality=ObservationQuality.GOOD,
        geometry={"type": "Point", "coordinates": [4.35, 50.85]},
    )
    cycle = fuse_observations(
        {
            "quantitative_radar": ProviderResult(
                provider_id="quantitative_radar",
                ready=True,
                status="operational",
                reason_code=None,
                observations=(dry_rate,),
                attempted_at=valid_at,
                completed_at=valid_at + 1.0,
            ),
            "coarse_echo_radar": ProviderResult(
                provider_id="coarse_echo_radar",
                ready=True,
                status="operational",
                reason_code=None,
                observations=(coarse_echo,),
                attempted_at=valid_at,
                completed_at=valid_at + 1.0,
            ),
        },
        evaluated_at=valid_at + 2.0,
        target_ids=("home",),
    )

    nowcast = ThinNowcastRuntime(targets={"home": (50.85, 4.35)}).advance(cycle)

    target = nowcast.target_results["home"]
    assert target.precipitation_rate_mm_h == 0.0
    assert target.current_state == "droog"
    assert target.forecast_state == "geen_dreiging"
    assert target.eta_minutes is None


def test_non_precipitation_observation_does_not_imply_dry_weather():
    valid_at = 100.0
    pressure = NormalizedObservation.create(
        provider_id="open_meteo",
        product_id="forecast",
        target_id="home",
        kind=ObservationKind.PRESSURE,
        value=1024.0,
        unit="hPa",
        observed_at=valid_at,
        received_at=valid_at + 1.0,
        latitude=55.45,
        longitude=7.58,
        quality=ObservationQuality.GOOD,
        geometry={"type": "Point", "coordinates": [7.58, 55.45]},
        metadata={"role": "model_guidance"},
    )
    cycle = fuse_observations(
        {
            "open_meteo": ProviderResult(
                provider_id="open_meteo",
                ready=True,
                status="operational",
                reason_code=None,
                observations=(pressure,),
                attempted_at=valid_at,
                completed_at=valid_at + 1.0,
            ),
        },
        evaluated_at=valid_at + 2.0,
        target_ids=("home",),
    )

    target = ThinNowcastRuntime(
        targets={"home": (55.45, 7.58)},
    ).advance(cycle).target_results["home"]

    assert target.current_state == "onbekend"
    assert target.forecast_state == "onvoldoende_data"
    assert target.eta_minutes is None


def test_two_polygon_frames_keep_motion_diagnostic_without_claiming_eta():
    runtime = ThinNowcastRuntime(targets={"home": (50.85, 4.30)})
    runtime.advance(_cycle(valid_at=100.0, longitude=4.00, target_id=None))

    nowcast = runtime.advance(
        _cycle(valid_at=700.0, longitude=4.10, target_id=None)
    )

    cell = nowcast.storm_cells[0]
    assert cell.velocity_east_kmh > 0.0
    assert abs(cell.velocity_north_kmh) < 1.0
    assert cell.motion_ready is False
    target = nowcast.target_results["home"]
    assert target.eta_minutes is None
    assert target.forecast_state != "regen_op_komst"
    assert target.arrival_cell_id is None


def test_older_cycle_cannot_move_track_state_backward():
    runtime = ThinNowcastRuntime(targets={"home": (50.85, 4.30)})
    runtime.advance(_cycle(valid_at=100.0, longitude=4.00, target_id=None))
    latest = runtime.advance(_cycle(valid_at=700.0, longitude=4.10, target_id=None))

    result = runtime.advance(_cycle(valid_at=400.0, longitude=4.05, target_id=None))

    assert result is latest
    assert result.evaluated_at == 702.0
    assert result.storm_cells[0].longitude == 4.10


def test_polygon_track_exposes_entry_exit_and_primary_method():
    runtime = ThinNowcastRuntime(targets={"home": (0.0, 0.10, 0.0)})
    cycle = _cycle(valid_at=100.0, longitude=0.0, target_id=None)
    cell = StormCell(
        cell_id="polygon-cell",
        provider_id="dwd_radolan",
        observed_at=100.0,
        latitude=0.0,
        longitude=0.0,
        geometry={
            "type": "Polygon",
            "coordinates": [[
                [-0.01, -0.01], [0.01, -0.01], [0.01, 0.01],
                [-0.01, 0.01], [-0.01, -0.01],
            ]],
        },
        precipitation_rate_mm_h=4.0,
        reflectivity_dbz=None,
        radar_level=None,
        velocity_east_kmh=60.0,
        velocity_north_kmh=0.0,
        speed_kmh=60.0,
        heading_deg=90.0,
        motion_ready=True,
    )

    nowcast = runtime.advance(cycle, tracked_cells=(cell,))

    target = nowcast.target_results["home"]
    assert target.passage_method == "translated_footprint"
    assert target.entry_at is not None
    assert target.exit_at is not None
    assert target.entry_at < target.exit_at
    assert nowcast.target_passages[0].cell_id == "polygon-cell"


def test_nowcast_keeps_multiple_labelled_qpe_passages_for_one_system() -> None:
    runtime = ThinNowcastRuntime(targets={"home": (0.0, 0.0, 0.0)})
    cycle = _cycle(valid_at=100.0, longitude=0.0, target_id=None)
    values = np.zeros((3, 7), dtype=float)
    values[1, 2] = 3.0
    values[1, 0] = 8.0
    field = ThinRadarField(
        "radar", "qpe", cycle.evaluated_at, "EPSG:3857",
        -3_500.0, 1_500.0, 1_000.0, 1_000.0,
        values, np.ones(values.shape, dtype=bool),
    )
    cell = StormCell(
        cell_id="qpe-system",
        provider_id="radar",
        observed_at=cycle.evaluated_at,
        latitude=0.0,
        longitude=-0.02,
        geometry={"type": "Point", "coordinates": [-0.02, 0.0]},
        precipitation_rate_mm_h=8.0,
        reflectivity_dbz=None,
        radar_level=None,
        velocity_east_kmh=36.0,
        velocity_north_kmh=0.0,
        speed_kmh=36.0,
        heading_deg=90.0,
        motion_ready=True,
        radar_field=field,
        footprint_pixels=((1, 0), (1, 2)),
    )

    result = runtime.advance(cycle, tracked_cells=(cell,))

    assert len(result.target_passages) == 2
    assert [item.interval_ordinal for item in result.target_passages] == [0, 1]
    assert result.target_results["home"].arrival_cell_id == "qpe-system"
    assert result.target_results["home"].passage_method == "advected_labelled_qpe"


def test_unrelated_system_cannot_cancel_existing_passage():
    runtime = ThinNowcastRuntime(targets={"home": (0.0, 0.10, 0.0)})
    runtime.update_targets(
        {"home": (0.0, 0.10, 0.0)},
        target_region_ids={"home": "region:old"},
    )
    cycle = _cycle(valid_at=100.0, longitude=0.0, target_id=None)
    moving = StormCell(
        cell_id="moving-cell",
        provider_id="fusion:old",
        observed_at=100.0,
        latitude=0.0,
        longitude=0.0,
        geometry={
            "type": "Polygon",
            "coordinates": [[
                [-0.01, -0.01], [0.01, -0.01], [0.01, 0.01],
                [-0.01, 0.01], [-0.01, -0.01],
            ]],
        },
        precipitation_rate_mm_h=2.0,
        reflectivity_dbz=None,
        radar_level=None,
        velocity_east_kmh=60.0,
        velocity_north_kmh=0.0,
        speed_kmh=60.0,
        heading_deg=90.0,
        motion_ready=True,
        region_id="region:old",
    )
    first = runtime.advance(cycle, tracked_cells=(moving,))
    original = first.target_results["home"]
    assert original.forecast_state == "regen_op_komst"

    runtime.update_targets(
        {"home": (0.0, 0.10, 0.0)},
        target_region_ids={"home": "region:new"},
    )
    handoff = runtime.advance(
        replace(cycle, evaluated_at=110.0), tracked_cells=(),
    )

    carried = handoff.target_results["home"]
    assert carried.forecast_state == "regen_op_komst"
    assert carried.reason_code == "tracked_cell_arrival_handoff_hold"
    assert carried.arrival_cell_id == original.arrival_cell_id
    assert carried.entry_at == original.entry_at
    assert carried.exit_at == original.exit_at
    assert handoff.target_passages == ()

    contradicted = runtime.advance(
        replace(cycle, evaluated_at=120.0),
        tracked_cells=(replace(
            moving,
            region_id="region:new",
            velocity_east_kmh=-60.0,
            heading_deg=270.0,
        ),),
    )
    assert contradicted.target_results["home"].forecast_state == "regen_op_komst"
    assert contradicted.target_results["home"].arrival_cell_id == original.arrival_cell_id


def test_target_forecast_only_uses_cells_from_its_own_region():
    runtime = ThinNowcastRuntime(targets={"home": (0.0, 0.10, 0.0)})
    runtime.update_targets(
        {"home": (0.0, 0.10, 0.0)},
        target_region_ids={"home": "region:home"},
    )
    cycle = _cycle(valid_at=100.0, longitude=0.0, target_id=None)
    external_cell = StormCell(
        cell_id="external-cell",
        provider_id="opera_radar",
        observed_at=100.0,
        latitude=0.0,
        longitude=0.0,
        geometry={
            "type": "Polygon",
            "coordinates": [[
                [-0.01, -0.01], [0.01, -0.01], [0.01, 0.01],
                [-0.01, 0.01], [-0.01, -0.01],
            ]],
        },
        precipitation_rate_mm_h=4.0,
        reflectivity_dbz=None,
        radar_level=None,
        velocity_east_kmh=60.0,
        velocity_north_kmh=0.0,
        speed_kmh=60.0,
        heading_deg=90.0,
        motion_ready=True,
        region_id="region:external",
    )

    nowcast = runtime.advance(cycle, tracked_cells=(external_cell,))

    target = nowcast.target_results["home"]
    assert target.arrival_cell_id is None
    assert target.forecast_state != "regen_op_komst"
    assert nowcast.target_passages == ()


@pytest.mark.parametrize("explicit_id", [True, False])
def test_cached_radar_frame_retains_its_id_and_motion(explicit_id):
    runtime = ThinNowcastRuntime(targets={"home": (50.85, 4.30)})

    def cycle(at, lon):
        result = _cycle(valid_at=at, longitude=lon, target_id=None)
        if not explicit_id:
            result = replace(result, observations=tuple(
                replace(obs, metadata={}) for obs in result.observations
            ))
        return result

    runtime.advance(cycle(100, 4.00))
    frame = cycle(700, 4.10)
    moving = runtime.advance(frame)
    for evaluated_at in (704, 706, 710):
        repeated = runtime.advance(replace(frame, evaluated_at=evaluated_at))
        assert repeated.storm_cells == moving.storm_cells
    following = runtime.advance(cycle(1000, 4.15))
    assert following.storm_cells[0].cell_id == moving.storm_cells[0].cell_id
    assert following.storm_cells[0].velocity_east_kmh == pytest.approx(
        moving.storm_cells[0].velocity_east_kmh)
