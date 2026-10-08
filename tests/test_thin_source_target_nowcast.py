from dataclasses import replace

import numpy as np
import pytest

from storm_tracker_worker.thin_fusion import FusionCycle, FusedTargetObservation
from storm_tracker_worker.thin_nowcast import ThinNowcastRuntime
from storm_tracker_worker.thin_provider_adapters import ThinRadarField
from storm_tracker_worker.thin_regional_radar_fusion import RegionalRadarLayer
from storm_tracker_worker.thin_source_radar_forecast import SourceRadarMotion
from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid


def motion(values=None, provider="knmi_radar"):
    values = np.full((10, 10), .2) if values is None else values
    field = ThinRadarField(provider, "rate", 900., "EPSG:3857", -5000.,
                           5000., 1000., 1000., values, np.ones(values.shape, bool))
    grid = AnalysisGrid("source-grid", "EPSG:3857", -5000., 5000.,
                        1000., 1000., 10, 10)
    layer = RegionalRadarLayer(provider, "rate", 900., grid, values,
        field.valid_mask, field.valid_mask, field.valid_mask.astype(float), 1000.)
    return SourceRadarMotion(field, layer, 950., "previous-field", 300.,
        ((-1000., 0., 1., 0.), (1000., 0., 1., 0.), (0., 1000., 1., 0.)))


def cycle(at=1000., rate=0.):
    target = FusedTargetObservation("home", "operational", at, rate,
        None, None, None, None, None, 0, .5, ("knmi_radar",), {})
    return FusionCycle(at, {"home": target}, {}, {}, ())


def runtime():
    result = ThinNowcastRuntime(targets={"home": (0., 0.)})
    result.update_targets({"home": (0., 0.)}, target_region_ids={"home": "r1"})
    return result


def test_weak_field_predicts_whole_passage_without_intensity_cells():
    values = np.zeros((10, 10)); values[:, 1:4] = .2
    source = motion(values)
    result = runtime().advance(cycle(), tracked_cells=(), source_motions={"r1": (source,)})
    target = result.target_results["home"]
    assert target.entry_at == pytest.approx(1930.)
    assert target.exit_at == pytest.approx(4930.)
    assert target.passage_method == "source_clock_full_field"
    assert target.eta_minutes == 15.5
    assert target.radar_field_forecast["primary_provider"] == "knmi_radar"
    assert target.radar_field_forecast["issued_at"] == 1000.
    assert target.radar_field_forecast["candidates"][0]["product_at"] == 900.
    assert result.target_passages[0].quantity == "precipitation_rate"
    assert result.target_passages[0].field_fingerprint == source.field.identity.content_fingerprint
    assert result.target_passages[0].entry_bracket_lower_at == 1900.
    assert result.target_passages[0].entry_bracket_upper_at == 1960.


def test_unsupported_field_is_not_published_as_no_future_rain():
    source = replace(motion(), vectors=(), reason_code="motion_support_missing")
    target = runtime().advance(cycle(), tracked_cells=(),
        source_motions={"r1": (source,)}).target_results["home"]
    assert target.forecast_state == "onvoldoende_data"
    assert target.entry_at is None
    assert target.radar_field_forecast["support_state"] == "unknown"


def test_field_horizon_does_not_create_an_end_while_raining():
    source = motion(np.full((10, 10), .2))
    source = replace(source, vectors=tuple((x,y,0.,0.) for x,y,_,_ in source.vectors))
    target = runtime().advance(cycle(rate=.2), tracked_cells=(),
        source_motions={"r1": (source,)}).target_results["home"]
    assert target.current_state == "regen"
    assert target.eta_minutes == 0.
    assert target.exit_at is None
    assert target.radar_field_forecast["intervals"][0]["end_status"] == "horizon_censored"


def test_source_region_never_supplies_another_target_region():
    instance = runtime()
    instance.update_targets({"home": (0.,0.), "other": (0.,0.)},
                            target_region_ids={"home": "r1", "other": "r2"})
    result = instance.advance(cycle(), tracked_cells=(), source_motions={"r1": (motion(),)})
    assert result.target_results["home"].passage_method == "source_clock_full_field"
    assert result.target_results["other"].passage_method is None


def test_missing_field_keeps_last_applicable_arrival_with_original_issue_time():
    instance = runtime()
    values = np.zeros((10,10)); values[:,1:4] = .2
    first = instance.advance(cycle(), tracked_cells=(), source_motions={"r1": (motion(values),)})
    unavailable = replace(motion(values), vectors=(), reason_code="motion_support_missing")
    second = instance.advance(cycle(1060.), tracked_cells=(), source_motions={"r1": (unavailable,)})
    target = second.target_results["home"]
    assert target.entry_at == first.target_results["home"].entry_at
    assert target.radar_field_forecast["issued_at"] == 1000.
    assert target.radar_field_forecast["support_state"] == "held"
    from storm_tracker_worker.thin_mqtt_service import _build_passages_contract
    held_passage = _build_passages_contract(second)["items"][0]
    assert held_passage["entry_at"] == 1930.
    assert held_passage["status"] == "coasting"
    instance.update_targets({"home": (1.,0.)}, target_region_ids={"home": "r1"})
    moved = instance.advance(cycle(1120.), tracked_cells=(), source_motions={"r1": (unavailable,)})
    assert moved.target_results["home"].entry_at is None


def test_live_cycle_supplies_completed_source_motion_to_target_forecast():
    import asyncio
    from test_thin_operational_runtime import (
        _RegionalCoordinator, _shared_home_state, ZeroRainProviders, _regional_result)
    from storm_tracker_worker.thin_operational_runtime import ThinOperationalRuntime
    from storm_tracker_worker.thin_region_planner import ThinRegionPlanner
    from storm_tracker_worker.desired_state import parse_desired_state
    state = _shared_home_state()
    plan = ThinRegionPlanner().plan(parse_desired_state(state), now=1000.)
    source = motion()
    # Put the actual field/metric projection at the Home target.
    from pyproj import Transformer
    x,y = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True).transform(4.42,51.05)
    source = replace(source, layer=replace(source.layer, grid=replace(
        source.layer.grid, origin_x_m=x-5000., origin_y_m=y+5000.)),
        vectors=tuple((x+px,y+py,0.,0.) for px,py,_,_ in source.vectors))
    coordinator = _RegionalCoordinator({r.region_id: replace(
        _regional_result(source.field), region_id=r.region_id,
        source_motions=(source,)) for r in plan.regions})
    worker = ThinOperationalRuntime(provider_runtime=ZeroRainProviders(),
        clock=lambda:1000., regional_radar_coordinator=coordinator)
    worker.apply_desired_state(state)
    result = asyncio.run(worker.run_cycle())
    assert result.nowcast.target_results["home"].passage_method == "source_clock_full_field"
    snapshot_targets = {t["target_id"]: t for t in result.snapshot["targets"]["items"]}
    assert snapshot_targets["home"]["next_threat"]["product_valid_at"] == 900.
    for target_id, forecast in result.nowcast.target_results.items():
        if forecast.radar_field_forecast["primary_provider"] is None:
            threat = snapshot_targets[target_id]["next_threat"]
            assert threat["state"] == threat["freshness"] == "unknown"
            assert threat["product_valid_at"] is None


def test_field_publication_keeps_unknown_support_and_source_times_without_passage():
    from storm_tracker_worker.thin_mqtt_service import _build_field_forecasts_contract, _build_passages_contract
    source = replace(motion(), vectors=(), reason_code="motion_support_missing")
    forecast = runtime().advance(cycle(), tracked_cells=(), source_motions={"r1": (source,)})
    payload = _build_passages_contract(forecast)
    assert not payload["items"]
    field = _build_field_forecasts_contract(forecast)["targets"]["home"]
    assert field["support_state"] == "unknown"
    assert field["candidates"][0]["product_at"] == 900.
    assert field["entry_at"] is None


@pytest.mark.asyncio
async def test_archived_unknown_forecast_retains_actual_source_pair_and_policy(monkeypatch):
    import json
    from test_thin_postgis import ConnectionFactory, FakeCursor, _settings
    from storm_tracker_worker.thin_postgis import ThinPostGISWriter
    source = replace(motion(), vectors=(), reason_code="motion_support_missing")
    monkeypatch.setattr(FakeCursor, "fetchone", lambda self: (True,), raising=False)
    forecast = runtime().advance(cycle(), tracked_cells=(), source_motions={"r1": (source,)})
    factory = ConnectionFactory()
    result = await ThinPostGISWriter(_settings(), connect_factory=factory).write_cycle(cycle(), forecast)
    assert result.status == "current"
    row = next(p for sql,p in factory.statements if sql.startswith("INSERT INTO stv3_target_forecast_issue("))
    assert row[3] == "source-clock-full-field-v1"
    assert row[4] == "source-clock-field-episode-v2"
    assert set(json.loads(row[6])) == {source.field.identity.content_fingerprint, "previous-field"}
    assert json.loads(row[7])["forecast"]["radar_field_forecast"]["support_state"] == "unknown"


def test_full_field_and_core_windows_are_published_separately(monkeypatch):
    from storm_tracker_worker.thin_nowcast import StormCell
    from storm_tracker_worker.thin_footprint_forecast import TargetPassage
    from storm_tracker_worker.thin_mqtt_service import _build_passages_contract
    import storm_tracker_worker.thin_nowcast as module
    values = np.zeros((10,10)); values[:,1:4] = .2
    # Controlled existing component predictor returns a short intensity core.
    # The actual full-field tracing and publication must not select its exit.
    core = TargetPassage("home",1000.,"core","knmi_radar",2000.,2060.,
        "translated_footprint",5.,{"type":"Point","coordinates":[0.,0.]})
    monkeypatch.setattr(module, "forecast_target_passages", lambda **kwargs: (core,))
    cell = StormCell("core","knmi_radar",900.,0.,0.,{},5.,None,None,
        3.6,0.,3.6,90.,True,region_id="r1")
    forecast = runtime().advance(cycle(), tracked_cells=(cell,),source_motions={"r1":(motion(values),)})
    assert forecast.target_results["home"].exit_at == 4930.
    assert forecast.target_results["home"].radar_field_forecast["intensity_core_windows"][0]["exit_at"] == 2060.
    assert {p.cell_id for p in forecast.target_passages} == {"core","field:r1:knmi_radar"}
    payload = _build_passages_contract(forecast)
    assert len(payload["items"]) == 1
    assert payload["items"][0]["exit_at"] == 4930.


def test_supported_dry_gap_is_not_joined_into_one_wet_period():
    values = np.zeros((10,10)); values[:,1:4] = .2; values[:,2] = 0.
    forecast = runtime().advance(cycle(),tracked_cells=(),source_motions={"r1":(motion(values),)})
    intervals = forecast.target_results["home"].radar_field_forecast["intervals"]
    assert len(intervals) == 2
    assert intervals[0]["exit_at"] == 2890.
    assert intervals[1]["entry_at"] == 3910.


def test_target_episode_uses_weak_edges_and_keeps_intensity_windows():
    values = np.zeros((10,10)); values[:,1:5] = .03; values[:,3] = .2
    result = runtime().advance(cycle(), tracked_cells=(),
        source_motions={"r1": (motion(values),)})
    target = result.target_results["home"]
    assert target.entry_at == 1000.  # first sample already wet, not an onset
    assert target.exit_at == 4930.
    summary = target.radar_field_forecast
    assert summary["intervals"][0]["left_censored"]
    assert summary["threshold_mm_h"] == 0.
    assert summary["threshold_operator"] == "gt"
    assert summary["intensity_threshold_mm_h"] == .1
    assert summary["intensity_intervals"][0]["entry_at"] == 1930.
    assert summary["intensity_intervals"][0]["exit_at"] == 2890.
    assert summary["candidates"][0]["intervals"][0]["exit_at"] == 4930.
    assert summary["candidates"][0]["intensity_intervals"][0]["exit_at"] == 2890.
    from storm_tracker_worker.thin_mqtt_service import _build_field_forecasts_contract
    published = _build_field_forecasts_contract(result)["targets"]["home"]
    assert published["exit_at"] == 4930.
    assert published["intensity_intervals"][0]["exit_at"] == 2890.


def test_weak_background_prevents_false_closed_target_end():
    values = np.full((10,10), .03); values[:,3:6] = .2
    source = motion(values)
    # Remain inside the quantitative/support domain for the whole horizon.
    source = replace(source, vectors=tuple((x,y,.3,0.) for x,y,_,_ in source.vectors))
    target = runtime().advance(cycle(rate=.03), tracked_cells=(),
        source_motions={"r1": (source,)}).target_results["home"]
    assert target.exit_at is None
    assert target.radar_field_forecast["intervals"][0]["end_status"] == "horizon_censored"


def test_colocated_targets_share_field_timing_and_conflicts_keep_primary():
    instance = runtime()
    instance.update_targets({"home":(0.,0.),"other":(0.,0.)},
        target_region_ids={"home":"r1","other":"r1"})
    wet = motion(); dry = motion(np.zeros((10,10)),provider="dwd_radolan")
    first = instance.advance(cycle(),tracked_cells=(),source_motions={"r1":(wet,)})
    result = instance.advance(cycle(1060.),tracked_cells=(),source_motions={"r1":(dry,wet)})
    for target in result.target_results.values():
        assert target.radar_field_forecast["source_conflict"]
        assert target.arrival_provider_id == "knmi_radar"
    assert result.target_results["home"].entry_at == result.target_results["other"].entry_at
