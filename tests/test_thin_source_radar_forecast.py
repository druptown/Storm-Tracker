from dataclasses import replace
import numpy as np
import pytest

from storm_tracker_worker import thin_source_radar_forecast as forecast
from storm_tracker_worker.thin_provider_adapters import ThinRadarField
from storm_tracker_worker.thin_regional_radar_fusion import RegionalRadarLayer
from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid


def field(at=900., provider="knmi_radar", values=None):
    values = np.full((10, 10), .2) if values is None else values
    return ThinRadarField(provider, "rate", at, "EPSG:3857", -5000., 5000.,
                          1000., 1000., values, np.ones(values.shape, bool))


def layer(source):
    grid = AnalysisGrid("same-grid", source.crs, source.origin_x_m,
                        source.origin_y_m, 1000., 1000.,
                        source.rain_rate_mm_h.shape[1], source.rain_rate_mm_h.shape[0])
    return RegionalRadarLayer(source.provider_id, source.product_id,
                              source.observed_at, grid, source.rain_rate_mm_h,
                              source.valid_mask, source.valid_mask,
                              source.valid_mask.astype(float), 1000.)


def motion(source=None, velocity=(1., 0.)):
    source = source or field()
    return forecast.SourceRadarMotion(
        field=source, layer=layer(source), received_at=950.,
        previous_fingerprint="previous", delta_seconds=300.,
        vectors=((-1000., 0., *velocity), (1000., 0., *velocity),
                 (0., 1000., *velocity)), reason_code=None,
    )


def test_trace_advects_age_and_weak_rain_outside_shield():
    values = np.zeros((10, 10)); values[:, :4] = .2
    samples = forecast.trace_target(motion(field(values=values)), 0., 0., 1000.)
    assert len(samples) == 121
    assert samples[0]["rate_mm_h"] == 0.
    assert samples[15]["rate_mm_h"] == 0.
    assert samples[16]["rate_mm_h"] == .2
    assert samples[16]["at"] == 1960.
    assert samples[16]["source_x_m"] == pytest.approx(-1060.)


def test_missing_local_support_is_unknown_not_dry():
    source = replace(motion(), vectors=((0., 0., 1., 0.),))
    samples = forecast.trace_target(source, 0., 0., 1000.)
    assert all(s["rate_mm_h"] is None for s in samples)
    assert samples[0]["reason"] == "motion_support_missing"


def test_nodata_is_unknown_even_with_zero_velocity():
    source = field(); source.valid_mask[5, 5] = False
    samples = forecast.trace_target(motion(source, (0., 0.)), 0., 0., 1000.)
    assert samples[0]["rate_mm_h"] is None
    assert samples[0]["reason"] == "source_nodata"


@pytest.mark.parametrize("issued,reason", [(899., "future_product"),
                                            (949., "future_receipt"),
                                            (1801., "source_stale")])
def test_unavailable_source_cannot_predict(issued, reason):
    samples = forecast.trace_target(motion(), 0., 0., issued)
    assert all(s["rate_mm_h"] is None for s in samples)
    assert samples[0]["reason"] == reason


def test_intervals_do_not_invent_exit_at_unknown_or_horizon():
    samples = [{"at": 1000., "rate_mm_h": 0.},
               {"at": 1060., "rate_mm_h": .2},
               {"at": 1120., "rate_mm_h": None},
               {"at": 1180., "rate_mm_h": .3}]
    intervals = forecast.rain_intervals(samples)
    assert intervals[0]["entry_at"] == 1030.
    assert intervals[0]["exit_at"] is None
    assert intervals[0]["end_status"] == "support_censored"
    assert intervals[1]["entry_at"] is None
    assert intervals[1]["end_status"] == "horizon_censored"


def test_real_dry_gap_remains_between_two_intervals():
    samples = [{"at": 1000.+i*60., "rate_mm_h": value}
               for i, value in enumerate((0., .2, 0., .3, 0.))]
    intervals = forecast.rain_intervals(samples)
    assert len(intervals) == 2
    assert intervals[0]["exit_at"] == 1090.
    assert intervals[1]["entry_at"] == 1150.


def test_primary_conflict_keeps_selected_source_not_average():
    a = {"provider_id": "a", "native_resolution_m": 500.,
         "product_at": 900., "samples": [{"at": 1000., "rate_mm_h": 4.}]}
    b = {"provider_id": "b", "native_resolution_m": 1000.,
         "product_at": 950., "samples": [{"at": 1000., "rate_mm_h": 0.}]}
    result = forecast.select_primary((a, b), previous_provider="b")
    assert result["primary_provider"] == "b"
    assert result["samples"][0]["rate_mm_h"] == 0.
    assert result["source_conflict"] is True
    assert result["selection_reason"] == "retained_supported_primary"


def test_combination_rejects_unaligned_valid_times():
    a = {"provider_id": "a", "native_resolution_m": 500., "product_at": 900.,
         "samples": [{"at": 1000., "rate_mm_h": .2}]}
    b = {**a, "provider_id": "b", "samples": [{"at": 1060., "rate_mm_h": .2}]}
    with pytest.raises(ValueError, match="unaligned_prediction_times"):
        forecast.select_primary((a, b))


def test_source_pair_keeps_own_clock_and_does_not_repeat_compute():
    calls = []
    def estimate(old, new):
        calls.append((old.field.observed_at, new.field.observed_at))
        return ((0., 0., 2., 0.), (1000., 0., 2., 0.), (0., 1000., 2., 0.))
    engine = forecast.SourceRadarForecastEngine(estimate=estimate)
    first = field(300.); second = field(900.)
    assert engine.advance(first, layer(first), received_at=350.).reason_code == "source_pair_missing"
    result = engine.advance(second, layer(second), received_at=950.)
    assert result.delta_seconds == 600.
    assert calls == [(300., 900.)]
    repeat = engine.advance(second, layer(second), received_at=1000.)
    assert repeat is result
    assert repeat.received_at == 950.
    assert calls == [(300., 900.)]


def test_source_pair_never_crosses_providers_or_grid_revision():
    engine = forecast.SourceRadarForecastEngine(estimate=lambda a,b: ())
    a = field(300.); b = field(600., "dwd_radolan")
    engine.advance(a, layer(a), received_at=350.)
    assert engine.advance(b, layer(b), received_at=650.).reason_code == "source_pair_missing"
    newer = field(900.)
    changed_layer = replace(layer(newer), grid=replace(layer(newer).grid, grid_id="changed"))
    result = engine.advance(newer, changed_layer, received_at=950.)
    assert result.reason_code == "source_pair_incompatible"
    assert result.delta_seconds is None


def test_same_time_correction_does_not_supply_independent_motion():
    engine = forecast.SourceRadarForecastEngine(estimate=lambda a,b: ())
    a = field(300.)
    engine.advance(a, layer(a), received_at=350.)
    corrected = field(300., values=np.full((10,10), .3))
    result = engine.advance(corrected, layer(corrected), received_at=400.)
    assert result.reason_code == "same_time_revision_requires_new_pair"
    assert result.delta_seconds is None


def test_real_lk_uses_source_600_seconds_not_composite_300_seconds():
    rows, cols = np.indices((64, 64))
    values = sum(.4*np.exp(-((rows-r)**2+(cols-c)**2)/18.)
                 for r,c in ((20,20),(20,40),(40,20),(40,40)))
    old = field(300., values=values)
    new = field(900., values=np.roll(values, 1, axis=1))
    engine = forecast.SourceRadarForecastEngine()
    engine.advance(old, layer(old), received_at=350.)
    result = engine.advance(new, layer(new), received_at=950.)
    assert len(result.vectors) >= 3
    assert np.median([v[2] for v in result.vectors]) == pytest.approx(1000./600., abs=.15)
    assert np.median([v[3] for v in result.vectors]) == pytest.approx(0., abs=.15)
