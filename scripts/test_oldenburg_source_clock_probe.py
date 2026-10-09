"""Focused identity, product-clock and historical availability regressions."""
import numpy as np
import pytest
from oldenburg_source_clock_probe import provider_pair, motion_common, provider_series
from oldenburg_source_clock_probe import combine_aligned_series
from oldenburg_source_clock_probe import restore_motion_config


def field(provider, valid, available=None, field_id=None, product='qpe', bit=1):
    return {'field_id': field_id or f'{provider}-{valid}',
            'metadata': {'provider_id': provider, 'product_id': product,
                         'observed_at': valid, 'unit': 'mm/h',
                         'identity': {'quantity': 'precipitation_rate',
                                      'processing_version': 'decoder-v1'}},
            'available_at': valid if available is None else available,
            'bit': bit}


def test_same_provider_bit_relabeling_does_not_discard_motion():
    a, b = field('knmi', 1000, bit=2), field('knmi', 1600, bit=1)
    old, new = provider_pair([a, b], 'knmi', 1700)
    assert (old, new) == (a, b)
    assert new['metadata']['observed_at']-old['metadata']['observed_at'] == 600
    assert motion_common(old, new, np.ones((2, 2)), np.ones((2, 2)),
                         np.ones((2, 2), bool), np.ones((2, 2), bool)).all()


def test_same_bit_different_provider_is_not_a_motion_pair():
    a, b = field('knmi', 1000, bit=1), field('mf', 1300, bit=1)
    assert provider_pair([a, b], 'knmi', 1400) is None
    with pytest.raises(ValueError, match='provider_or_product_mismatch'):
        motion_common(a, b, np.ones((2, 2)), np.ones((2, 2)),
                      np.ones((2, 2), bool), np.ones((2, 2), bool))


def test_real_provider_delta_not_composite_delta():
    a, b = field('knmi', 1200), field('knmi', 1800)
    # MF's composite clock changed 1500->1800: not this provider's clock.
    old, new = provider_pair([a, field('mf', 1500), b], 'knmi', 1900)
    assert new['metadata']['observed_at']-old['metadata']['observed_at'] == 600


def test_future_receipt_and_future_product_are_not_used():
    a = field('knmi', 1000, 1100)
    future_receipt = field('knmi', 1300, 2000)
    future_product = field('knmi', 1800, 1400)
    assert provider_pair([a, future_receipt, future_product], 'knmi', 1500) is None


def test_repeated_valid_time_is_not_zero_delta_motion():
    a, b = field('knmi', 1000), field('knmi', 1300)
    repeated = field('knmi', 1300, 1400, 'same-time-other-receipt')
    old, new = provider_pair([a, b, repeated], 'knmi', 1500)
    assert old == a
    # Deterministic earliest-available version at each valid time.
    assert new == b


def test_incompatible_product_or_processing_not_mixed():
    a = field('dwd', 1000, product='hourly_rw')
    b = field('dwd', 1300, product='instantaneous')
    assert provider_pair([a, b], 'dwd', 1400) is None
    b = field('dwd', 1300, product='hourly_rw')
    b['metadata']['identity']['processing_version'] = 'other-decoder'
    assert provider_pair([a, b], 'dwd', 1400) is None


def test_product_gap_remains_unknown():
    assert provider_pair([field('knmi', 1000), field('knmi', 2500)],
                         'knmi', 2600, maximum_gap=1200) is None


def test_provider_age_anchors_reverse_trace_and_nodata_stays_unknown():
    from types import SimpleNamespace
    g = SimpleNamespace(origin_x_m=0., origin_y_m=5.,
                        x_resolution_m=1., y_resolution_m=1.)
    values = np.tile(np.arange(12, dtype=float), (5, 1))
    vectors = [{'current_x_m': x, 'current_y_m': 2.5,
                'vx_m_s': 1., 'vy_m_s': 0.} for x in (0., 5., 10.)]
    samples = provider_series(values, np.ones_like(values, bool), g, vectors,
                              product_at=1000., issued_at=1002.,
                              target_x=8.5, target_y=2.5, horizon_seconds=2,
                              step_seconds=1)
    assert [s['rate_mm_h'] for s in samples] == [6., 5., 4.]
    valid = np.zeros_like(values, bool)
    assert all(s['rate_mm_h'] is None for s in provider_series(
        values, valid, g, vectors, product_at=1000., issued_at=1002.,
        target_x=8.5, target_y=2.5, horizon_seconds=2, step_seconds=1))


def test_only_same_issue_times_can_be_combined():
    with pytest.raises(ValueError, match='unaligned_prediction_times'):
        combine_aligned_series({'knmi': [{'at': 1000, 'rate_mm_h': 1.}],
                                'mf': [{'at': 1005, 'rate_mm_h': 2.}]}, ['knmi', 'mf'])


def test_fixed_source_precedence_and_unknown_not_dry():
    series = {'knmi': [{'at': 1000, 'rate_mm_h': 0.},
                       {'at': 1060, 'rate_mm_h': None}],
              'mf': [{'at': 1000, 'rate_mm_h': 2.},
                     {'at': 1060, 'rate_mm_h': None}]}
    combined = combine_aligned_series(series, ['knmi', 'mf'])
    assert combined[0]['rate_mm_h'] == 0.
    assert combined[0]['provider_id'] == 'knmi'
    assert combined[1]['rate_mm_h'] is None
    assert combined[1]['reason'] == 'all_provider_support_unknown'


def test_unknown_primary_can_use_known_time_aligned_secondary():
    combined = combine_aligned_series(
        {'knmi': [{'at': 1000, 'rate_mm_h': None}],
         'mf': [{'at': 1000, 'rate_mm_h': .2}]}, ['knmi', 'mf'])
    assert combined[0]['rate_mm_h'] == .2
    assert combined[0]['provider_id'] == 'mf'


def test_frozen_json_config_restores_immutable_covariance_without_changing_policy():
    import dataclasses
    import json
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from storm_tracker_worker.tracking_core.engine.pyramidal_sparse_flow import SparsePyramidalConfig
    original = SparsePyramidalConfig(131072,384,6.,2,.8,1e-4,3,.8,3,20,20,.03,1e-4,1e6,0.,1e-6,((1.,0.),(0.,1.)))
    restored = restore_motion_config(json.loads(json.dumps(dataclasses.asdict(original))))
    assert restored.fingerprint == original.fingerprint
    assert restored.velocity_process_noise_covariance_m2_s2 == ((1.,0.),(0.,1.))
