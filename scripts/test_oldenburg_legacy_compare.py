"""Behavior checks for the offline legacy boundary, not source-text checks."""
from pathlib import Path

import numpy as np
import pytest

from oldenburg_legacy_compare import eligible_fields, LegacyHarness, paired_label, compare_pairs, geometry_intervals, verify_completed_hashes

LEGACY = Path('/tmp/storm-legacy-0498-LS35Lb/custom_components/storm_tracker_v3')


def field(fid, product, available, provider='knmi_radar'):
    return {'field_id': fid, 'available_at': available,
            'metadata': {'provider_id': provider, 'observed_at': product}}


def test_future_receipts_future_valid_times_and_other_sources_are_excluded():
    # Removing either causal guard would admit b or c.
    rows = [field('a', 100, 110), field('b', 100, 130),
            field('c', 130, 110), field('d', 100, 110, 'dwd_radolan')]
    assert [r['field_id'] for r in eligible_fields(rows, 120)] == ['a']


def test_repeated_product_does_not_manufacture_a_history_frame():
    rows = [field('b', 100, 115), field('a', 100, 110), field('c', 105, 116)]
    assert [r['field_id'] for r in eligible_fields(rows, 120)] == ['a', 'c']


def test_adapter_uses_real_legacy_threshold_without_inventing_dbz():
    harness = LegacyHarness(LEGACY, 1000.)
    values = np.array([[.099, .1, .6], [np.nan, 10., .2]])
    valid = np.array([[True, True, True], [False, False, True]])
    obs = harness.observations(values, valid, lambda r, c: (r, c),
                               product_at=900., pixel_area_km2=2.25)
    assert len(obs) == 1
    assert obs[0].area_km2 == 6.75  # Three real, four-connected pixels.
    assert obs[0].max_dbz is None
    assert obs[0].timestamp == 900.
    assert obs[0].intensity == 2
    assert obs[0].lat == pytest.approx(5 / 6)
    assert obs[0].lon == pytest.approx(13 / 6)


def test_no_track_is_unknown_not_a_dry_future_or_an_invented_end():
    harness = LegacyHarness(LEGACY, 1000.)
    result = harness.evaluate(1000., 53.1435, 8.2146)
    assert result['selected_geometry_samples'][0]['wet'] is None
    assert result['published_end_at'] is None
    assert not result['published_status']['forecast_available']


def test_real_legacy_history_tracks_translation_and_does_not_unlock_dbz_forecast():
    harness = LegacyHarness(LEGACY, 1000.)
    for index in range(6):
        values = np.zeros((8, 30))
        values[2:5, 2 + index:5 + index] = .2
        # Independent small metric-to-geographic fixture: eastward 1 km/5 min.
        corner = lambda r, c: (53.1435 + (4-r)/110.574,
                              8.2146 + (c-20)/(111.32*np.cos(np.deg2rad(53.1435))))
        at = 1000. + index * 300.
        obs = harness.observations(values, np.ones(values.shape, bool), corner,
                                   product_at=at, pixel_area_km2=1.)
        harness.process(obs, at + 30.)
    result = harness.evaluate(2530., 53.1435, 8.2146)
    assert result['selected_motion']['eta_minutes'] is not None
    assert result['selected_storm']['speed_kmh'] == pytest.approx(12., abs=.2)
    assert result['published_status']['forecast_block_reason'] == 'intensiteit_onbekend'
    assert result['published_end_at'] is None
    assert any(s['wet'] is True for s in result['selected_geometry_samples'])


def test_pair_classification_keeps_abstention_separate_from_missed_rain():
    assert paired_label(None, True) == 'unknown_on_wet'
    assert paired_label(False, True) == 'miss'
    assert paired_label(True, False) == 'false_wet'


def test_paired_score_compares_same_times_and_reports_both_supported_subset():
    old = [{'issued_at': 0., 'selected_geometry_samples':
            [{'at': 600., 'wet': None}, {'at': 900., 'wet': True}]}]
    new = [{'issued_at': 0., 'samples':
            [{'at': 600., 'rate_mm_h': .2}, {'at': 900., 'rate_mm_h': .2}]}]
    refs = [{'metadata': {'observed_at': 600.}, 'target_valid': True, 'target_rate_mm_h': .2},
            {'metadata': {'observed_at': 900.}, 'target_valid': True, 'target_rate_mm_h': 0.}]
    score = compare_pairs(old, new, refs, .1)['5-30min']
    assert score['old_all']['unknown_on_wet'] == 1
    assert score['new_all']['hit'] == 1
    assert score['new_all']['false_wet'] == 1
    assert score['common_known_pairs'] == 1
    assert score['old_on_common']['false_wet'] == 1
    assert score['new_on_common']['hit'] == 0


def test_invalid_observation_is_not_counted_as_ground_dry():
    old = [{'issued_at': 0., 'selected_geometry_samples': [{'at': 600., 'wet': True}]}]
    new = [{'issued_at': 0., 'samples': [{'at': 600., 'rate_mm_h': .2}]}]
    refs = [{'metadata': {'observed_at': 600.}, 'target_valid': False, 'target_rate_mm_h': None}]
    assert compare_pairs(old, new, refs, .1)['5-30min']['forecast_observation_pairs'] == 0


def test_geometry_interval_has_times_but_never_a_fabricated_rain_rate():
    result = geometry_intervals([{'at': 0., 'wet': False}, {'at': 60., 'wet': True},
                                 {'at': 120., 'wet': True}, {'at': 180., 'wet': False}])
    assert len(result) == 1
    assert result[0]['entry_at'] == 30.
    assert result[0]['exit_at'] == 150.
    assert 'mean_mm_h' not in result[0]
    assert 'peak_mm_h' not in result[0]


def test_historical_factories_and_empty_batch_quirk_are_preserved():
    harness = LegacyHarness(LEGACY, 1000.)
    obs = harness.observations(np.ones((1, 1)), np.ones((1, 1), bool),
                               lambda r, c: (53+r/1000, 8+c/1000),
                               product_at=1000., pixel_area_km2=1.)
    harness.process(obs, 1030.)
    storm = harness.engine.get_storms()[0]
    assert storm.first_seen == 1030.
    assert storm.last_update == 1030.
    harness.process([], 3000.)
    assert not storm.is_dormant  # Old empty batch returns before expiration.
    assert len(harness.engine.get_active_storms()) == 1


def test_completed_manifest_checks_real_files_and_rejects_tampering(tmp_path):
    import hashlib
    path = tmp_path / 'a.json'
    path.write_bytes(b'original')
    manifest = {'current_files': {'a.json': hashlib.sha256(b'original').hexdigest()}}
    assert verify_completed_hashes(tmp_path, manifest) == 1
    path.write_bytes(b'changed')
    with pytest.raises(AssertionError):
        verify_completed_hashes(tmp_path, manifest)
