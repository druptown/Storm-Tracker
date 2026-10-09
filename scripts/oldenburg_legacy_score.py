"""Verification only; later observed fields never enter the legacy predictor."""
import argparse
import collections
import hashlib
import json
from pathlib import Path

from oldenburg_legacy_compare import ROOT, compare_pairs, eligible_fields, verify_completed_hashes
from oldenburg_field_score import bracket_error_minutes, local_time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args()
    dest = args.output_dir
    assert not (dest/'metrics.json').exists(), 'Preserve prior score'
    source = ROOT/'source-clock-corrected'
    old = json.loads((dest/'predictions.json').read_text())
    new = json.loads((source/'predictions.json').read_text())
    reference = json.loads((source/'metrics.json').read_text())
    native = json.loads((source/'native-manifest.json').read_text())
    assert old['complete'] and new['complete']
    assert len(old['records']) == len(new['records']) == 68
    for name, expected in (
            ('predictions.json', old['policy']['new_predictions_sha256']),
            ('native-manifest.json', old['policy']['native_manifest_sha256'])):
        assert hashlib.sha256((source/name).read_bytes()).hexdigest() == expected
    original = json.loads((source/'final-manifest.json').read_text())
    checked = verify_completed_hashes(source, original)
    assert checked == 9
    consumed = set()
    by_id = {f['field_id']: f for f in native['fields']}
    for earlier, later in zip(old['records'], new['records']):
        assert earlier['field_id'] == later['field_id']
        assert earlier['issued_at'] == later['issued_at']
        eligible = eligible_fields(native['fields'], earlier['issued_at'])
        assert earlier['eligible_native_fields'] == len(eligible)
        for f in earlier['newly_consumed']:
            assert f['field_id'] not in consumed
            consumed.add(f['field_id'])
            actual = by_id[f['field_id']]
            assert f['available_at'] == actual['available_at'] <= earlier['issued_at']
            assert f['product_at'] == actual['metadata']['observed_at'] <= earlier['issued_at']
        new_input = later['provider_predictions']['knmi_radar'].get('new_field_id')
        if new_input:
            assert eligible[-1]['field_id'] == new_input, 'Both cores must share latest eligible KNMI input'
        assert earlier['published_end_at'] is None
        assert all('mean_mm_h' not in p and 'peak_mm_h' not in p
                   for p in earlier['adapter_derived_intervals'])
    observations = reference['source_timed_observations']
    main_event = max(reference['observed_policy_events'],
                     key=lambda e: e['last_wet_at']-e['first_wet_at'])
    issue_rows = []
    for earlier, later in zip(old['records'], new['records']):
        issued = earlier['issued_at']
        old_first = next(iter(earlier['adapter_derived_intervals']), {})
        new_first = next(iter(later['passages']), {})
        motion = earlier['selected_motion']
        contour_at = None if motion.get('eta_minutes') is None else issued+motion['eta_minutes']*60
        row = {
            'issued_at': issued, 'issue_local': local_time(issued),
            'old_original_publication': earlier['published_status'],
            'old_contour_eta_local': local_time(contour_at),
            'old_eta_basis': motion.get('eta_basis'),
            'old_centroid_pass_local': local_time(None if motion.get('closest_pass_minutes') is None
                                                 else issued+motion['closest_pass_minutes']*60),
            'old_adapter_first_interval': old_first or None,
            'new_first_interval': new_first or None,
            'old_known_geometry_samples': earlier['known_geometry_samples'],
            'new_known_samples': later['known_sample_count'],
            'old_motion': earlier['selected_storm'],
        }
        if issued < main_event['first_wet_at']:
            row['pre_main_event'] = True
            row['old_first_interval_entry_error_minutes_vs_main'] = bracket_error_minutes(
                old_first.get('entry_at'), main_event['entry_bracket'])
            row['new_first_interval_entry_error_minutes_vs_main'] = bracket_error_minutes(
                new_first.get('entry_at'), main_event['entry_bracket'])
            row['arrival_error_caveat'] = 'First interval only, not hindsight-matched; may be false/current-wet interval, inspect censoring and all-pair false-wet counts'
        issue_rows.append(row)
    result = {
        'status': 'complete_controlled_legacy_core_comparison', 'production_changed': False,
        'issues': 68, 'native_fields_consumed_once': len(consumed),
        'original_final_manifest_entries_checked': checked,
        'old_processing_seconds': old['elapsed_seconds'],
        'old_original_publication_available': sum(r['published_status']['forecast_available'] for r in old['records']),
        'old_publication_block_reasons': dict(collections.Counter(
            r['published_status'].get('forecast_block_reason') or 'none' for r in old['records'])),
        'old_geometry_full_issues': sum(r['known_geometry_samples'] == 91 for r in old['records']),
        'old_geometry_unknown_issues': sum(r['known_geometry_samples'] == 0 for r in old['records']),
        'new_full_issues': sum(r['known_sample_count'] == 121 for r in new['records']),
        'new_partial_issues': sum(0 < r['known_sample_count'] < 121 for r in new['records']),
        'new_unknown_issues': sum(r['known_sample_count'] == 0 for r in new['records']),
        'reference': 'Later source-timed KNMI local QPE, NOT exact ground truth',
        'policy': old['policy'], 'main_policy_event': main_event,
        'observed_weak_events': reference['observed_weak_events'],
        'observed_policy_events': reference['observed_policy_events'],
        'paired_threshold_checks': {str(t): compare_pairs(old['records'], new['records'], observations, t)
                                    for t in (0., .1)},
        'issued_predictions': issue_rows,
        'limits': [
            'Old geometry samples/intervals are diagnostic adapter outputs, not old published arrival/end forecasts.',
            'Old has no native forecast-end output and quantitative adapters do not invent reflectivity.',
            'Old original published closest-centroid passage is not first-rain arrival.',
            'The two cores share then-available KNMI QPE and projection, not full original source-policy inputs.',
            'Legacy threshold >=0.1 excludes weak >0 echoes by construction; weak score is a diagnostic not threshold-matched ranking.',
            'Ninety-to-120-minute lead is outside old horizon; compare 5-to-90-minute leads separately.',
            'Forecast/observation pairs are dependent within a single event; no calibrated accuracy percentage.',
            'Nearby station hour accumulations and KNMI pixels are not exact target ground onset/end.',
            'Original source/input artifacts retained; source receipts/errors/gaps remain historical limitations.',
        ],
    }
    (dest/'metrics.json').write_text(json.dumps(result, indent=2))
    (dest/'executed-score.py').write_bytes(Path(__file__).read_bytes())
    print(json.dumps({k: result[k] for k in ('status', 'issues', 'native_fields_consumed_once',
        'old_processing_seconds', 'old_original_publication_available', 'old_publication_block_reasons',
        'old_geometry_full_issues', 'new_full_issues', 'new_partial_issues', 'new_unknown_issues',
        'paired_threshold_checks')}, indent=2))


if __name__ == '__main__':
    main()
