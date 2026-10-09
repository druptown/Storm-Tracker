"""Small post-processing audit; no new radar replay or production changes."""
import hashlib
import json
import statistics
from datetime import datetime
from pathlib import Path

DEST = Path('/home/ikke/storm-tracker/.analysis/oldenburg-replay-20261008')

def epoch(value):
    return datetime.fromisoformat(value).timestamp()

def predicted_wet(record, key, when):
    # Treat absence as "no wet forecast", not a measured dry state.
    for passage in record.get(key, []):
        end = passage['exit_at']
        if end is None:
            if not passage.get('right_censored', key == 'overlap_union'):
                raise ValueError('unknown_end_not_horizon_censored')
            end = record['issued_at'] + 7200
        if passage['entry_at'] <= when <= end:
            return True
    return False

def confusion(records, frames, key, threshold):
    result = {}
    for lo, hi in ((5, 30), (30, 60), (60, 120)):
        counts = {k: 0 for k in ('hit', 'no_wet_forecast_on_wet', 'false_wet', 'no_wet_forecast_on_dry', 'pairs_with_no_candidate', 'pairs')}
        for record in records:
            for frame in frames:
                when = frame['metadata']['observed_at']
                lead = (when - record['issued_at']) / 60
                if not (lo <= lead < hi) or not frame['target_valid']:
                    continue
                wet = frame['target_rate_mm_h'] > 0 if threshold == 0 else frame['target_rate_mm_h'] >= threshold
                pred = predicted_wet(record, key, when)
                label = 'hit' if pred and wet else 'false_wet' if pred else 'no_wet_forecast_on_wet' if wet else 'no_wet_forecast_on_dry'
                counts[label] += 1
                counts['pairs'] += 1
                counts['pairs_with_no_candidate'] += not bool(record.get(key))
        result[f'{lo}-{hi}min'] = counts
    return result

def main():
    manifest = json.loads((DEST/'export-manifest.json').read_text())
    replay = json.loads((DEST/'replay-comparison.json').read_text())
    audit = json.loads((DEST/'event-audit.json').read_text())
    frames, records = manifest['frames'], replay['records']
    assert len(frames) == len(records) == 68
    assert not manifest['errors']
    assert not any('error' in record for record in records)
    for frame, record in zip(frames, records):
        assert frame['field_id'] == record['field_id']
        assert epoch(frame['available_at']) == record['issued_at']
        assert frame['metadata']['observed_at'] == record['product_at']
        assert record['issued_at'] >= record['product_at']
    original_hashes = json.loads((DEST/'files-sha256.json').read_text())
    mismatches = []
    for name, item in original_hashes.items():
        path = DEST/name
        if not path.exists() or hashlib.sha256(path.read_bytes()).hexdigest() != item['sha256']:
            mismatches.append(name)
    immutable_mismatches = [name for name in mismatches if name != 'progress.json']
    assert not immutable_mismatches, immutable_mismatches
    main_records = [record for record in records if 1791416700 <= record['product_at'] <= 1791431100]
    by_issue = {}
    for passage in audit['passages']:
        by_issue.setdefault(passage['forecast_issue_id'], []).append(passage)
    main_pre_issues = [issue for issue in audit['issues'] if 1791411900 <= epoch(issue['issued_at']) < 1791416700]
    main_pre_summary = []
    for issue in main_pre_issues:
        forecast = issue['forecast_metadata']['forecast']
        main_pre_summary.append({
            'issued_at': issue['issued_at'], 'reason': forecast['reason_code'],
            'entry_at': forecast['entry_at'], 'exit_at': forecast['exit_at'],
            'passage_count': len(by_issue.get(issue['forecast_issue_id'], []))})
    result = {
        'status': 'complete_offline_comparison',
        'production_changed': False,
        'replay_records': len(records),
        'replay_seconds': replay['total_seconds'],
        'replay_caught_errors': 0,
        'preserved_original_hash_entries': len(original_hashes),
        'immutable_hash_mismatches': immutable_mismatches,
        'expected_progress_hash_change': 'progress.json' in mismatches,
        'interval_counts': {key: sum(len(record.get(key, [])) for record in records) for key in ('baseline_candidates', 'overlap_union', 'global_mask_candidates')},
        'frames_with_candidates': {key: sum(bool(record.get(key)) for record in records) for key in ('baseline_candidates', 'global_mask_candidates')},
        'main_product_frames': len(main_records),
        'main_motion_support_median': statistics.median(record['motion_support_fraction'] for record in main_records),
        'main_pre_original_issues': main_pre_summary,
        'threshold_matched_pair_checks': {
            str(threshold): {key: confusion(records, frames, key, threshold) for key in ('baseline_candidates', 'overlap_union', 'global_mask_candidates')}
            for threshold in (0, .1)
        },
        'pair_check_interpretation': [
            'Descriptive forecast-product pairs from one event, not independent trials or a calibrated accuracy percentage.',
            'Only stored valid future radar products at 5-120 minute leads are verified; midpoint interval endpoints used, no interpolation through source gaps.',
            'No candidate means no wet forecast, not proof of dry ground or operational current-state classification.',
            'Right-censored support capped at the existing 120-minute horizon; no invented exit beyond it.',
            'Future fields are used solely as verifying observations, never predictors.',
            'Original immutable issues are the production baseline; replay lacks full initial state, scheduler and hysteresis.',
            'Nearby gauges are different measurement positions; exact target ground onset/end unavailable.'
        ],
    }
    (DEST/'comparison-metrics.json').write_text(json.dumps(result, indent=2))
    result.pop('main_pre_original_issues')
    print(json.dumps(result))

if __name__ == '__main__':
    main()
