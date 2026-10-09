"""Score new historical predictions against later observations, not old forecasts."""
import collections
import hashlib
import json
import math
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from oldenburg_field_probe import passage_intervals
from oldenburg_final_comparison import predicted_wet

DEST = Path('/home/ikke/storm-tracker/.analysis/oldenburg-replay-20261008')


def sample_prediction(samples, when):
    if not samples or when < samples[0]['at'] or when > samples[-1]['at']:
        return None, 'outside_prediction_support'
    nearest = min(samples, key=lambda sample: abs(sample['at'] - when))
    if abs(nearest['at'] - when) > 30.000001:
        return None, 'prediction_sampling_gap'
    return nearest['rate_mm_h'], nearest.get('reason')


def classification(rate, observed_rate, threshold):
    observed_wet = observed_rate > 0 if threshold == 0 else observed_rate >= threshold
    if rate is None:
        return 'unknown_on_wet' if observed_wet else 'unknown_on_dry'
    predicted = rate > 0 if threshold == 0 else rate >= threshold
    return ('hit' if observed_wet else 'false_wet') if predicted else ('miss' if observed_wet else 'correct_dry')


def bracket_error_minutes(predicted, bracket):
    if predicted is None or bracket is None or any(value is None for value in bracket):
        return None
    return [(predicted-bracket[1])/60., (predicted-bracket[0])/60.]


def local_time(value):
    return None if value is None else datetime.fromtimestamp(value, ZoneInfo('Europe/Brussels')).isoformat()


def pair_scores(records, baseline, frames, threshold):
    output = {}
    labels = ('hit', 'miss', 'false_wet', 'correct_dry', 'unknown_on_wet', 'unknown_on_dry')
    for lower, upper in ((5, 30), (30, 60), (60, 120)):
        new = dict.fromkeys(labels, 0)
        old_all = dict.fromkeys(labels, 0)
        old_supported = dict.fromkeys(labels, 0)
        reasons = collections.Counter()
        errors = []
        for record in records:
            old = baseline[record['field_id']]
            for frame in frames:
                when = frame['metadata']['observed_at']
                lead = (when-record['issued_at'])/60.
                if not lower <= lead < upper or not frame['target_valid']:
                    continue
                observed = frame['target_rate_mm_h']
                rate, reason = sample_prediction(record['samples'], when)
                new[classification(rate, observed, threshold)] += 1
                # The old algorithm's intervals are comparison output, never truth.
                old_rate = 2. if predicted_wet(old, 'baseline_candidates', when) else 0.
                label = classification(old_rate, observed, threshold)
                old_all[label] += 1
                if rate is None:
                    reasons[reason] += 1
                else:
                    old_supported[label] += 1
                    errors.append(rate-observed)
        known = len(errors)
        total = sum(new.values())
        output[f'{lower}-{upper}min'] = {
            'forecast_observation_pairs': total,
            'new_field': new, 'old_object_candidates_all_pairs': old_all,
            'old_object_candidates_on_new_supported_pairs': old_supported,
            'new_known_pairs': known,
            'new_unknown_pairs': total-known,
            'unknown_reasons': dict(reasons),
            'new_radar_rate_mae_mm_h': None if not errors else sum(abs(e) for e in errors)/known,
            'new_radar_rate_rmse_mm_h': None if not errors else math.sqrt(sum(e*e for e in errors)/known),
        }
    return output


def main():
    manifest = json.loads((DEST/'export-manifest.json').read_text())
    probe = json.loads((DEST/'field-motion-probe.json').read_text())
    previous = json.loads((DEST/'replay-comparison.json').read_text())
    references = json.loads((DEST/'metrics.json').read_text())
    assert probe['complete'], 'Do not score a running partial experiment as complete'
    assert len(probe['records']) == len(manifest['frames']) == 68
    baseline = {record['field_id']: record for record in previous['records']}
    source_hashes = json.loads((DEST/'files-sha256.json').read_text())
    mismatches = []
    for name, item in source_hashes.items():
        if name == 'progress.json':
            continue
        path = DEST/name
        if not path.exists() or hashlib.sha256(path.read_bytes()).hexdigest() != item['sha256']:
            mismatches.append(name)
    assert not mismatches, mismatches
    for frame, record in zip(manifest['frames'], probe['records']):
        assert frame['field_id'] == record['field_id']
        assert frame['metadata']['observed_at'] == record['product_at']
        assert datetime.fromisoformat(frame['available_at']).timestamp() == record['issued_at']
        assert record['product_at'] <= record['issued_at']
        for sample in record['samples']:
            assert sample['at'] >= record['issued_at']
            rate = sample['rate_mm_h']
            assert rate is None or math.isfinite(rate)
    issue_summaries = []
    for record in probe['records']:
        weak = passage_intervals(record['samples'], threshold=math.nextafter(0., 1.))
        issue_summaries.append({
            'product_local': local_time(record['product_at']),
            'issue_local': local_time(record['issued_at']),
            'status': record['prediction_status'],
            'known_samples': record.get('known_sample_count', 0),
            'vector_count': len(record.get('converged_vectors', [])),
            'weak_positive_intervals': weak,
            'policy_intervals': record['passages'],
            'old_object_intervals': baseline[record['field_id']]['baseline_candidates'],
        })
    result = {
        'status': 'complete_offline_field_benchmark',
        'production_changed': False,
        'verification_truth': 'Later archived local quantitative radar; exact ground truth at target unavailable',
        'records': len(probe['records']), 'processing_seconds': probe['total_seconds'],
        'status_counts': dict(collections.Counter(record['prediction_status'] for record in probe['records'])),
        'errors': [record for record in probe['records'] if 'error' in record],
        'immutable_hash_entries_checked': len(source_hashes)-1,
        'immutable_hash_mismatches': mismatches,
        'policy': probe['policy'],
        'observed_weak_events': references['weak_positive_events'],
        'observed_policy_events': references['policy_threshold_events'],
        'threshold_matched_pair_checks': {
            str(threshold): pair_scores(probe['records'], baseline, manifest['frames'], threshold)
            for threshold in (0., .1)
        },
        'issued_predictions': issue_summaries,
        'interpretation_limits': [
            'New forecasts regenerated chronologically from then-available inputs; later observations used only by this scoring stage.',
            'Old forecasts are baseline comparisons, not verification truth or a constraint on the new method.',
            'Unknown support is counted separately from dry prediction and missed wet prediction; compare coverage together with conditional skill.',
            'One event and strongly dependent forecast-observation pairs; no calibrated accuracy/probability percentage.',
            'Nearest 60-second prediction sample with at most 30-second quantization; no interpolation across missing predictions or observed product gaps.',
            'Thresholds >0 and >=0.1mm/h distinguish very weak echoes from existing passage policy.',
            'Deterministic stationary motion persistence; growth/decay, multiscale evolution and ensemble uncertainty not modeled.',
            'Not an end-to-end production simulation: scheduler, live admission state and nowcast hysteresis not reconstructed.',
            'Nearby stations are separate point observations and hourly rain accumulation does not timestamp exact target onset.',
            'Censored interval ends remain unknown; no fabricated full-zone duration beyond the two-hour horizon.',
        ],
    }
    audit_path = DEST/'source-mask-audit.json'
    if audit_path.exists():
        mask_audit = json.loads(audit_path.read_text())
        result['method_qualification'] = 'not_qualified_cross_frame_source_mask_assumption'
        result['source_mask_audit'] = mask_audit
        result['interpretation_limits'].insert(0,
            'Contributor-mask values are layer-index codes, not stable provider IDs; direct cross-frame equality in this probe is invalid. Forecast-support and skill counts are preliminary pending identity-corrected replay.')
    else:
        result['method_qualification'] = 'exploratory_not_calibrated'
    (DEST/'field-probe-metrics.json').write_text(json.dumps(result, indent=2))
    print(json.dumps({key: value for key, value in result.items()
                      if key in ('status', 'records', 'processing_seconds', 'status_counts',
                                 'immutable_hash_mismatches', 'threshold_matched_pair_checks')}))


if __name__ == '__main__':
    main()
