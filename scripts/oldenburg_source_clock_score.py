"""Score the corrected fixed-source trial against later source-timed QPE."""
import collections
import hashlib
import json
import math
import sys
import zlib
from datetime import datetime
from io import BytesIO
from pathlib import Path
import numpy as np

from oldenburg_field_probe import passage_intervals
from oldenburg_field_score import pair_scores, local_time


def reference_events(frames, threshold):
    events, active, previous = [], None, None
    for f in frames:
        when = f['metadata']['observed_at']
        rate = f['target_rate_mm_h']
        if rate is None:
            if active is not None:
                active.update(exit_bracket=None, end_status='coverage_censored')
                events.append(active)
                active = None
            previous = None
            continue
        gap = 0 if previous is None else when-previous
        wet = rate > 0 if threshold == 0 else rate >= threshold
        if wet and active is None:
            active = {'entry_bracket': None if previous is None else [previous, when],
                      'first_wet_at': when, 'last_wet_at': when, 'peak_mm_h': rate,
                      'maximum_adjacent_product_gap_seconds': gap}
        if active is not None:
            active['maximum_adjacent_product_gap_seconds'] = max(
                active['maximum_adjacent_product_gap_seconds'], gap)
            if wet:
                active.update(last_wet_at=when, peak_mm_h=max(active['peak_mm_h'], rate))
            else:
                active.update(exit_bracket=[active['last_wet_at'], when], end_status='bracketed')
                events.append(active)
                active = None
        previous = when
    if active is not None:
        active.update(exit_bracket=None, end_status='window_censored')
        events.append(active)
    return events


def recorded_passage_expectation(passages, when):
    unknown = False
    for p in passages:
        if when < p['entry_at']:
            continue
        if p['exit_at'] is None:
            unknown = True
        elif when <= p['exit_at']:
            return 'issued_wet_interval'
    return 'unknown_censored_end' if unknown else 'no_issued_wet_interval'


def ep(value):
    return None if value is None else datetime.fromisoformat(value).timestamp()


def main():
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from storm_tracker_worker.thin_provider_adapters import ThinRadarField
    from storm_tracker_worker.thin_regional_radar_fusion import project_radar_field
    from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid
    root = Path('/home/ikke/storm-tracker/.analysis/oldenburg-replay-20261008')
    dest = root / 'source-clock-corrected'
    probe = json.loads((dest / 'predictions.json').read_text())
    assert probe['complete'] and len(probe['records']) == 68, 'Trial still running: do not score as completed'
    assert not (dest / 'metrics.json').exists(), 'Preserve completed corrected score; do not overwrite'
    policy = probe['policy']
    assert hashlib.sha256((Path(__file__).parent / 'oldenburg_source_clock_probe.py').read_bytes()).hexdigest() == policy['script_sha256']
    assert hashlib.sha256((dest / 'native-manifest.json').read_bytes()).hexdigest() == policy['native_manifest_sha256']
    assert policy['providers_in_fixed_precedence'] == ['knmi_radar'], 'This score is the declared KNMI control only'
    hashes = json.loads((root / 'files-sha256.json').read_text())
    mismatch = [name for name, ref in hashes.items() if name != 'progress.json'
                and hashlib.sha256((root / name).read_bytes()).hexdigest() != ref['sha256']]
    assert not mismatch, mismatch
    manifest = json.loads((root / 'export-manifest.json').read_text())
    native = json.loads((dest / 'native-manifest.json').read_text())
    audit = json.loads((root / 'event-audit.json').read_text())
    old = json.loads((root / 'replay-comparison.json').read_text())
    baseline = {r['field_id']: r for r in old['records']}
    native_by_id = {f['field_id']: f for f in native['fields']}
    verified_payloads = set()
    for f in native['fields']:
        for ref in f['arrays'].values():
            if ref['file'] not in verified_payloads:
                assert hashlib.sha256((dest / ref['file']).read_bytes()).hexdigest() == ref['sha256']
                verified_payloads.add(ref['file'])
    for frame, r in zip(manifest['frames'], probe['records']):
        assert frame['field_id'] == r['field_id']
        assert ep(frame['available_at']) == r['issued_at']
        for p, item in r['provider_predictions'].items():
            if 'new_field_id' not in item:
                continue
            older, newer = [native_by_id[item[k]] for k in ('old_field_id', 'new_field_id')]
            assert older['metadata']['provider_id'] == newer['metadata']['provider_id'] == p
            assert older['available_at'] <= r['issued_at'] and newer['available_at'] <= r['issued_at']
            assert item['product_at'] == newer['metadata']['observed_at'] <= r['issued_at']
            assert item['pair_dt_seconds'] == newer['metadata']['observed_at']-older['metadata']['observed_at']
            assert 0 < item['pair_dt_seconds'] <= 1200
        assert len(r['samples']) == 121
        assert all(s['at'] >= r['issued_at'] and (s['rate_mm_h'] is None or math.isfinite(s['rate_mm_h'])) for s in r['samples'])
    fm = manifest['frames'][0]['metadata']
    grid = AnalysisGrid(fm['identity']['grid_id'], fm['crs'], *fm['affine'], 340, 340)
    unique = {}
    for f in native['fields']:
        if f['metadata']['provider_id'] == 'knmi_radar':
            unique.setdefault(f['metadata']['observed_at'], f)
    observations = []
    for at, f in sorted(unique.items()):
        arrays = {key: np.load(BytesIO(zlib.decompress((dest / ref['file']).read_bytes())), allow_pickle=False)
                  for key, ref in f['arrays'].items()}
        for key, a in arrays.items():
            assert hashlib.sha256(a.tobytes()).hexdigest() == f['arrays'][key]['descriptor']['sha256']
        m = f['metadata']
        layer = project_radar_field(ThinRadarField(m['provider_id'], m['product_id'], at,
            m['crs'], *m['affine'], arrays['values'], arrays['valid_mask']), grid)
        valid = bool(layer.valid_mask[170,170])
        observations.append({'field_id': f['field_id'], 'metadata': {'observed_at': at},
                             'provider_id': m['provider_id'], 'available_at': f['available_at'],
                             'target_valid': valid,
                             'target_rate_mm_h': float(layer.rain_rate_mm_h[170,170]) if valid else None})
    issues = {i['forecast_issue_id']: i for i in audit['issues']}
    links = {(l['cycle_id'], l['field_id']): l['forecast_issue_id'] for l in audit['links']}
    passages = collections.defaultdict(list)
    for p in audit['passages']:
        passages[p['forecast_issue_id']].append({**p, 'entry_at': ep(p['entry_at']), 'exit_at': ep(p['exit_at'])})
    actual_issues = []
    original_pair_counts = {str(th): collections.Counter() for th in (0., .1)}
    summaries = []
    for f, r in zip(manifest['frames'], probe['records']):
        iid = links[(f['cycle_id'], f['field_id'])]
        i = issues[iid]
        assert ep(i['issued_at']) == r['issued_at']
        original = {'issue_id': iid, 'issued_at': i['issued_at'],
                    'forecast': i['forecast_metadata']['forecast'],
                    'stored_passages': passages[iid]}
        actual_issues.append(original)
        summaries.append({'issue_local': local_time(r['issued_at']),
                          'native_product_local': local_time(r['provider_predictions']['knmi_radar'].get('product_at')),
                          'status': r['prediction_status'], 'known_samples': r['known_sample_count'],
                          'weak_intervals': r['weak_passages'], 'policy_intervals': r['passages'],
                          'original_immutable_issue_id': iid,
                          'original_forecast': original['forecast'], 'original_stored_passages': passages[iid]})
        for observation in observations:
            when = observation['metadata']['observed_at']
            if not 5 <= (when-r['issued_at'])/60 < 120 or not observation['target_valid']:
                continue
            state = recorded_passage_expectation(passages[iid], when)
            for threshold in (0., .1):
                wet = observation['target_rate_mm_h'] > 0 if threshold == 0 else observation['target_rate_mm_h'] >= threshold
                original_pair_counts[str(threshold)][state + ('_on_wet' if wet else '_on_dry')] += 1
    result = {
        'status': 'complete_corrected_single_source_control', 'production_changed': False,
        'qualification': 'Provider identity and product clocks explicit; exploratory meteorological method, not calibrated or production-qualified',
        'records': len(probe['records']), 'processing_seconds': probe['total_seconds'],
        'status_counts': dict(collections.Counter(r['prediction_status'] for r in probe['records'])),
        'errors': [r['errors'] for r in probe['records'] if r.get('errors')],
        'immutable_original_entries_checked': len(hashes)-1, 'immutable_hash_mismatches': mismatch,
        'native_payload_hashes_checked': len(verified_payloads), 'policy': policy,
        'reference': 'KNMI-only projected quantitative local QPE with own valid times, NOT exact target ground truth; later fields used ONLY for scoring',
        'source_timed_observations': observations,
        'observed_weak_events': reference_events(observations, 0.),
        'observed_policy_events': reference_events(observations, .1),
        'threshold_matched_pair_checks': {str(th): pair_scores(probe['records'], baseline, observations, th) for th in (0., .1)},
        'legacy_fused_reference_sensitivity_only': {str(th): pair_scores(probe['records'], baseline, manifest['frames'], th) for th in (0., .1)},
        'recorded_production_passage_expectations': {k: dict(v) for k,v in original_pair_counts.items()},
        'original_immutable_issues_matched': actual_issues,
        'issued_predictions': summaries,
        'limits': [
            'Only KNMI control executed; other native source arrays preserved but no combined-provider forecast claimed.',
            'Offline object intervals are a separately labelled comparison; immutable original issues are retained as production baseline, never verification truth.',
            'No issued wet interval is not ground dry truth; unknown/censored original ends are never replaced with a invented end.',
            'Dependent forecast-observation pairs from one event are descriptive, not calibrated accuracy or probability.',
            'Unknown motion/coverage remains abstention, not dry; compare availability and false/missed wet expectations together.',
            'Observation brackets reflect actual product gaps; no future interpolation into predictors or exact onset/continuity between unobserved scans.',
            'Nearby hourly ground accumulations are not exact target onset; no exact ground passage truth.',
            'Stationary two-hour optical-flow persistence does not model growth/decay or multiscale evolution.',
        ],
    }
    (dest / 'metrics.json').write_text(json.dumps(result, indent=2))
    executed = dest / 'executed-source'
    for name in ('oldenburg_source_clock_score.py', 'test_oldenburg_source_clock_score.py'):
        (executed / name).write_bytes((Path(__file__).parent / name).read_bytes())
    artifact_hashes = {str(p.relative_to(dest)): hashlib.sha256(p.read_bytes()).hexdigest()
                       for p in dest.rglob('*') if p.is_file() and p.name not in ('progress.json', 'artifact-hashes.json')}
    (dest / 'artifact-hashes.json').write_text(json.dumps(artifact_hashes, indent=2))
    print(json.dumps({k: result[k] for k in ('status', 'records', 'processing_seconds', 'status_counts',
                                            'errors', 'observed_weak_events', 'observed_policy_events',
                                            'threshold_matched_pair_checks', 'recorded_production_passage_expectations')}))


if __name__ == '__main__':
    main()
