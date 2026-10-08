"""Bounded downstream replay with immutable 106 trajectories, not new motion.

Only target episode/intensity semantics and its continuity state are replayed.
Reference observations enter scoring after all new forecasts are generated.
"""
from collections import Counter
from hashlib import sha256
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from storm_tracker_worker.thin_fusion import FusionCycle, FusedTargetObservation
from storm_tracker_worker.thin_nowcast import ThinNowcastRuntime
from storm_tracker_worker.thin_raw_code import source_tree_digest
from oldenburg_target_score import field_expectation, local


def replay_cached(records):
    region = records[0]['forecast']['radar_field_forecast']['region_id']
    position = (53.1435, 8.2146, .01)
    runtime = ThinNowcastRuntime(targets={'test_tracker': position})
    runtime.update_targets({'test_tracker': position}, target_region_ids={'test_tracker': region})
    provenance = {}
    for record in records:
        for c in record['forecast']['radar_field_forecast']['candidates']:
            # Held summaries deliberately refer to older products, not this tick.
            provenance[c['product_at']] = c
    output = []
    for record in records:
        issued, product = record['issued_at'], record['product_at']
        old = record['forecast']; samples = record['samples']
        assert (old['target_latitude'], old['target_longitude']) == position[:2]
        assert old['radar_field_forecast']['region_id'] == region
        assert len(samples) == 121 and samples[0]['at'] == issued
        assert all(s['at'] == issued+i*60 for i,s in enumerate(samples))
        p = provenance.get(product)
        assert product <= issued
        # The frozen sample series already contains source staleness, nodata,
        # flow abstentions and exact source-age offsets from the 106 engine.
        if p is None:
            # 106 held output can hide the current unusable source's provenance.
            # No timing information is lost: all archived trace samples are
            # unknown. Do not invent a native binding to fill that omission.
            assert all(s['rate_mm_h'] is None for s in samples)
            motions = ()
        else:
            assert p['available_at'] <= issued
            motion = SimpleNamespace(field=SimpleNamespace(observed_at=product),
                delta_seconds=p['pair_dt_seconds'], provenance=lambda p=p: dict(p))
            motions = (motion,)
        point = FusedTargetObservation('test_tracker',
            'operational' if old['precipitation_rate_mm_h'] is not None else 'unknown',
            issued, old['precipitation_rate_mm_h'], None, None, None, None, None,
            0, old['confidence'], tuple(old['contributors']), {'knmi_radar':product})
        with patch('storm_tracker_worker.thin_nowcast.trace_target', return_value=samples):
            result = runtime.advance(FusionCycle(issued,{'test_tracker':point},{},{},()),
                tracked_cells=(), source_motions={region:motions})
        output.append({**{k:v for k,v in record.items() if k != 'forecast'},
                       'forecast':result.target_results['test_tracker'].as_dict()})
    return output


def score(records, observations):
    counts = {str(th):{name:Counter() for name in ('episode','intensity')}
              for th in (0., .1)}
    for record in records:
        s = record['forecast']['radar_field_forecast']
        for obs in observations:
            at = obs['metadata']['observed_at']
            if not obs['target_valid'] or not 5 <= (at-record['issued_at'])/60 < 120:
                continue
            for mode in ('episode','intensity'):
                view = s if mode == 'episode' else {**s,'intervals':s.get('intensity_intervals',s['intervals'])}
                predicted = field_expectation(view,at)
                for threshold in (0., .1):
                    wet = obs['target_rate_mm_h'] > 0 if threshold == 0 else obs['target_rate_mm_h'] >= threshold
                    counts[str(threshold)][mode][predicted+('_on_wet' if wet else '_on_dry')] += 1
    return {th:{m:dict(c) for m,c in modes.items()} for th,modes in counts.items()}


def main():
    root = Path('/home/ikke/storm-tracker/.analysis/oldenburg-replay-20261008')
    dest = root/'episode-semantics107'
    dest.mkdir(exist_ok=True)
    assert not any(dest.iterdir()), 'Never overwrite completed or partial results'
    original = root/'target-runtime106/predictions.json'
    old_bytes = original.read_bytes(); baseline = json.loads(old_bytes)
    assert baseline['complete'] and len(baseline['records']) == 163
    hashes = {str(p.relative_to(root)):sha256(p.read_bytes()).hexdigest() for p in (
        original,root/'target-runtime106/policy.json',root/'source-clock-corrected/metrics.json',
        root/'rear-edge-diagnosis106/initial-motion-controls.json')}
    new = replay_cached(baseline['records'])
    (dest/'predictions.json').write_text(json.dumps({'complete':True,'records':new},indent=2,allow_nan=False))
    # Future radar is verification only, not an argument of replay_cached.
    reference = json.loads((root/'source-clock-corrected/metrics.json').read_text())
    result = dict(source_code=source_tree_digest(), input_hashes=hashes,
        method='new ThinNowcastRuntime issues using immutable106 source trajectories; motion NOT recomputed',
        issues=len(new), old_scores=score(baseline['records'],reference['source_timed_observations']),
        new_scores=score(new,reference['source_timed_observations']),
        states=dict(Counter(r['forecast']['forecast_state'] for r in new)),
        supports=dict(Counter(r['forecast']['radar_field_forecast']['support_state'] for r in new)),
        diagnosed_issue={name:{'issue':local(r['issued_at']),'entry':local(r['forecast']['entry_at']),
            'exit':local(r['forecast']['exit_at']), 'field':r['forecast']['radar_field_forecast']}
            for name,r in (('old',baseline['records'][60]),('new',new[60]))},
        limits=['One KNMI event, dependent pairs, not a calibrated probability',
                'Positive QPE is radar indication, exact local ground truth unknown',
                'Only downstream semantics/hold state replayed, original motion unchanged',
                'All-unknown traces lacking current provenance supplied as absent motion; no invented bindings',
                'No intensity cores replayed; preservation covered by focused regressions'])
    for name,expected in hashes.items():
        assert sha256((root/name).read_bytes()).hexdigest() == expected
    (dest/'metrics.json').write_text(json.dumps(result,indent=2,allow_nan=False))
    print(json.dumps({k:result[k] for k in ('issues','old_scores','new_scores','states','supports')}))


if __name__ == '__main__':
    main()
