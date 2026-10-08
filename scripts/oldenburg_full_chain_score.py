"""Assess new full-runtime issues against later radar, never feed truth back."""
import argparse
from collections import Counter, defaultdict
from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path

from oldenburg_target_score import field_expectation, original_expectation, local


BANDS = ((5,30),(30,60),(60,120))


def issued_expectation(record,at,mode='episode'):
    summary = record['forecast']['radar_field_forecast']
    if mode != 'kernel_windows' and summary is not None:
        view = summary if mode == 'episode' else {
            **summary,'intervals':summary.get('intensity_intervals',[])}
        return field_expectation(view,at)
    # Missing cell warnings are not explicit dry predictions. Censored cell
    # windows likewise do not imply indefinitely continuing rain.
    intervals = [p for p in record.get('passages',[])
        if p['method'] != 'source_clock_full_field']
    result = original_expectation(intervals,at)
    return 'wet' if result == 'issued_wet_interval' else 'unknown'


def compare(records,observations,old_by_time,old_passages):
    scores = {str(th):{f'{lo}-{hi}min':{m:Counter() for m in
        ('episode','intensity','kernel_windows','original')} for lo,hi in BANDS}
        for th in (0.,.1)}
    for record in records:
        issued = record['issued_at']; old = old_by_time[issued]
        for obs in observations:
            if not obs['target_valid']: continue
            at = obs['metadata']['observed_at']; lead = (at-issued)/60
            band = next((f'{lo}-{hi}min' for lo,hi in BANDS if lo <= lead < hi),None)
            if band is None: continue
            states = {m:issued_expectation(record,at,m) for m in
                ('episode','intensity','kernel_windows')}
            states['original'] = original_expectation(old_passages[old['forecast_issue_id']],at)
            for th in (0.,.1):
                wet = obs['target_rate_mm_h'] > 0 if th == 0. else obs['target_rate_mm_h'] >= th
                for mode,state in states.items():
                    scores[str(th)][band][mode][state+('_on_wet' if wet else '_on_dry')] += 1
    return {th:{b:{m:dict(c) for m,c in modes.items()} for b,modes in bands.items()}
            for th,bands in scores.items()}


def digest(path): return sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args = parser.parse_args(); root,dest = args.data,args.output
    assert not (dest/'metrics.json').exists(), 'Never overwrite assessed results'
    progress = json.loads((dest/'progress.json').read_text())
    policy = json.loads((dest/'policy.json').read_text())
    predictions = json.loads((dest/'predictions.json').read_text())
    audit = json.loads((root/'event-audit.json').read_text())
    assert progress['status'] == 'completed' and predictions['complete']
    records = predictions['records']
    assert len(records) == len(audit['issues']) == len(audit['cycles']) == 163
    assert digest(Path(__file__).with_name('oldenburg_full_chain_replay.py')) == policy['runner_sha256']
    for name,expected in policy['input_hashes'].items(): assert digest(root/name) == expected,name
    from storm_tracker_worker.thin_raw_code import source_tree_digest
    assert source_tree_digest() == policy['code'] == progress['code']
    verified = 0
    for name,ref in json.loads((root/'files-sha256.json').read_text()).items():
        if name != 'progress.json':
            assert digest(root/name) == ref['sha256'],name
            verified += 1
    original = {datetime.fromisoformat(i['issued_at']).timestamp():i for i in audit['issues']}
    cycles = {c['cycle_id']:c for c in audit['cycles']}
    passages = defaultdict(list)
    for p in audit['passages']:
        passages[p['forecast_issue_id']].append({**p,
            'entry_at':datetime.fromisoformat(p['entry_at']).timestamp(),
            'exit_at':None if p['exit_at'] is None else datetime.fromisoformat(p['exit_at']).timestamp()})
    inputs = json.loads((root/'full-chain107-inputs/manifest.json').read_text())
    receipts = {c['capture_id']:c for c in inputs['captures']}
    checked = set(); chronology = []; supports = Counter(); providers = Counter()
    decoder_errors = records[-1]['decoder_failures']
    for r in records:
        at = r['issued_at']; old = original[at]; c = cycles[r['original_cycle_id']]
        assert datetime.fromisoformat(c['evaluated_at']).timestamp() == at
        for ids in c['policy']['provider_inputs'].values():
            for cid in ids:
                receipt = receipts[cid]
                assert receipt['received_at'] <= at and receipt['requested_at'] <= at
                if receipt['sha256'] not in checked:
                    ref = inputs['payloads'][receipt['sha256']]
                    assert digest(root/ref['file']) == receipt['sha256']
                    checked.add(receipt['sha256'])
        f = r['forecast']; s = f['radar_field_forecast']
        assert (f['target_latitude'],f['target_longitude']) == (53.1435,8.2146)
        if s is not None:
            assert s['issued_at'] <= at
            assert s['threshold_mm_h'] == 0. and s['threshold_operator'] == 'gt'
            for candidate in s['candidates']:
                assert candidate['product_at'] <= s['issued_at']
                assert candidate['available_at'] <= s['issued_at']
                if candidate['previous_product_at'] is not None:
                    assert candidate['previous_product_at'] < candidate['product_at']
                    assert candidate['pair_dt_seconds'] == candidate['product_at']-candidate['previous_product_at']
            supports[s['support_state']] += 1; providers[s['primary_provider']] += 1
        chronology.append({'issue':local(at),'new_entry':local(f['entry_at']),
            'new_exit':local(f['exit_at']),'state':f['forecast_state'],
            'primary_provider':None if s is None else s['primary_provider'],
            'support':None if s is None else s['support_state'],
            'source_conflict':None if s is None else s['source_conflict'],
            'intervals':[] if s is None else [{k:local(p[k]) for k in
                ('entry_at','exit_at','first_supported_at','last_supported_at')} for p in s['intervals']],
            'original_entry':local(old['forecast_metadata']['forecast']['entry_at']),
            'original_exit':local(old['forecast_metadata']['forecast']['exit_at']),
            'tracked_cells':r['tracked_cells'],'historical_complete':r['historical_complete']})
    reference_path = root/'source-clock-corrected/metrics.json'
    reference = json.loads(reference_path.read_text())
    scores = compare(records,reference['source_timed_observations'],original,passages)
    result = {'status':'completed','issue_count':len(records),'code':policy['code'],
        'original_artifacts_verified':verified,'native_payloads_verified':len(checked),
        'reference_sha256':digest(reference_path),'predictions_sha256':digest(dest/'predictions.json'),
        'scorer_sha256':digest(Path(__file__)),'scores':scores,'chronology':chronology,
        'supports':dict(supports),'selected_sources':dict(providers),
        'decoder_errors':decoder_errors,'fusion_errors':progress['fusion_errors'],
        'source_engine_advances':progress['source_advances'],
        'tracking_calls':sum(len(r['tracking_calls']) for r in records),
        'tracking_statuses':dict(Counter(c['status'] for r in records for c in r['tracking_calls'])),
        'observed_weak_events':reference['observed_weak_events'],
        'observed_policy_events':reference['observed_policy_events'],
        'limits':policy['limits'],
        'interpretation':['New complete numerical chain, not cached106 motion or downstream107-only replay.',
            'Later source-timed KNMI target pixels are verification only; original issues are baseline, not truth.',
            'Episode >0, intensity >=.1, and kernel windows are scored separately against both references.',
            'Missing original warning is not dry; partial/censored/held horizons keep unknowns explicit.',
            'One event, dependent issue-observation pairs, unknown exact target ground truth: no accuracy percentage.']}
    (dest/'metrics.json').write_text(json.dumps(result,indent=2,allow_nan=False))
    print(json.dumps({k:result[k] for k in ('status','issue_count','native_payloads_verified',
        'scores','supports','selected_sources','source_engine_advances','tracking_statuses')}))


if __name__ == '__main__': main()
