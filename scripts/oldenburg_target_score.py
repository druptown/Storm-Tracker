"""Cheap issued-interval assessment; never recomputes motion or changes production."""
from collections import Counter, defaultdict
from datetime import datetime
from hashlib import sha256
from io import BytesIO
import json
import math
from pathlib import Path
import sys
import zlib
from zoneinfo import ZoneInfo
import numpy as np


def field_expectation(summary, at):
    if not summary['issued_at'] <= at <= summary['issued_at'] + 7200:
        return 'unknown'
    censored = False
    for p in summary['intervals']:
        entry = p['entry_at'] if p['entry_at'] is not None else p['first_supported_at']
        if at < entry:
            continue
        exit_at = p['exit_at']
        if exit_at is not None and at <= exit_at:
            return 'wet'
        if exit_at is None:
            if at <= p['last_supported_at']:
                return 'wet'
            censored = True
    if censored:
        return 'unknown'
    complete = summary['known_sample_count'] == summary['total_sample_count']
    return 'dry' if complete and summary['support_state'] in ('complete', 'held') else 'unknown'


def original_expectation(passages, at):
    unknown = False
    for p in passages:
        if at < p['entry_at']:
            continue
        if p['exit_at'] is None:
            unknown = True
        elif at <= p['exit_at']:
            return 'issued_wet_interval'
    return 'unknown_censored_end' if unknown else 'no_issued_interval'


def epoch(at):
    return None if at is None else datetime.fromisoformat(at).timestamp()


def local(at):
    return None if at is None else datetime.fromtimestamp(at, ZoneInfo('Europe/Brussels')).isoformat()


def digest(path):
    return sha256(path.read_bytes()).hexdigest()


def main():
    root = Path('/home/ikke/storm-tracker/.analysis/oldenburg-replay-20261008')
    dest = root/'target-runtime106'
    output = dest/'issued-interval-metrics.json'
    assert not output.exists(), 'Do not overwrite completed assessment'
    policy = json.loads((dest/'policy.json').read_text())
    progress = json.loads((dest/'progress.json').read_text())
    predictions = json.loads((dest/'predictions.json').read_text())
    assert progress['status'] == 'completed' and not progress['errors']
    records = predictions['records']
    assert predictions['complete'] and len(records) == 163
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from storm_tracker_worker.thin_raw_code import source_tree_digest
    assert source_tree_digest() == policy['source_code'] == progress['source_code']
    assert digest(Path(__file__).with_name('oldenburg_target_replay.py')) == policy['runner_sha256']
    for name, expected in policy['input_hashes'].items():
        assert digest(root/name) == expected, name
    hashes = json.loads((root/'files-sha256.json').read_text())
    verified_original = 0
    for name, expected in hashes.items():
        if name != 'progress.json':
            assert digest(root/name) == expected['sha256'], name
            verified_original += 1
    audit = json.loads((root/'event-audit.json').read_text())
    original = {i['forecast_issue_id']: i for i in audit['issues']}
    reference_path = root/'source-clock-corrected/metrics.json'
    reference = json.loads(reference_path.read_text())
    observations = reference['source_timed_observations']
    native = json.loads((root/'source-clock-corrected/native-manifest.json').read_text())
    by_fingerprint = {f['metadata']['identity']['content_fingerprint']: f for f in native['fields']}
    checked_payloads = set()
    for f in native['fields']:
        if f['metadata']['provider_id'] != 'knmi_radar':
            continue
        for ref in f['arrays'].values():
            if ref['file'] in checked_payloads:
                continue
            raw = (root/'source-clock-corrected'/ref['file']).read_bytes()
            assert sha256(raw).hexdigest() == ref['sha256']
            array = np.load(BytesIO(zlib.decompress(raw)), allow_pickle=False)
            assert sha256(array.tobytes()).hexdigest() == ref['descriptor']['sha256']
            checked_payloads.add(ref['file'])
    passages = defaultdict(list)
    for p in audit['passages']:
        passages[p['forecast_issue_id']].append({**p, 'entry_at':epoch(p['entry_at']), 'exit_at':epoch(p['exit_at'])})
    scores = {str(th):{f'{lo}-{hi}min':{'new':Counter(), 'original':Counter(), 'held_new':Counter()}
              for lo, hi in ((5,30),(30,60),(60,120))} for th in (0.,.1)}
    chronology = []
    causal_pairs = 0
    for r in records:
        old = original[r['original_issue_id']]
        issued = r['issued_at']
        assert epoch(old['issued_at']) == issued
        forecast = r['forecast']; s = forecast['radar_field_forecast']
        assert (forecast['target_latitude'],forecast['target_longitude']) == (53.1435,8.2146)
        assert s['region_id'] == old['region_revision_id'] and s['issued_at'] <= issued
        for c in s['candidates']:
            assert c['product_at'] <= s['issued_at'] and c['available_at'] <= s['issued_at']
            new = by_fingerprint[c['field_fingerprint']]
            assert new['available_at'] <= s['issued_at']
            assert new['metadata']['observed_at'] == c['product_at']
            if c['previous_fingerprint'] is not None:
                prev = by_fingerprint[c['previous_fingerprint']]
                assert prev['available_at'] <= s['issued_at']
                assert prev['metadata']['observed_at'] == c['previous_product_at']
                assert 0 < c['pair_dt_seconds'] <= 1200
                assert c['pair_dt_seconds'] == c['product_at']-c['previous_product_at']
                causal_pairs += 1
        assert all(x['at'] >= issued and (x['rate_mm_h'] is None or math.isfinite(x['rate_mm_h'])) for x in r['samples'])
        chronology.append(dict(issue=local(issued), product=local(r['product_at']),
            state=forecast['forecast_state'], support=s['support_state'],
            summary_issue=local(s['issued_at']), age_seconds=s.get('age_seconds'),
            new_entry=local(forecast['entry_at']), new_exit=local(forecast['exit_at']),
            intervals=[dict(entry=local(p['entry_at']), first_supported=local(p['first_supported_at']),
                exit=local(p['exit_at']),last_supported=local(p['last_supported_at']),end_status=p['end_status']) for p in s['intervals']],
            original_entry=local(old['forecast_metadata']['forecast']['entry_at']),
            original_exit=local(old['forecast_metadata']['forecast']['exit_at'])))
        for obs in observations:
            at = obs['metadata']['observed_at']
            if not obs['target_valid']:
                continue
            lead = (at-issued)/60
            band = next((f'{lo}-{hi}min' for lo,hi in ((5,30),(30,60),(60,120)) if lo<=lead<hi),None)
            if band is None:
                continue
            ns = field_expectation(s, at)
            os = original_expectation(passages[r['original_issue_id']],at)
            for threshold in (0.,.1):
                wet = obs['target_rate_mm_h'] > 0 if threshold == 0 else obs['target_rate_mm_h'] >= threshold
                suffix = '_on_wet' if wet else '_on_dry'
                counts = scores[str(threshold)][band]
                counts['new'][ns+suffix] += 1
                counts['original'][os+suffix] += 1
                if s['support_state'] == 'held':
                    counts['held_new'][ns+suffix] += 1
    result = dict(status='completed', qualification='single KNMI event, issued-interval assessment',
        original_artifacts_verified=verified_original, native_payloads_verified=len(checked_payloads),
        causal_motion_pairs_verified=causal_pairs, issue_count=len(records),
        input_hashes=policy['input_hashes'], reference_sha256=digest(reference_path),
        predictions_sha256=digest(dest/'predictions.json'), scorer_sha256=digest(Path(__file__)),
        scores=scores, chronology=chronology,
        observed_weak_events=reference['observed_weak_events'], observed_policy_events=reference['observed_policy_events'],
        interpretation=['New intervals always use >=0.1 mm/h; >0 reference is sensitivity, not a different issued forecast.',
          'Wet inside issued intervals; censored end wet only through last support. Partial/unknown outside intervals remain unknown.',
          'Complete or held-complete dry only inside original two-hour summary horizon; held retains original absolute timestamps.',
          'Original no-issued-interval is absence of warning, NOT an observed dry prediction or target truth.',
          'All interval windows in issued radar_field_forecast scored; primary entry/exit separately preserved in chronology.',
          'Dependent pairs, 1.5 km projection, unknown exact target ground truth: no calibrated accuracy percentage.'])
    output.write_text(json.dumps(result,indent=2,allow_nan=False))
    print(json.dumps({k:result[k] for k in ('status','original_artifacts_verified','native_payloads_verified','causal_motion_pairs_verified','issue_count','scores')}))


if __name__ == '__main__':
    main()
