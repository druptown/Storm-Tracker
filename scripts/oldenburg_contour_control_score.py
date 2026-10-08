"""Score already saved causal broad-zone controls; no new flow or future input."""
from collections import Counter
from hashlib import sha256
import json
from pathlib import Path
from oldenburg_target_replay import classify


def main():
    root = Path('/home/ikke/storm-tracker/.analysis/oldenburg-replay-20261008')
    output = root/'episode-semantics107/contour-control-metrics.json'
    assert not output.exists()
    paths = [root/'rear-edge-diagnosis106/initial-motion-controls.json',
             root/'target-runtime106/predictions.json',root/'source-clock-corrected/metrics.json']
    hashes = {str(p.relative_to(root)):sha256(p.read_bytes()).hexdigest() for p in paths}
    controls = json.loads(paths[0].read_text())['controls']
    original = json.loads(paths[1].read_text())['records'][60]
    observed = json.loads(paths[2].read_text())['source_timed_observations']
    series = {'local106':original['samples'],**{k:v['samples'] for k,v in controls.items()}}
    scores = {}
    for name,samples in series.items():
        scores[name] = {}
        for threshold in (0.,.1):
            counts = Counter()
            for obs in observed:
                at = obs['metadata']['observed_at']
                if not obs['target_valid'] or not 5 <= (at-original['issued_at'])/60 < 120:
                    continue
                sample = min(samples,key=lambda s:abs(s['at']-at))
                assert abs(sample['at']-at) <= 30.000001
                counts[classify(sample['rate_mm_h'],obs['target_rate_mm_h'],threshold)] += 1
            scores[name][str(threshold)] = dict(counts)
    for path,expected in hashes.items():
        assert sha256((root/path).read_bytes()).hexdigest() == expected
    result = dict(issue_at=original['issued_at'],scores=scores,input_hashes=hashes,
        limits=['One selected issue and two-hour window, not event-wide qualification',
                'Past01:35/01:40 broad-zone translation controls only, no future motion input',
                'Frozen coherent translation is a diagnostic alternative, not production',
                'Verification uses later radar, exact local ground truth unknown'])
    output.write_text(json.dumps(result,indent=2,allow_nan=False))
    print(json.dumps(scores))


if __name__ == '__main__':
    main()
