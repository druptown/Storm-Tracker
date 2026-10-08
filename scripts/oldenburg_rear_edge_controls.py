"""Reproduce cheap initial-motion counterfactuals; no future predictor input."""
import json
from pathlib import Path
import sys
import numpy as np


def main():
    sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
    from storm_tracker_worker.thin_source_radar_forecast import rain_intervals
    root=Path('/home/ikke/storm-tracker/.analysis/oldenburg-replay-20261008')
    dest=root/'rear-edge-diagnosis106';output=dest/'initial-motion-controls.json'
    assert not output.exists(), 'Preserve completed control'
    refined=json.loads((dest/'subpixel-sensitivity.json').read_text())
    prediction=json.loads((root/'target-runtime106/predictions.json').read_text())['records'][60]
    arrays=np.load(dest/'projected-fields.npz')
    base=next(i for i,x in enumerate(arrays['times']) if x==prediction['product_at'])
    rates=arrays['values'][base];valid=arrays['valid'][base]
    native=json.loads((root/'source-clock-corrected/native-manifest.json').read_text())
    available={f['metadata']['observed_at']:f['available_at'] for f in native['fields']
               if f['metadata']['provider_id']=='knmi_radar'}
    results={}
    for name,value in refined['results'].items():
        motion=value['pairs'][0]
        assert motion['new_at']==prediction['product_at']
        assert available[motion['old_at']]<=prediction['issued_at']
        assert available[motion['new_at']]<=prediction['issued_at']
        samples=[]
        for index in range(121):
            at=prediction['issued_at']+index*60;age=at-prediction['product_at']
            x,y=-motion['vx']*age,-motion['vy']*age
            row=int(np.floor((255000-y)/1500));col=int(np.floor((x+255000)/1500))
            supported=0<=row<340 and 0<=col<340 and valid[row,col]
            samples.append(dict(at=at,rate_mm_h=float(rates[row,col]) if supported else None))
        results[name]=dict(past_pair_motion=motion,policy_intervals=rain_intervals(samples,.1),
            weak_intervals=rain_intervals(samples,np.nextafter(0.,1.)),samples=samples)
    output.write_text(json.dumps(dict(method='frozen initial-pair bulk-motion controls on full initial QPE; only 01:35/01:40 images, no later motion; same121 samples and2hour horizon; diagnostic only, not production',
        controls=results,original_weak_intervals=rain_intervals(prediction['samples'],np.nextafter(0.,1.))),indent=2))


if __name__=='__main__':
    main()
