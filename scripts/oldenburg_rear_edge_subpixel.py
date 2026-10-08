"""Cheap subpixel sensitivity of existing registrations, never a forecast."""
import json
from pathlib import Path
from hashlib import sha256
import math
import numpy as np


def peak_offset(left, center, right):
    if not all(math.isfinite(x) for x in (left,center,right)):
        return 0.
    curvature = left-2*center+right
    if curvature >= -1e-12:
        return 0.
    return float(np.clip(.5*(left-right)/curvature,-.5,.5))


def main():
    dest=Path('/home/ikke/storm-tracker/.analysis/oldenburg-replay-20261008/rear-edge-diagnosis106')
    output=dest/'subpixel-sensitivity.json'
    assert not output.exists(), 'Do not overwrite sensitivity'
    source=json.loads((dest/'results.json').read_text())
    initial=json.loads((dest/'initial-pair-registration.json').read_text())
    arrays=np.load(dest/'projected-fields.npz')
    values,valid,times=arrays['values'],arrays['valid'],arrays['times']
    indices={at:i for i,at in enumerate(times)}
    results={}
    for name,box in source['policy']['rois'].items():
        r0,r1,c0,c1=box;cumulative=np.zeros(2);pairs=[];traces=[]
        baseline=[dict(old_at=initial[name]['old_at'],new_at=initial[name]['new_at'],
                       match=initial[name]['fit'],accepted=True,dt=initial[name]['dt'])]+source['registration'][name]['pairs']
        for pair in baseline:
            old,new=indices[pair['old_at']],indices[pair['new_at']]
            a=np.log1p(np.maximum(values[old,r0:r1,c0:c1],0)/.1)
            ac=a-a.mean();ae=float(np.sum(ac*ac))
            def ncc(dr,dc):
                bv=valid[new,r0+dr:r1+dr,c0+dc:c1+dc]
                if not np.all(bv):return float('nan')
                b=np.log1p(np.maximum(values[new,r0+dr:r1+dr,c0+dc:c1+dc],0)/.1)
                bc=b-b.mean();be=float(np.sum(bc*bc))
                return float(np.sum(ac*bc)/np.sqrt(ae*be)) if ae*be>1e-20 else float('nan')
            dr,dc=pair['match']['dr'],pair['match']['dc'];center=ncc(dr,dc)
            assert abs(center-pair['match']['ncc'])<1e-8
            rr=dr+peak_offset(ncc(dr-1,dc),center,ncc(dr+1,dc))
            cc=dc+peak_offset(ncc(dr,dc-1),center,ncc(dr,dc+1))
            dt=pair['dt']
            result=dict(old_at=pair['old_at'],new_at=pair['new_at'],dr=rr,dc=cc,
                        ncc=center,vx=cc*1500/dt,vy=-rr*1500/dt,
                        speed_km_h=float(np.hypot(rr,cc)*1500/dt*3.6))
            pairs.append(result)
            if pair['new_at']<=source['policy']['base_product_at']:
                continue
            cumulative+=[rr,cc]
            # Original nearest-cell QPE sampling, refined translation only.
            row,col=np.floor(np.array([170.5,170.5])-cumulative).astype(int)
            base=indices[source['policy']['base_product_at']]
            supported=0<=row<340 and 0<=col<340 and bool(valid[base,row,col])
            traces.append(dict(at=pair['new_at'],cumulative_dr_dc=cumulative.tolist(),
                initial_rate=float(values[base,row,col]) if supported else None,
                actual_rate=float(values[new,170,170]),support=supported))
        results[name]=dict(pairs=pairs,traces=traces)
    result=dict(status='completed',method='one separable quadratic subpixel refinement of existing NCC peaks; no parameter search',
        results=results,results_sha256=sha256((dest/'results.json').read_bytes()).hexdigest(),
        cache_sha256=sha256((dest/'projected-fields.npz').read_bytes()).hexdigest(),
        script_sha256=sha256(Path(__file__).read_bytes()).hexdigest(),
        limitation='Retrospective movement diagnosis, not causal prediction. Bulk translation and separable refinement still approximate deformation.')
    output.write_text(json.dumps(result,indent=2,allow_nan=False))
    for name,result in results.items():
        print(name,'initial motion',result['pairs'][0])
        print('late target diagnosis',[(p['at'],p['initial_rate'],p['actual_rate']) for p in result['traces'] if p['at']>=1791423600.])


if __name__=='__main__':
    main()
