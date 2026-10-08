"""Read-only radar-development diagnosis, not a causal forecast benchmark."""
from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
import sys
import time
from zoneinfo import ZoneInfo
import numpy as np


def window_sum(array, height, width):
    integral = np.pad(np.cumsum(np.cumsum(array,axis=0),axis=1),((1,0),(1,0)))
    return integral[height:,width:]-integral[:-height,width:]-integral[height:,:-width]+integral[:-height,:-width]


def match_translation(template, search, valid, margin):
    """Integer normalized template registration; later images for diagnosis ONLY."""
    h,w = template.shape
    assert search.shape == valid.shape == (h+2*margin,w+2*margin)
    if not np.all(np.isfinite(template)):
        return None
    centered = template-template.mean()
    energy = float(np.sum(centered**2))
    if energy < 1e-12:
        return None
    b = np.where(valid & np.isfinite(search),search,0.)
    shape = tuple(2**int(np.ceil(np.log2(n+m-1))) for n,m in zip(b.shape,template.shape))
    cross = np.fft.irfftn(np.fft.rfftn(b,shape,axes=(0,1))*
                         np.fft.rfftn(centered[::-1,::-1],shape,axes=(0,1)),shape,axes=(0,1))
    cross = cross[h-1:h+2*margin,w-1:w+2*margin]
    sums = window_sum(b,h,w); squares = window_sum(b*b,h,w)
    variance = np.maximum(squares-sums*sums/(h*w),0.)
    denominator = np.sqrt(energy*variance)
    usable = (window_sum(valid.astype(float),h,w) == h*w) & (denominator>1e-10)
    if not np.any(usable):
        return None
    ncc = np.full(usable.shape,-np.inf)
    ncc[usable] = cross[usable]/denominator[usable]
    row,col = np.unravel_index(int(np.argmax(ncc)),ncc.shape)
    return dict(dr=int(row-margin),dc=int(col-margin),ncc=float(ncc[row,col]),
                boundary=bool(row in (0,2*margin) or col in (0,2*margin)))


def changes(old, new, valid, threshold):
    valid = valid & np.isfinite(old) & np.isfinite(new)
    a = valid & (old>0 if threshold == 0 else old>=threshold)
    b = valid & (new>0 if threshold == 0 else new>=threshold)
    return dict(valid_pixels=int(valid.sum()),old_wet=int(a.sum()),new_wet=int(b.sum()),
                newly_wet=int((~a&b).sum()),lost_wet=int((a&~b).sum()),retained_wet=int((a&b).sum()))


def local(at):
    return datetime.fromtimestamp(at,ZoneInfo('Europe/Brussels')).isoformat()


def main():
    sys.path.insert(0,str(Path(__file__).parent))
    from oldenburg_target_replay import restore
    from storm_tracker_worker.thin_regional_radar_fusion import project_radar_field
    from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid
    root = Path('/home/ikke/storm-tracker/.analysis/oldenburg-replay-20261008')
    source = root/'source-clock-corrected'; oldrun = root/'target-runtime106'
    dest = root/'rear-edge-diagnosis106'
    dest.mkdir(exist_ok=False)
    def save(name,data):
        (dest/name).write_text(json.dumps(data,indent=2,allow_nan=False))
    started = time.monotonic()
    original_policy = json.loads((oldrun/'policy.json').read_text())
    for name,expected in original_policy['input_hashes'].items():
        assert sha256((root/name).read_bytes()).hexdigest() == expected
    original_pred = json.loads((oldrun/'predictions.json').read_text())
    issue = original_pred['records'][60]
    assert original_pred['complete'] and issue['product_at'] == 1791416400.
    manifest = json.loads((source/'native-manifest.json').read_text())
    reference = json.loads((source/'metrics.json').read_text())
    obs = {o['metadata']['observed_at']:o for o in reference['source_timed_observations']}
    selected = sorted([f for f in manifest['fields'] if f['metadata']['provider_id']=='knmi_radar'
                 and 1791416100. <= f['metadata']['observed_at'] <= 1791430800.],
                 key=lambda f:f['metadata']['observed_at'])
    geometry = json.loads((root/'export-manifest.json').read_text())['frames'][0]['metadata']
    grid = AnalysisGrid(geometry['identity']['grid_id'],geometry['crs'],*geometry['affine'],340,340)
    rois = {'upstream':(130,250,70,190),'target_neighborhood':(130,210,130,210)}
    policy = dict(task='distinguish translation and radar evolution behind early predicted end',
        base_product_at=issue['product_at'],base_issue_at=issue['issued_at'],selected_fields=len(selected),
        method='fixed-window log1p QPE normalized cross-correlation; no production optical-flow replay',
        rois=rois,grid_resolution_m=1500.,thresholds=[0.,.1],minimum_ncc=.4,
        maximum_registration_speed_m_s=40.,minimum_search_margin_pixels=4,
        original_prediction_sha256=sha256((oldrun/'predictions.json').read_bytes()).hexdigest(),
        reference_sha256=sha256((source/'metrics.json').read_bytes()).hexdigest(),
        input_hashes=original_policy['input_hashes'],script_sha256=sha256(Path(__file__).read_bytes()).hexdigest(),
        limitations=['Later measured motion is a retrospective diagnostic, NEVER a forecast accuracy improvement.',
          'Bulk integer translation is not a perfect material motion field. Residuals mix growth, deformation and registration error.',
          'Fixed source, quantity and thresholds; invalid pixels excluded; no target ground truth.',
          'Fixed parameters before data run, no fitting or parameter search.'])
    save('policy.json',policy)
    progress=dict(status='running',processed_fields=0,selected_fields=len(selected),errors=[])
    save('progress.json',progress)
    layers=[]; field_records=[]
    for f in selected:
        layer=project_radar_field(restore(f,source),grid)
        at=f['metadata']['observed_at']; measured=obs[at]
        assert bool(layer.valid_mask[170,170])==measured['target_valid']
        if measured['target_valid']:
            assert abs(float(layer.rain_rate_mm_h[170,170])-measured['target_rate_mm_h'])<1e-9
        layers.append(layer)
        field_records.append(dict(field_id=f['field_id'],at=at,time=local(at),
          actual_target_rate=measured['target_rate_mm_h'],
          wet_counts={name:{str(th):changes(layer.rain_rate_mm_h[box[0]:box[1],box[2]:box[3]],
                layer.rain_rate_mm_h[box[0]:box[1],box[2]:box[3]],
                layer.valid_mask[box[0]:box[1],box[2]:box[3]],th)['new_wet'] for th in (0.,.1)}
                for name,box in rois.items()}))
        progress.update(processed_fields=len(layers),elapsed_seconds=time.monotonic()-started)
        save('progress.json',progress)
    base_index=next(i for i,x in enumerate(layers) if x.observed_at==issue['product_at'])
    base=layers[base_index]
    results={}
    for name,box in rois.items():
        r0,r1,c0,c1=box; cumulative=np.zeros(2); complete=True; pairs=[]; traces=[]
        for i in range(base_index+1,len(layers)):
            a,b=layers[i-1],layers[i];dt=b.observed_at-a.observed_at
            margin=max(4,int(np.ceil(40.*dt/1500.)))
            av=a.valid_mask[r0:r1,c0:c1]
            template=np.log1p(np.maximum(a.rain_rate_mm_h[r0:r1,c0:c1],0)/.1)
            search=np.log1p(np.maximum(b.rain_rate_mm_h[r0-margin:r1+margin,c0-margin:c1+margin],0)/.1)
            bv=b.valid_mask[r0-margin:r1+margin,c0-margin:c1+margin]
            fit=match_translation(template,search,bv,margin) if av.all() else None
            accepted=fit is not None and fit['ncc']>=.4 and not fit['boundary']
            pair=dict(old_at=a.observed_at,new_at=b.observed_at,new_time=local(b.observed_at),dt=dt,
              match=fit,accepted=accepted)
            if accepted:
                dr,dc=fit['dr'],fit['dc']
                newer=b.rain_rate_mm_h[r0+dr:r1+dr,c0+dc:c1+dc]
                common=av & b.valid_mask[r0+dr:r1+dr,c0+dc:c1+dc]
                pair.update(vx_m_s=dc*1500/dt,vy_m_s=-dr*1500/dt,
                  speed_km_h=float(np.hypot(dr,dc)*1500/dt*3.6),
                  changes={str(th):changes(a.rain_rate_mm_h[r0:r1,c0:c1],newer,common,th) for th in (0.,.1)})
                cumulative += [dr,dc]
            else:
                complete=False
            # Uses later measured translations: mechanism attribution, NOT a new forecast.
            row,col=(np.array([170,170])-cumulative).astype(int)
            supported=complete and 0<=row<340 and 0<=col<340 and bool(base.valid_mask[row,col])
            traces.append(dict(at=b.observed_at,time=local(b.observed_at),
              cumulative_dr_dc=cumulative.tolist(),support=supported,
              initial_field_rate_under_later_motion=float(base.rain_rate_mm_h[row,col]) if supported else None,
              actual_rate=field_records[i]['actual_target_rate']))
            pairs.append(pair)
        results[name]=dict(pairs=pairs,retrospective_initial_field_trace=traces)
    xy=np.array([[s['source_x_m'],s['source_y_m']] for s in issue['samples']])
    speeds=np.linalg.norm(np.diff(xy,axis=0)/60,axis=1)*3.6
    result=dict(status='completed',fields=field_records,registration=results,
        original_exit=issue['forecast']['exit_at'],original_issue=issue['issued_at'],
        original_trace=issue['samples'],original_speed_km_h=dict(min=float(speeds.min()),
            median=float(np.median(speeds)),max=float(speeds.max())),
        elapsed_seconds=time.monotonic()-started,policy=policy)
    save('results.json',result)
    np.savez_compressed(dest/'projected-fields.npz',values=np.stack([l.rain_rate_mm_h for l in layers]),
                       valid=np.stack([l.valid_mask for l in layers]),times=[l.observed_at for l in layers])
    progress.update(status='completed',elapsed_seconds=time.monotonic()-started)
    save('progress.json',progress)
    print(json.dumps(dict(status='completed',fields=len(layers),seconds=progress['elapsed_seconds'],
        registration={name:dict(accepted=sum(p['accepted'] for p in value['pairs']),
          total=len(value['pairs']),median_speed_km_h=float(np.median([p['speed_km_h'] for p in value['pairs'] if p['accepted']])))
          for name,value in results.items()})))


if __name__=='__main__':
    main()
