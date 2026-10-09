"""Disposable field-motion experiment; not production code or calibrated skill."""
import math
from datetime import datetime
import numpy as np

def local_motion(points, velocities, x, y, radius_m=60000., minimum_vectors=3, maximum_spread_m_s=8.):
    if len(points) < minimum_vectors:
        return None
    distances=np.sum((points-np.array([x,y]))**2,axis=1)
    indices=np.argsort(distances)[:8]
    indices=indices[distances[indices]<=radius_m**2]
    if len(indices)<minimum_vectors:
        return None
    vectors=velocities[indices]
    if not np.all(np.isfinite(vectors)):
        return None
    weights=1./(distances[indices]+1500.**2)
    weights/=np.sum(weights)
    vector=np.sum(vectors*weights[:,None],axis=0)
    spread=float(np.sqrt(np.sum(weights*np.sum((vectors-vector)**2,axis=1))))
    if spread>maximum_spread_m_s:
        return None
    return {'vector':vector.tolist(),'maximum_distance_m':float(np.sqrt(np.max(distances[indices]))),'spread_m_s':spread,'support_vectors':len(indices)}

def reverse_target_series(values, valid, grid, motion, target_x, target_y, product_at, issued_at, horizon_seconds=7200., step_seconds=60.):
    if issued_at<product_at:
        raise ValueError('future_product')
    if horizon_seconds<0 or step_seconds<=0:
        raise ValueError('invalid_time_support')
    x,y=float(target_x),float(target_y)
    unavailable=None
    max_distance=max_spread=0.
    def advance(duration):
        nonlocal x,y,unavailable,max_distance,max_spread
        remaining=duration
        while remaining>1e-9 and unavailable is None:
            first=motion(x,y)
            if first is None:
                unavailable='motion_support_missing';break
            vx,vy=first['vector']
            speed=math.hypot(vx,vy)
            dt=min(remaining,30.,.25*min(grid.x_resolution_m,grid.y_resolution_m)/max(speed,1e-9))
            middle=motion(x-vx*dt/2,y-vy*dt/2)
            if middle is None:
                unavailable='motion_support_missing';break
            x-=middle['vector'][0]*dt;y-=middle['vector'][1]*dt
            max_distance=max(max_distance,float(middle['maximum_distance_m']))
            max_spread=max(max_spread,float(middle['spread_m_s']))
            remaining-=dt
    advance(issued_at-product_at)
    samples=[]
    offsets=list(np.arange(0.,horizon_seconds+1e-9,step_seconds))
    if not offsets or offsets[-1]<horizon_seconds:
        offsets.append(float(horizon_seconds))
    previous=0.
    for offset in offsets:
        advance(float(offset)-previous);previous=float(offset)
        reason=unavailable
        row=math.floor((grid.origin_y_m-y)/grid.y_resolution_m)
        column=math.floor((x-grid.origin_x_m)/grid.x_resolution_m)
        rate=None
        if reason is None:
            if not (0<=row<values.shape[0] and 0<=column<values.shape[1]):
                reason='source_outside_radar'
            elif not valid[row,column] or not np.isfinite(values[row,column]):
                reason='source_nodata'
            else:
                rate=float(values[row,column])
        samples.append({'at':issued_at+float(offset),'rate_mm_h':rate,'reason':reason,'source_x_m':x,'source_y_m':y,'maximum_support_distance_m':max_distance,'maximum_velocity_spread_m_s':max_spread})
    return samples

def passage_intervals(samples, threshold=.1):
    result=[];active=None;previous=None
    def finish(status,exit_bracket=None):
        nonlocal active
        active['end_status']=status
        active['exit_bracket']=exit_bracket
        active['exit_at']=None if exit_bracket is None else sum(exit_bracket)/2
        values=active.pop('rates')
        active['mean_mm_h']=float(np.mean(values));active['peak_mm_h']=max(values)
        result.append(active);active=None
    for sample in samples:
        at=sample['at'];rate=sample['rate_mm_h']
        if rate is None:
            if active is not None:
                finish('support_censored')
            previous=None;continue
        wet=rate>=threshold
        if wet and active is None:
            bracket=None if previous is None else [previous,at]
            entry=at if previous is None and sample is samples[0] else None if bracket is None else sum(bracket)/2
            active={'entry_at':entry,'entry_bracket':bracket,'left_censored':sample is samples[0],'last_supported_at':at,'rates':[]}
        if wet:
            active['last_supported_at']=at;active['rates'].append(rate)
        elif active is not None:
            finish('bracketed',[active['last_supported_at'],at])
        previous=at
    if active is not None:
        finish('horizon_censored')
    return result

def available_history(frames, issued_at):
    return sorted([f for f in frames if f['metadata']['observed_at']<=issued_at and datetime.fromisoformat(f['available_at']).timestamp()<=issued_at],key=lambda f:f['metadata']['observed_at'])

def tiled_motion_vectors(old_values,new_values,common,grid,old_id,new_id,old_at,new_at,config):
    import collections,dataclasses,sys
    from pathlib import Path
    sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
    from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid
    from storm_tracker_worker.tracking_core.engine.hierarchical_detection import NormalizedRasterFrame
    from storm_tracker_worker.tracking_core.engine.pyramidal_sparse_flow import estimate_latest_sparse_flow
    tile_config=dataclasses.replace(config,max_features=24)
    height,width=old_values.shape
    kept={};counts=collections.Counter();tiles=0;selected=0;checksums=[]
    for row in range(0,height,96):
        for column in range(0,width,96):
            h=min(128,height-row);w=min(128,width-column)
            if min(h,w)<32:
                continue
            sl=np.s_[row:row+h,column:column+w]
            g=AnalysisGrid(f'{grid.grid_id}:tile:{row}:{column}',grid.crs,
                           grid.origin_x_m+column*grid.x_resolution_m,
                           grid.origin_y_m-row*grid.y_resolution_m,
                           grid.x_resolution_m,grid.y_resolution_m,w,h)
            def frame(values,frame_id,valid_at):
                image=np.where(common[sl],np.log1p(np.maximum(values[sl],0.)/.1),np.nan)
                rows=tuple(tuple(None if not np.isfinite(v) else float(v) for v in r) for r in image)
                return NormalizedRasterFrame(f'{frame_id}:tile:{row}:{column}',valid_at,'frozen-fused-radar','log1p_precipitation_rate','dimensionless',g,rows)
            pair=estimate_latest_sparse_flow((frame(old_values,old_id,old_at),frame(new_values,new_id,new_at)),tile_config)
            tiles+=1;selected+=pair.feature_audit.selected_count;checksums.append(pair.checksum)
            counts.update(dict(pair.status_counts))
            for v in pair.vectors:
                if v.status!='available_converged':continue
                global_row=row+v.start_row;global_col=column+v.start_column
                key=(global_row,global_col)
                item={'start_row':global_row,'start_column':global_col,
                      'current_x_m':grid.origin_x_m+(global_col+.5+v.displacement_column_pixels)*grid.x_resolution_m,
                      'current_y_m':grid.origin_y_m-(global_row+.5+v.displacement_row_pixels)*grid.y_resolution_m,
                      'vx_m_s':v.vx_m_s,'vy_m_s':v.vy_m_s,
                      'residual':v.forward_residual_rms,'quality':v.structure_quality}
                if key not in kept or item['residual']<kept[key]['residual']:
                    kept[key]=item
    return list(kept.values()),{'tiles':tiles,'selected_including_overlap':selected,
                               'status_counts_including_overlap':dict(counts),'unique_converged_vectors':len(kept),
                               'tile_config_fingerprint':tile_config.fingerprint,'pair_checksums':checksums}

def main():
    import argparse,collections,dataclasses,hashlib,json,sys,time,zlib
    from io import BytesIO
    from pathlib import Path
    sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
    from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid
    from storm_tracker_worker.tracking_core.engine.hierarchical_detection import NormalizedRasterFrame
    from storm_tracker_worker.tracking_core.engine.pyramidal_sparse_flow import SparsePyramidalConfig,estimate_latest_sparse_flow
    parser=argparse.ArgumentParser();parser.add_argument('--limit',type=int)
    args=parser.parse_args()
    dest=Path('/home/ikke/storm-tracker/.analysis/oldenburg-replay-20261008')
    manifest=json.loads((dest/'export-manifest.json').read_text())
    previous_replay=json.loads((dest/'replay-comparison.json').read_text())
    baseline={r['field_id']:r for r in previous_replay['records']}
    config=SparsePyramidalConfig(
        maximum_input_pixels=131072,max_features=384,
        minimum_feature_distance_pixels=6.,feature_window_radius_pixels=2,
        minimum_feature_valid_fraction=.8,minimum_feature_eigenvalue=1e-4,
        pyramid_levels=3,minimum_pyramid_valid_fraction=.8,
        tracking_window_radius_pixels=3,minimum_tracking_samples=20,
        maximum_iterations=20,convergence_threshold_pixels=.03,
        minimum_tracking_eigenvalue=1e-4,maximum_condition_number=1e6,
        regularization=0.,residual_variance_floor=1e-6,
        velocity_process_noise_covariance_m2_s2=((1.,0.),(0.,1.)))
    policy={
        'scope':'Disposable complete-rain-field experiment, not production and not a calibrated probability',
        'algorithm':'Existing pure-NumPy sparse pyramidal Lucas-Kanade, spatial local inverse-distance interpolation, RK2 reverse trajectory',
        'motion_config':dataclasses.asdict(config),'config_fingerprint':config.fingerprint,
        'input_transform':'ln(1+rain_rate_mm_h/0.1), applied to both motion frames only; output QPE remains original mm/h',
        'source_mask':'Both valid finite pixels and identical nonzero contributor bitmask at each pixel; unmatched composition excluded from flow estimation',
        'pair_max_gap_seconds':1200.,'local_radius_m':60000.,'local_nearest_vectors':8,
        'minimum_local_vectors':3,'maximum_local_rms_velocity_spread_m_s':8.,
        'spatial_feature_balance':'128px tiles, 96px stride, maximum24 features per tile; identical source masks and unchanged motion numerics; overlapping start points deduplicated',
        'interpolation_weight_floor_m':1500.,'only_converged_features':True,
        'output_step_seconds':60.,'horizon_seconds':7200.,'rain_threshold_mm_h':.1,
        'integration':'RK2 substeps <=30s and <=0.25 native pixels; unknown support never becomes zero motion',
        'target':'Oldenburg53.1435,8.2146, exact target projected x=y=0 in archived AEQD',
        'calibration':'Fixed exploratory choices before reading results; not optimized on arrival, not externally calibrated',
        'intensity_kernels':'Existing earlier replay admitted-object candidates retained separately without redefining total-zone duration',
        'script_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    (dest/'field-probe-policy.json').write_text(json.dumps(policy,indent=2))
    cache={}
    def load(f):
        if f['field_id'] in cache:return cache[f['field_id']]
        arrays={}
        for name,item in f['arrays'].items():
            raw=(dest/item['file']).read_bytes()
            assert hashlib.sha256(raw).hexdigest()==item['sha256']
            array=np.load(BytesIO(zlib.decompress(raw)),allow_pickle=False)
            assert hashlib.sha256(array.tobytes()).hexdigest()==item['descriptor']['sha256']
            arrays[name]=array
        cache[f['field_id']]=arrays
        if len(cache)>3:
            del cache[next(iter(cache))]
        return arrays
    records=[];started=time.monotonic()
    frames=manifest['frames'][:args.limit] if args.limit else manifest['frames']
    for index,current in enumerate(frames):
        stage=time.monotonic();m=current['metadata']
        issued=datetime.fromisoformat(current['available_at']).timestamp()
        history=available_history(manifest['frames'],issued)
        assert history[-1]['field_id']==current['field_id']
        record={'field_id':current['field_id'],'product_at':m['observed_at'],'issued_at':issued,
                'target_rate_mm_h':current['target_rate_mm_h'],
                'intensity_object_candidates':baseline[current['field_id']]['baseline_candidates'],
                'prediction_status':'unknown_cold_start','samples':[],'passages':[]}
        try:
            if len(history)>=2:
                old=history[-2];om=old['metadata'];dt=m['observed_at']-om['observed_at']
                assert om['crs']==m['crs'] and om['affine']==m['affine']
                newa=load(current);olda=load(old)
                height,width=newa['values'].shape
                g=AnalysisGrid(m['identity']['grid_id'],m['crs'],*m['affine'],width,height)
                if dt>policy['pair_max_gap_seconds']:
                    record['prediction_status']='unknown_product_gap'
                else:
                    common=(olda['valid_mask'] & newa['valid_mask'] &
                            (olda['contributor_mask']==newa['contributor_mask']) &
                            (newa['contributor_mask']!=0) &
                            np.isfinite(olda['values']) & np.isfinite(newa['values']))
                    record['flow_input_common_fraction']=float(np.mean(common))
                    audit_vectors,feature_audit=tiled_motion_vectors(olda['values'],newa['values'],common,g,
                        old['field_id'],current['field_id'],om['observed_at'],m['observed_at'],config)
                    record['motion_pair_frames']=[old['field_id'],current['field_id']]
                    record['motion_pair_dt_seconds']=dt
                    record['feature_status_counts']=feature_audit['status_counts_including_overlap']
                    record['feature_audit']=feature_audit
                    points=[];speeds=[]
                    for v in audit_vectors:
                        points.append([v['current_x_m'],v['current_y_m']]);speeds.append([v['vx_m_s'],v['vy_m_s']])
                    points=np.asarray(points,dtype=float).reshape(-1,2)
                    speeds=np.asarray(speeds,dtype=float).reshape(-1,2)
                    record['converged_vectors']=audit_vectors
                    def spatial_motion(x,y):return local_motion(points,speeds,x,y)
                    samples=reverse_target_series(newa['values'],newa['valid_mask'],g,spatial_motion,0.,0.,m['observed_at'],issued)
                    record['samples']=samples;record['passages']=passage_intervals(samples)
                    record['known_sample_count']=sum(s['rate_mm_h'] is not None for s in samples)
                    record['prediction_status']='candidate_with_support' if record['known_sample_count'] else 'unknown_motion_support'
        except (ValueError,RuntimeError) as error:
            record['error']=str(error);record['prediction_status']='failed_experiment'
        record['elapsed_seconds']=time.monotonic()-stage;records.append(record)
        output={'scope':policy['scope'],'policy':policy,'records':records,
                'processed_frames':len(records),'planned_frames':len(manifest['frames']),
                'complete':len(records)==len(manifest['frames']),'total_seconds':time.monotonic()-started}
        filename='field-motion-probe-smoke.json' if args.limit else 'field-motion-probe.json'
        (dest/filename).write_text(json.dumps(output,separators=(',',':')))
        print(json.dumps({'frame':index+1,'of':len(frames),'status':record['prediction_status'],
                         'vectors':len(record.get('converged_vectors',[])),
                         'known_samples':record.get('known_sample_count',0),
                         'passages':len(record['passages']),'seconds':round(record['elapsed_seconds'],2)}),flush=True)

if __name__=='__main__':
    main()
