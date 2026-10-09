"""Disposable offline comparison, never production or full pipeline equivalence."""
import dataclasses,hashlib,json,math,sys,time,zlib
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
import numpy as np
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from storm_tracker_worker.thin_provider_adapters import ThinRadarField,RadarFieldIdentity
from storm_tracker_worker.thin_radar_tracking import ThinRadarTracker
from storm_tracker_worker.thin_footprint_forecast import forecast_target_passages
from storm_tracker_worker.thin_passage_forecast import advect_labelled_qpe,AdvectionMotion,PassageTarget,PassageForecastPolicy

DEST=Path('/home/ikke/storm-tracker/.analysis/oldenburg-replay-20261008')
REGION='region:external:29c255316ea15b8d'
def load_field(f):
    m=f['metadata']; arrays={}
    for name,r in f['arrays'].items():
        b=(DEST/r['file']).read_bytes();assert hashlib.sha256(b).hexdigest()==r['sha256']
        a=np.load(BytesIO(zlib.decompress(b)),allow_pickle=False)
        assert hashlib.sha256(a.tobytes()).hexdigest()==r['descriptor']['sha256']
        arrays[name]=a
    identity=dict(m['identity']);identity['source_composition']=tuple(identity['source_composition'])
    return ThinRadarField(m['provider_id'],m['product_id'],m['observed_at'],m['crs'],*m['affine'],arrays['values'],arrays['valid_mask'],RadarFieldIdentity(**identity))
def compact(p):
    return {k:getattr(p,k) for k in ['entry_at','exit_at','predicted_intensity_mm_h','peak_intensity_mm_h','right_censored','motion_status']}
def union_intervals(intervals):
    result=[]
    for p in sorted(intervals,key=lambda p:p['entry_at']):
        if result and (result[-1]['exit_at'] is None or p['entry_at']<=result[-1]['exit_at']):
            result[-1]['exit_at']=None if result[-1]['exit_at'] is None or p['exit_at'] is None else max(result[-1]['exit_at'],p['exit_at'])
            result[-1]['members']+=1
        else:result.append({'entry_at':p['entry_at'],'exit_at':p['exit_at'],'members':1})
    return result
def main():
    manifest=json.loads((DEST/'export-manifest.json').read_text())
    scope=SimpleNamespace(target_id=REGION,latitude=53.1435,longitude=8.2146,observation_radius_km=250.)
    tracker=ThinRadarTracker();records=[];start=time.monotonic()
    for index,f in enumerate(manifest['frames']):
        field=load_field(f);issued=__import__('datetime').datetime.fromisoformat(f['available_at']).timestamp()
        t=time.monotonic();record={'field_id':f['field_id'],'product_at':field.observed_at,'issued_at':issued,'target_rate_mm_h':f['target_rate_mm_h']}
        try:
            transition=tracker.advance(field,scope)
            tracks=tracker.forecast_tracks(field,REGION)
            record['tracking_status']=getattr(transition,'status','same_time')
            record['tracking_reasons']=list(getattr(transition,'reason_codes',()))
            record['detections']=len(transition.detection_frame.detections) if transition is not None else None
            admitted=[x for x in tracks if x.motion_ready]
            record['tracks']=len(tracks);record['admitted']=len(admitted)
            passages=[]
            for x in admitted:
                d={k:getattr(x,k) for k in x.__dataclass_fields__};d['cell_id']=f'{x.provider_id}:{REGION}:{x.track_id}'
                cell=SimpleNamespace(**d)
                passages.extend(compact(p) for p in forecast_target_passages(cell=cell,target_id='test_tracker',target_latitude=53.1435,target_longitude=8.2146,evaluated_at=issued,accuracy_buffer_km=.01))
            record['baseline_candidates']=passages;record['overlap_union']=union_intervals(passages)
            # Non-overlapping admitted pixel support prevents core/shield double weighting.
            covered=set();weighted=[]
            for x in sorted(admitted,key=lambda x:len(x.footprint_pixels),reverse=True):
                fresh=set(x.footprint_pixels)-covered
                if fresh:weighted.append((len(fresh),x.velocity_east_kmh/3.6,x.velocity_north_kmh/3.6));covered.update(fresh)
            wetmask=field.valid_mask & (field.rain_rate_mm_h>=.1)
            rows,cols=np.where(wetmask);count=len(rows)
            record['motion_support_fraction']=len(covered)/count if count else None
            if weighted:
                w=sum(x[0] for x in weighted);vx=sum(n*x for n,x,y in weighted)/w;vy=sum(n*y for n,x,y in weighted)/w
                record['global_vector_m_s']=[vx,vy]
                record['global_vector_status']='unadmitted_hypothesis_from_admitted_members_not_operational'
                ints=advect_labelled_qpe(field,tuple(zip(rows.tolist(),cols.tolist())),AdvectionMotion(vx,vy,'offline-global-comparator'),PassageTarget('test_tracker',53.1435,8.2146,.01),evaluated_at=issued,policy=PassageForecastPolicy())
                record['global_mask_candidates']=[{'entry_at':issued if p.left_censored else (p.entry_bracket.lower_at+p.entry_bracket.upper_at)/2,'exit_at':None if p.exit_bracket is None else (p.exit_bracket.lower_at+p.exit_bracket.upper_at)/2,'mean_mm_h':p.mean_intensity_mm_h,'peak_mm_h':p.peak_intensity_mm_h,'right_censored':p.right_censored} for p in ints]
        except (ValueError,RuntimeError) as e:record['error']=str(e)
        record['elapsed_seconds']=time.monotonic()-t;records.append(record)
        (DEST/'replay-comparison.json').write_text(json.dumps({'scope':'exact deployed tracker/footprint functions; NOT exact end-to-end live replay (no scheduler, full initial state or nowcast hysteresis restored)','records':records,'total_seconds':time.monotonic()-start},separators=(',',':')))
        print(json.dumps({'frame':index+1,'of':len(manifest['frames']),'status':record.get('tracking_status',record.get('error')),'detections':record.get('detections'),'admitted':record.get('admitted'),'baseline_intervals':len(record.get('baseline_candidates',[])),'seconds':round(record['elapsed_seconds'],2)}),flush=True)
    print('Replay comparison saved',flush=True)
if __name__=='__main__':main()
