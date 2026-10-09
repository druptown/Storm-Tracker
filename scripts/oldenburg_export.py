"""Export only recorded Oldenburg used fields and receipts, read-only DB."""
import hashlib,json,sys
from pathlib import Path
from datetime import datetime
from collections import Counter
import numpy as np
from pyproj import Transformer
sys.path.insert(0,str(Path(__file__).resolve().parent))
from check_live_raw_archive_schema import existing_admin_connection
from storm_tracker_worker.thin_postgis_raw_archive import read_archived_field
from storm_tracker_worker.thin_raw_field_codec import decode_radar_field

DEST=Path('/home/ikke/storm-tracker/.analysis/oldenburg-replay-20261008')
def epoch(v): return datetime.fromisoformat(v).timestamp()
def main():
    audit=json.loads((DEST/'event-audit.json').read_text())
    con=existing_admin_connection()
    try:
        con.execute('SET TRANSACTION READ ONLY')
        con.execute("SET LOCAL statement_timeout='20s'")
        (DEST/'fields').mkdir(exist_ok=True);(DEST/'payloads').mkdir(exist_ok=True)
        links=sorted(audit['links'],key=lambda l:next(c['evaluated_at'] for c in audit['cycles'] if c['cycle_id']==l['cycle_id']))
        cycles={c['cycle_id']:c for c in audit['cycles']};seen=set();frames=[];errors=[];receipts={};total_bytes=0
        for l in links:
            fid=l['field_id']
            if fid in seen:continue
            seen.add(fid); c=cycles[l['cycle_id']]
            try:r=read_archived_field(con.cursor(),fid,cycle_id=l['cycle_id'],available_at=epoch(c['evaluated_at']))
            except ValueError as e:
                errors.append({'field_id':fid,'cycle_id':l['cycle_id'],'error':str(e)});continue
            decoded=decode_radar_field(r);m=decoded['metadata'];a=decoded['arrays'];refs={}
            for name,b in r.arrays.items():
                digest=hashlib.sha256(b).hexdigest();p=DEST/'fields'/digest
                if not p.exists():p.write_bytes(b)
                refs[name]={'file':str(p.relative_to(DEST)),'sha256':digest,'descriptor':dict(r.array_descriptors[name])}
            frame={'field_id':fid,'cycle_id':l['cycle_id'],'available_at':c['evaluated_at'],'metadata':m,'arrays':refs}
            if m['kind']=='quantitative':
                x,y=Transformer.from_crs('EPSG:4326',m['crs'],always_xy=True).transform(8.2146,53.1435)
                ox,oy,dx,dy=m['affine'];col=int(np.floor((x-ox)/dx));row=int(np.floor((oy-y)/dy))
                values=a['values'];valid=a['valid_mask'];inside=0<=row<values.shape[0] and 0<=col<values.shape[1]
                frame.update(target_row=row,target_column=col,target_valid=bool(inside and valid[row,col]),
                             target_rate_mm_h=float(values[row,col]) if inside and valid[row,col] and np.isfinite(values[row,col]) else None)
                if inside and 'contributor_mask' in a:frame['target_contributor_mask']=int(a['contributor_mask'][row,col])
            frames.append(frame)
            rows=con.execute('''SELECT c.capture_id,c.provider_id,c.received_at,c.disposition,c.reason_code,
              p.sha256,p.byte_count,p.payload FROM public.stv3_raw_cycle_field_source_v1 l
              JOIN public.stv3_raw_capture_v1 c USING(capture_id)
              LEFT JOIN public.stv3_raw_payload_v1 p ON p.sha256=c.payload_sha256
              WHERE l.cycle_id=%s AND l.field_id=%s''',(l['cycle_id'],fid)).fetchall()
            for cid,provider,received,disposition,reason,digest,size,body in rows:
                if cid in receipts:continue
                if body is not None:
                    b=bytes(body);assert len(b)==size and hashlib.sha256(b).hexdigest()==digest
                    p=DEST/'payloads'/digest
                    if not p.exists():
                        total_bytes+=len(b)
                        if total_bytes>512*1024*1024:raise RuntimeError('bounded_native_export_limit')
                        p.write_bytes(b)
                receipts[cid]={'capture_id':cid,'provider':provider,'received_at':str(received),'sha256':digest,'bytes':size,'disposition':disposition,'reason':reason}
        frames.sort(key=lambda f:f['metadata']['observed_at'])
        result={'frames':frames,'errors':errors,'receipts':list(receipts.values()),'native_bytes_written':total_bytes,
                'gap_counts':dict(Counter(g for c in audit['cycles'] for g in c['gaps']))}
        (DEST/'export-manifest.json').write_text(json.dumps(result,separators=(',',':')))
        wet=[f for f in frames if f.get('target_rate_mm_h',0) is not None and f.get('target_rate_mm_h',0)>0]
        print(json.dumps({'frames':len(frames),'errors':len(errors),'receipts':len(receipts),'native_bytes':total_bytes,'gaps':result['gap_counts'],
           'first_wet':[{k:f[k] for k in ('available_at','target_rate_mm_h','metadata')} for f in wet[:2]],
           'last_wet':[{k:f[k] for k in ('available_at','target_rate_mm_h','metadata')} for f in wet[-2:]]},separators=(',',':')))
    finally:con.rollback();con.close()
if __name__=='__main__':main()
