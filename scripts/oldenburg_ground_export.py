"""Read-only local station evidence and exact native fields for cross-check."""
import json,sys,hashlib
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parent))
from check_live_raw_archive_schema import existing_admin_connection
DEST=Path('/home/ikke/storm-tracker/.analysis/oldenburg-replay-20261008')
def main():
    con=existing_admin_connection()
    try:
        con.execute('SET TRANSACTION READ ONLY');con.execute("SET LOCAL statement_timeout='20s'")
        cur=con.execute('''SELECT m.provider_id,m.station_id,m.observed_at,m.received_at,
            m.measurement_type,m.value,m.unit,m.quality,m.metadata,
            ST_Distance(s.position::geography,ST_SetSRID(ST_MakePoint(8.2146,53.1435),4326)::geography)/1000 AS distance_km
            FROM public.stv3_station_measurement m JOIN public.stv3_ground_station s USING(provider_id,station_id)
            WHERE m.observed_at >= '2026-10-07T22:05:08Z'
            AND m.measurement_type LIKE '%%rain%%'
            AND ST_DWithin(s.position::geography,ST_SetSRID(ST_MakePoint(8.2146,53.1435),4326)::geography,20000)
            ORDER BY m.observed_at''')
        rows=[dict(zip([x.name for x in cur.description],r)) for r in cur.fetchall()]
        (DEST/'ground-measurements.json').write_text(json.dumps(rows,default=str,separators=(',',':')))
        by={}
        for r in rows:
            key=r['provider_id']+':'+r['station_id']+':'+r['measurement_type']
            by.setdefault(key,{'count':0,'distance_km':round(r['distance_km'],2),'unit':r['unit'],'first_positive':None,'last_positive':None})
            z=by[key];z['count']+=1
            if r['value']>0:
                z['first_positive']=z['first_positive'] or str(r['observed_at']);z['last_positive']=str(r['observed_at'])
        print(json.dumps({'measurement_rows':len(rows),'stations':by},separators=(',',':')))
    finally:con.rollback();con.close()
if __name__=='__main__':main()
