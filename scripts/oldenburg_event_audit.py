"""Read-only bounded event audit. No credentials or source bodies printed."""
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from check_live_raw_archive_schema import existing_admin_connection

START = '2026-10-07T22:05:08Z'

def main():
    con = existing_admin_connection()
    try:
        con.execute('SET TRANSACTION READ ONLY')
        con.execute("SET LOCAL statement_timeout='20s'")
        out = {'start': START}
        def rows(sql, params=()):
            cur = con.execute(sql, params)
            return [dict(zip([d.name for d in cur.description], r)) for r in cur.fetchall()]
        out['positions'] = rows('''SELECT count(*) AS count,min(observed_at) AS first,
            max(observed_at) AS last,round(ST_Y(position)::numeric,4) AS lat,
            round(ST_X(position)::numeric,4) AS lon FROM public.stv3_target_position
            WHERE target_id='test_tracker' AND observed_at >= %s
            GROUP BY lat,lon ORDER BY first''', (START,))
        out['issues'] = rows('''SELECT issued_at,forecast_issue_id,region_revision_id,
            forecast_metadata FROM public.stv3_target_forecast_issue
            WHERE target_id='test_tracker' AND issued_at >= %s ORDER BY issued_at''',(START,))
        out['passages'] = rows('''SELECT p.forecast_issue_id,p.status,p.entry_at,p.exit_at,
            p.entry_bracket_lower_at,p.entry_bracket_upper_at,p.right_censored,
            p.predicted_mean_intensity_mm_h,p.predicted_peak_intensity_mm_h
            FROM public.stv3_target_passage_issue p JOIN public.stv3_target_forecast_issue i
            USING(forecast_issue_id) WHERE i.target_id='test_tracker' AND i.issued_at >= %s
            ORDER BY i.issued_at,p.entry_at''',(START,))
        out['cycles'] = rows('''SELECT cycle_id,evaluated_at,complete,gaps,policy
            FROM public.stv3_raw_cycle_v1 WHERE evaluated_at >= %s
            AND policy::text LIKE '%%test_tracker%%' ORDER BY evaluated_at''',(START,))
        out['fields'] = rows('''SELECT DISTINCT f.field_id,f.observed_at,f.metadata,l.region_revision_id,l.role
            FROM public.stv3_raw_cycle_field_v1 l JOIN public.stv3_raw_field_v1 f USING(field_id)
            JOIN public.stv3_raw_cycle_v1 c USING(cycle_id) WHERE c.evaluated_at >= %s
            AND l.region_revision_id IN (SELECT DISTINCT region_revision_id
              FROM public.stv3_target_forecast_issue WHERE target_id='test_tracker' AND issued_at >= %s)
            ORDER BY f.observed_at''',(START,START))
        out['links'] = rows('''SELECT l.* FROM public.stv3_raw_issue_field_v1 l
            JOIN public.stv3_target_forecast_issue i USING(forecast_issue_id)
            WHERE i.target_id='test_tracker' AND i.issued_at >= %s''',(START,))
        dest = Path('/home/ikke/storm-tracker/.analysis/oldenburg-replay-20261008')
        dest.mkdir(exist_ok=True)
        (dest/'event-audit.json').write_text(json.dumps(out,default=str,separators=(',',':')))
        print(json.dumps({'counts':{k:len(v) for k,v in out.items() if isinstance(v,list)},
                          'positions':out['positions'],'path':str(dest/'event-audit.json')},default=str))
    finally:
        con.rollback(); con.close()

if __name__ == '__main__': main()
