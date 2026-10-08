"""Read-only, bounded live provenance check; no source polling."""
import json
import argparse
from check_source_clock_archive_bindings import connection


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--code', default='f759b0007666841c67f683e0184564a510d7f9b3320e1ba4fc985d1486f25b73')
    args = parser.parse_args()
    con = connection()
    try:
        con.execute('SET TRANSACTION READ ONLY')
        con.execute("SET LOCAL statement_timeout='15s'")
        rows = con.execute('''SELECT DISTINCT ON (target_id) target_id,
            extract(epoch FROM issued_at),region_revision_id,forecast_metadata,
            forecast_issue_id,input_fingerprints
            FROM public.stv3_target_forecast_issue WHERE algorithm_version=%s
            ORDER BY target_id,issued_at DESC''',('source-clock-full-field-v1',)).fetchall()
        output = []
        for target,issue,region,metadata,issue_id,inputs in rows:
            forecast = metadata['forecast']; field = forecast['radar_field_forecast']
            bound = con.execute('''SELECT DISTINCT field.metadata->'identity'->>'content_fingerprint'
                FROM public.stv3_raw_issue_field_v1 AS link
                JOIN public.stv3_raw_field_v1 AS field ON field.field_id=link.field_id
                WHERE link.forecast_issue_id=%s''',(issue_id,)).fetchall()
            output.append({'target':target,'issue':float(issue),'region':region,
                'field':{k:field.get(k) for k in ('primary_provider','support_state','known_sample_count','source_conflict',
                    'issued_at','policy_version','threshold_mm_h','threshold_operator','intensity_threshold_mm_h')},
                'episode_count':len(field.get('intervals',())),
                'intensity_count':len(field.get('intensity_intervals',())),
                'entry':forecast.get('entry_at'),'exit':forecast.get('exit_at'),
                'input_fingerprints':len(inputs),'bound_inputs':len(bound),
                'missing_inputs':len(set(inputs)-{b[0] for b in bound})})
        cycles = con.execute('''SELECT extract(epoch FROM evaluated_at),complete,gaps
            FROM public.stv3_raw_cycle_v1 WHERE code_revision=%s
            ORDER BY evaluated_at DESC LIMIT 2''',
            (args.code,)).fetchall()
        print(json.dumps({'targets':output,'cycles':[
            {'evaluated_at':float(t),'complete':complete,'gaps':gaps} for t,complete,gaps in cycles]}))
    finally:
        con.rollback(); con.close()


if __name__ == '__main__':
    main()
