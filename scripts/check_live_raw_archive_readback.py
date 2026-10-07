"""Read-only live archive verification; prints compact facts, never source bodies."""
import hashlib
import json
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from check_live_raw_archive_schema import existing_admin_connection
from storm_tracker_worker.thin_postgis_raw_archive import read_archived_field
from storm_tracker_worker.thin_raw_field_codec import decode_radar_field
from storm_tracker_worker.thin_provider_adapters import _radar_content_fingerprint


def main():
    connection=existing_admin_connection()
    try:
        connection.execute('SET TRANSACTION READ ONLY')
        connection.execute("SET LOCAL statement_timeout='10s'")
        counts={name:connection.execute('SELECT count(*) FROM public.'+name).fetchone()[0]
                for name in ('stv3_raw_capture_v1','stv3_raw_field_v1','stv3_raw_cycle_v1',
                             'stv3_raw_issue_field_v1','stv3_target_forecast_issue','stv3_observation','stv3_station_measurement')}
        bodies=connection.execute('''SELECT DISTINCT ON(capture.provider_id) capture.provider_id,
          capture.capture_id,capture.received_at, payload.sha256,payload.byte_count,payload.payload
          FROM public.stv3_raw_capture_v1 AS capture
          JOIN public.stv3_raw_payload_v1 AS payload ON payload.sha256=capture.payload_sha256
          ORDER BY capture.provider_id,capture.received_at DESC''').fetchall()
        native=[]
        for provider,identifier,received_at,digest,size,body in bodies:
            assert len(body)==size and hashlib.sha256(bytes(body)).hexdigest()==digest
            native.append({'provider':provider,'capture_id':identifier,'received_at':received_at.isoformat(),'byte_count':size,'sha256_exact':True})
        # Read every deduplicated native body, including tile/listing inputs
        # that are not the last receipt for a provider. Never print the body.
        native_payloads=connection.execute('''SELECT DISTINCT payload.sha256,payload.byte_count,payload.payload
            FROM public.stv3_raw_capture_v1 AS capture
            JOIN public.stv3_raw_payload_v1 AS payload ON payload.sha256=capture.payload_sha256''').fetchall()
        for digest,size,body in native_payloads:
            assert len(body)==size and hashlib.sha256(bytes(body)).hexdigest()==digest
        receipt_stats=[{'provider':provider,'receipts':amount,'first_received_at':first.isoformat(),
                        'latest_received_at':last.isoformat(),'content_types':types,'transports':transports}
                       for provider,amount,first,last,types,transports in connection.execute('''
            SELECT provider_id,count(*),min(received_at),max(received_at),
                   array_agg(DISTINCT safe_context->>'content_type'),
                   array_agg(DISTINCT safe_context->>'transport')
            FROM public.stv3_raw_capture_v1 GROUP BY provider_id ORDER BY provider_id''').fetchall()]
        used_products=[{'provider':provider,'region':region,'observed_at':observed_at.isoformat(),
                        'field_count':amount}
                       for provider,region,observed_at,amount in connection.execute('''
            SELECT field.metadata->>'provider_id',link.region_revision_id,field.observed_at,
                   count(DISTINCT field.field_id)
            FROM public.stv3_raw_cycle_field_v1 AS link
            JOIN public.stv3_raw_field_v1 AS field ON field.field_id=link.field_id
            WHERE link.role='used'
            GROUP BY field.metadata->>'provider_id',link.region_revision_id,field.observed_at
            ORDER BY link.region_revision_id,field.observed_at''').fetchall()]
        all_cycle_status=connection.execute('''SELECT count(*),count(*) FILTER(WHERE NOT complete),
            max(evaluated_at),array_agg(DISTINCT code_revision) FROM public.stv3_raw_cycle_v1''').fetchone()
        gap_history=[{'reason':reason,'cycles':amount,'latest_at':last.isoformat()}
                     for reason,amount,last in connection.execute('''
            SELECT reason,count(*),max(evaluated_at)
            FROM public.stv3_raw_cycle_v1 CROSS JOIN LATERAL jsonb_array_elements_text(gaps) AS reason
            GROUP BY reason ORDER BY reason''').fetchall()]
        latest=connection.execute('''SELECT cycle_id,extract(epoch FROM evaluated_at),complete,gaps,policy
          FROM public.stv3_raw_cycle_v1 ORDER BY evaluated_at DESC LIMIT 2''').fetchall()
        cycles=[]
        for identifier,evaluated,complete,gaps,policy in latest:
            links=connection.execute('''SELECT field_id,region_revision_id,role FROM public.stv3_raw_cycle_field_v1
              WHERE cycle_id=%s ORDER BY region_revision_id,role,field_id''',(identifier,)).fetchall()
            fields=[]
            for field_id,region,role in links:
                try:
                    decoded=decode_radar_field(read_archived_field(connection.cursor(),field_id,cycle_id=identifier,available_at=float(evaluated)))
                except ValueError as error:
                    fields.append({'field_id':field_id,'region':region,'role':role,'error_code':str(error)})
                    continue
                metadata=decoded['metadata'];arrays=decoded['arrays']
                if metadata['kind']=='quantitative':
                    assert _radar_content_fingerprint(arrays['values'],arrays['valid_mask'])==metadata['identity']['content_fingerprint']
                fields.append({'field_id':field_id,'region':region,'role':role,'provider':metadata['provider_id'],
                               'observed_at':metadata['observed_at'],'kind':metadata['kind'],
                               'quantity':metadata['quantity'],'unit':metadata['unit'],
                               'crs':metadata['crs'],'affine':metadata['affine'],
                               'arrays':{name:{'dtype':value.dtype.str,'shape':list(value.shape),'bytes':value.nbytes}
                                         for name,value in arrays.items()},'exact_decode':True})
            issues=connection.execute('''SELECT count(*),count(issue.forecast_issue_id)
              FROM public.stv3_raw_issue_field_v1 AS link LEFT JOIN public.stv3_target_forecast_issue AS issue
              ON issue.forecast_issue_id=link.forecast_issue_id WHERE cycle_id=%s''',(identifier,)).fetchone()
            cycles.append({'cycle_id':identifier,'evaluated_at':float(evaluated),'complete':complete,'gaps':gaps,
                           'targets':[{k:t[k] for k in ('target_id','latitude','longitude')} for t in policy['targets']],
                           'fields':fields,'issue_bindings':issues[0],'existing_issues':issues[1]})
        retention=connection.execute("SELECT last_run_at,last_result FROM public.stv3_retention_state WHERE policy_id='one_month_weather_history'").fetchone()
        print(json.dumps({'counts':counts,'native':native,'native_payloads_checked':len(native_payloads),
                          'native_payloads_all_exact':True,'receipt_stats':receipt_stats,
                          'used_products':used_products,'cycles':cycles,'gap_history':gap_history,
                          'all_cycle_status':{'total':all_cycle_status[0],'incomplete':all_cycle_status[1],
                            'latest_evaluated_at':all_cycle_status[2].isoformat() if all_cycle_status[2] else None,
                            'code_revisions':all_cycle_status[3]},
                          'retention':str(retention)},separators=(',',':')))
    finally:
        connection.rollback();connection.close()


if __name__=='__main__': main()
