"""Bounded migration/write/retention check, always rolled back; no credentials printed."""
from pathlib import Path
import base64
import hashlib
import importlib.util
import json
import subprocess
import sys
from uuid import uuid4

import numpy as np
import psycopg

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from storm_tracker_worker.thin_postgis_raw_archive import RawArchiveBatch, write_archive_batch
from storm_tracker_worker.thin_raw_archive import RawArchiveBuffer, RawCaptureContext, capture_scope, capture_received
from storm_tracker_worker.thin_raw_field_codec import snapshot_field_job, decode_radar_field
from storm_tracker_worker.thin_provider_adapters import ThinRadarField


def existing_admin_connection():
    spec = importlib.util.spec_from_file_location('existing_route', '/home/ikke/storm-tracker/.analysis/apply_live_postgis_migration.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    encoded = base64.b64encode(module.REMOTE_OPTIONS.encode()).decode()
    ssh = module.SSH[:-1] + ['-o', 'MACs=hmac-sha2-256-etm@openssh.com', module.SSH[-1]]
    result = subprocess.run(ssh + [f"sudo -n python3 -c 'import base64;exec(base64.b64decode(\"{encoded}\"))'"],
                            capture_output=True, text=True, timeout=45)
    if result.returncode:
        raise RuntimeError('existing_admin_route_unavailable')
    options = json.loads(result.stdout)
    return psycopg.connect(**module.connection_kwargs(options))


def main():
    connection = existing_admin_connection()
    stage = 'begin'
    try:
        connection.execute("SET LOCAL statement_timeout='15s'")
        connection.execute("SET LOCAL lock_timeout='3s'")
        migration_root = ROOT / 'storm_tracker_worker/postgis_migrations'
        stage = '0008_scientific_passage_forecast.sql'
        source = (migration_root / stage).read_text().strip()
        assert source.startswith('BEGIN;') and source.endswith('COMMIT;')
        # Existing versioned migration; retain its statements in OUR rollback
        # transaction rather than executing its standalone transaction wrapper.
        connection.execute(source[len('BEGIN;'):-len('COMMIT;')])
        for _ in range(2):
            for name in ('0009_raw_replay_archive.sql','0010_one_month_weather_retention.sql'):
                stage = name
                connection.execute((migration_root / name).read_text())
        stage = 'fixture-write-and-readback'
        code = 'dry-run-' + str(uuid4())
        connection.execute('INSERT INTO public.stv3_raw_code_revision_v1 VALUES(%s,%s,%s,%s,%s::jsonb)',
                           (code,code,'dry-run','dry-run','{}'))
        now = connection.execute('SELECT extract(epoch FROM clock_timestamp())').fetchone()[0]
        now = float(now)
        buffer = RawArchiveBuffer()
        context = RawCaptureContext(str(uuid4()),'netatmo','weather',now,0,('dry-run-region',),(),code)
        body = b'{"body":[]}'
        with capture_scope(context,buffer):
            capture_received('https://api.netatmo.com/api/getpublicdata',body,received_at=now)
            capture_received('https://api.netatmo.com/api/getpublicdata',body,received_at=now+1)
        receipts = buffer.pending()
        field = ThinRadarField('knmi_radar','qpe',now,'EPSG:28992',1,2,1000,1000,
                               np.array([[1.25,np.nan]],dtype='>f8'),np.array([[True,False]]))
        job = snapshot_field_job(field,region_revision_id='dry-run-region',source_capture_ids=tuple(r.capture_id for r in receipts))
        cycle_id = str(uuid4())
        with connection.cursor() as cursor:
            counts = write_archive_batch(cursor,RawArchiveBatch(cycle_id,now+2,code,{},receipts,(job,),(),()))
        assert counts['raw_complete_cycles'] == 1
        actual = connection.execute('SELECT payload FROM public.stv3_raw_payload_v1 WHERE sha256=%s',(hashlib.sha256(body).hexdigest(),)).fetchone()[0]
        assert bytes(actual) == body
        assert connection.execute('SELECT count(*) FROM public.stv3_raw_capture_v1 WHERE capture_id=ANY(%s)',([r.capture_id for r in receipts],)).fetchone()[0] == 2
        from storm_tracker_worker.thin_postgis_raw_archive import read_archived_field
        field_id = connection.execute('SELECT field_id FROM public.stv3_raw_field_source_v1 WHERE capture_id=%s',(receipts[0].capture_id,)).fetchone()[0]
        try:
            read_archived_field(connection.cursor(),field_id,cycle_id=cycle_id,available_at=now)
        except ValueError as error:
            assert str(error) == 'raw_field_not_available_at_issue'
        else:
            raise AssertionError('future source receipt accepted')
        decoded = decode_radar_field(read_archived_field(connection.cursor(),field_id,cycle_id=cycle_id,available_at=now+2))
        assert decoded['arrays']['values'].dtype.str == '>f8'
        assert decoded['arrays']['values'].tobytes() == field.rain_rate_mm_h.tobytes()
        assert decoded['arrays']['valid_mask'].tobytes() == field.valid_mask.tobytes()
        old_id = 'dry-run-expired-' + str(uuid4())
        connection.execute("INSERT INTO public.stv3_raw_capture_v1(capture_id,provider_id,product_id,requested_at,received_at,safe_context,disposition) VALUES(%s,'test','test',clock_timestamp()-interval '1 month 1 day',clock_timestamp()-interval '1 month 1 day','{}','received')",(old_id,))
        stage = 'retention-execution'
        result = connection.execute('SELECT public.stv3_enforce_retention()').fetchone()[0]
        assert result['status'] == 'applied'
        assert not connection.execute('SELECT EXISTS(SELECT 1 FROM public.stv3_raw_capture_v1 WHERE capture_id=%s)',(old_id,)).fetchone()[0]
        assert connection.execute('SELECT EXISTS(SELECT 1 FROM public.stv3_raw_capture_v1 WHERE capture_id=%s)',(receipts[0].capture_id,)).fetchone()[0]
        print(json.dumps({'schema_idempotent':True,'native_readback_exact':True,'two_real_receipts':True,
                          'monthly_retention_executed':True,'field_readback_exact':True,
                          'future_receipt_excluded':True,'transaction':'rolled_back'}))
    except Exception as error:
        print(json.dumps({'check':'failed','stage':stage,'error_type':type(error).__name__,
                          'sqlstate':getattr(error,'sqlstate',None),
                          'sql_context':getattr(getattr(error,'diag',None),'context',None)}))
        return 1
    finally:
        connection.rollback()
        connection.close()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
