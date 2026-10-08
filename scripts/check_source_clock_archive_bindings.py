"""Bounded existing-archive binding/readback check; always rolled back."""
from pathlib import Path
import importlib.util
import sys
import json
import time
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from storm_tracker_worker.thin_postgis_raw_archive import (
    RawArchiveBatch, write_archive_batch, read_archived_field)
from storm_tracker_worker.thin_raw_field_codec import decode_radar_field


def connection():
    path = '/tmp/storm-raw-storage-pIx2yF/scripts/check_live_raw_archive_schema.py'
    spec = importlib.util.spec_from_file_location('existing_archive_route', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.existing_admin_connection()


def main():
    con = connection()
    try:
        con.execute("SET LOCAL statement_timeout='15s'")
        code = con.execute('SELECT code_revision FROM public.stv3_raw_code_revision_v1 LIMIT 1').fetchone()[0]
        row = con.execute('''SELECT metadata->>'region_revision_id',
            metadata->'identity'->>'content_fingerprint', extract(epoch FROM clock_timestamp())
            FROM public.stv3_raw_field_v1 WHERE metadata->>'provider_id'='knmi_radar'
            AND metadata->>'kind'='quantitative'
            ORDER BY observed_at DESC LIMIT 1''').fetchone()
        assert row is not None
        revision, fingerprint, now = row
        cycle, issue = str(uuid4()), 'check-'+str(uuid4())
        start = time.monotonic()
        with con.cursor() as cursor:
            counts = write_archive_batch(cursor, RawArchiveBatch(cycle,float(now),code,
                {'forecast_inputs':{issue:[fingerprint]}},(),(),((issue,revision),),()))
            assert counts['raw_complete_cycles'] == 1
            field_id = con.execute('''SELECT field_id FROM public.stv3_raw_issue_field_v1
                WHERE forecast_issue_id=%s AND cycle_id=%s LIMIT 1''',(issue,cycle)).fetchone()[0]
            record = read_archived_field(cursor,field_id,cycle_id=cycle,available_at=float(now))
            decoded = decode_radar_field(record)
            assert decoded['metadata']['identity']['content_fingerprint'] == fingerprint
        print(json.dumps({'binding_readback_exact':True,'elapsed_seconds':round(time.monotonic()-start,3),
            'transaction':'rolled_back'}))
    finally:
        con.rollback(); con.close()


if __name__ == '__main__':
    main()
