"""Bounded read-only native-field export for the approved source-clock repair."""
import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from check_live_raw_archive_schema import existing_admin_connection
from storm_tracker_worker.thin_postgis_raw_archive import read_archived_field
from storm_tracker_worker.thin_raw_field_codec import decode_radar_field

ROOT = Path('/home/ikke/storm-tracker/.analysis/oldenburg-replay-20261008')
DEST = ROOT / 'source-clock-corrected'


def main():
    if (DEST / 'native-manifest.json').exists():
        raise RuntimeError('native_export_already_exists_do_not_overwrite')
    audit = json.loads((ROOT / 'event-audit.json').read_text())
    cycles = {c['cycle_id']: c for c in audit['cycles']}
    regions = sorted({l['region_revision_id'] for l in audit['links']})
    DEST.mkdir(exist_ok=True)
    (DEST / 'native-arrays').mkdir(exist_ok=True)
    con = existing_admin_connection()
    total_bytes = 0
    fields, memberships, errors = {}, [], []
    try:
        con.execute('SET TRANSACTION READ ONLY')
        con.execute("SET LOCAL statement_timeout='20s'")
        rows = con.execute('''SELECT l.cycle_id,l.region_revision_id,l.field_id
            FROM public.stv3_raw_cycle_field_v1 l
            WHERE l.cycle_id=ANY(%s) AND l.region_revision_id=ANY(%s)
              AND l.role='native_input' ''', (list(cycles), regions)).fetchall()
        rows.sort(key=lambda r: (cycles[r[0]]['evaluated_at'], r[2]))
        for cid, region, fid in rows:
            available = datetime.fromisoformat(cycles[cid]['evaluated_at']).timestamp()
            receipts = con.execute('''SELECT c.capture_id,c.provider_id,c.received_at,
                c.product_valid_at FROM public.stv3_raw_cycle_field_source_v1 l
                JOIN public.stv3_raw_capture_v1 c USING(capture_id)
                WHERE l.cycle_id=%s AND l.field_id=%s''', (cid, fid)).fetchall()
            if not receipts or any(r[2].timestamp() > available for r in receipts):
                errors.append({'cycle_id': cid, 'field_id': fid,
                               'reason': 'missing_or_future_receipt'})
                continue
            memberships.append({'cycle_id': cid, 'field_id': fid,
                                'available_at': available,
                                'capture_ids': [r[0] for r in receipts],
                                'received_at': [r[2].timestamp() for r in receipts]})
            if fid in fields:
                continue
            try:
                raw = read_archived_field(con.cursor(), fid, cycle_id=cid,
                                          available_at=available)
                decoded = decode_radar_field(raw)
            except ValueError as error:
                errors.append({'cycle_id': cid, 'field_id': fid,
                               'reason': str(error)})
                continue
            m = decoded['metadata']
            if m['kind'] != 'quantitative' or m['observed_at'] > available:
                errors.append({'cycle_id': cid, 'field_id': fid,
                               'reason': 'nonquantitative_or_future_product'})
                continue
            if any(r[1] != m['provider_id'] for r in receipts):
                raise RuntimeError('native_provider_receipt_mismatch')
            refs = {}
            for name, payload in raw.arrays.items():
                digest = hashlib.sha256(payload).hexdigest()
                path = DEST / 'native-arrays' / digest
                if not path.exists():
                    total_bytes += len(payload)
                    if total_bytes > 512*1024*1024:
                        raise RuntimeError('native_export_exceeds_512MiB_bound')
                    path.write_bytes(payload)
                refs[name] = {'file': str(path.relative_to(DEST)),
                              'sha256': digest,
                              'descriptor': dict(raw.array_descriptors[name])}
            fields[fid] = {'field_id': fid, 'metadata': m, 'arrays': refs,
                           'available_at': available, 'first_cycle_id': cid}
        result = {'scope': 'Read-only exact native inputs from the original Oldenburg cycle window only',
                  'fields': sorted(fields.values(), key=lambda f: (f['available_at'], f['field_id'])),
                  'memberships': memberships, 'errors': errors,
                  'native_bytes_written': total_bytes}
        (DEST / 'native-manifest.json').write_text(json.dumps(result, separators=(',', ':')))
        print(json.dumps({'unique_fields': len(fields), 'memberships': len(memberships),
                          'errors': errors, 'bytes': total_bytes}), flush=True)
    finally:
        con.rollback()
        con.close()


if __name__ == '__main__':
    main()
