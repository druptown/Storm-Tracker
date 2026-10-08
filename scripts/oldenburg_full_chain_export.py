"""Read-only full recorded input export; never fetch a weather provider."""
from collections import Counter
import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path

from check_source_clock_archive_bindings import connection

DATA = Path('/home/ikke/storm-tracker/.analysis/oldenburg-replay-20261008')
DEST = DATA/'full-chain107-inputs'


def epoch(value):
    return datetime.fromisoformat(value).timestamp()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--resume', action='store_true')
    args = parser.parse_args()
    audit_path = DATA/'event-audit.json'
    audit = json.loads(audit_path.read_text())
    ids = sorted({cid for cycle in audit['cycles']
                  for refs in cycle['policy']['provider_inputs'].values() for cid in refs})
    con = connection()
    try:
        con.execute('SET TRANSACTION READ ONLY')
        con.execute("SET LOCAL statement_timeout='30s'")
        # New export owns its files. Existing experiment results cannot be replaced.
        if args.resume:
            assert DEST.is_dir() and not (DEST/'manifest.json').exists()
        else:
            DEST.mkdir(exist_ok=False)
        (DEST/'payloads').mkdir(exist_ok=args.resume)
        rows = con.execute('''SELECT capture_id,provider_id,product_id,
            requested_at,received_at,product_valid_at,safe_context,disposition,
            reason_code,payload_sha256 FROM public.stv3_raw_capture_v1
            WHERE capture_id=ANY(%s)''',(ids,)).fetchall()
        captures = []; payloads = {}; total = 0
        for cid,provider,product,requested,received,valid,context,status,reason,digest in rows:
            captures.append({'capture_id':cid,'provider_id':provider,'product_id':product,
                'requested_at':requested.timestamp(),'received_at':received.timestamp(),
                'product_valid_at':None if valid is None else valid.timestamp(),
                'context':context,'disposition':status,'reason_code':reason,'sha256':digest})
            if digest is None or digest in payloads:
                continue
            local = DATA/'payloads'/digest
            if not local.exists():
                local = DEST/'payloads'/digest
            if local.exists():
                body = local.read_bytes()
                path = local
            else:
                row = con.execute('SELECT payload,byte_count FROM public.stv3_raw_payload_v1 WHERE sha256=%s',
                    (digest,)).fetchone()
                if row is None:
                    payloads[digest] = {'missing':True}; continue
                body = bytes(row[0]); assert len(body) == row[1]
                total += len(body)
                if total > 16*1024*1024*1024:
                    raise RuntimeError('full_chain_export_exceeds_sixteen_gib')
                path = DEST/'payloads'/digest
                path.write_bytes(body)
            assert hashlib.sha256(body).hexdigest() == digest
            payloads[digest] = {'file':str(path.relative_to(DATA)),'bytes':len(body),'sha256':digest}
            if len(payloads)%100 == 0:
                print(json.dumps({'exported_payloads':len(payloads),'new_bytes':total}),flush=True)
        missing = sorted(set(ids)-{r['capture_id'] for r in captures})
        # Capture linkage is input provenance, not evidence of a successful decode.
        manifest = {'audit_sha256':hashlib.sha256(audit_path.read_bytes()).hexdigest(),
            'transaction':'read-only, rolled back','captures':captures,'payloads':payloads,
            'missing_capture_ids':missing,'new_payload_bytes':total,
            'providers':dict(Counter(r['provider_id'] for r in captures)),
            'dispositions':dict(Counter(r['disposition'] for r in captures))}
        (DEST/'manifest.json').write_text(json.dumps(manifest,indent=2,allow_nan=False))
        print(json.dumps({k:v for k,v in manifest.items() if k not in ('captures','payloads')}))
    finally:
        con.rollback(); con.close()


if __name__ == '__main__':
    main()
