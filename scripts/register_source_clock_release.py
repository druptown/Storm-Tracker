"""Register verified packaged code only; never migrate or alter retention."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
import tarfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from check_source_clock_archive_bindings import connection
from storm_tracker_worker.release_artifacts import verify_release_candidate_evidence
from storm_tracker_worker.thin_raw_code import source_tree_digest


def archive_digest(archive):
    digest = hashlib.sha256()
    with tarfile.open(archive, 'r:gz') as bundle:
        members = sorted((m for m in bundle.getmembers() if m.isfile()
            and Path(m.name).suffix in {'.py', '.sql'}), key=lambda m: m.name)
        for member in members:
            name = Path(member.name).relative_to('storm_tracker_worker').as_posix().encode()
            body = bundle.extractfile(member).read()
            digest.update(len(name).to_bytes(8, 'big')); digest.update(name)
            digest.update(len(body).to_bytes(8, 'big')); digest.update(body)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--evidence', type=Path, required=True)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    evidence = verify_release_candidate_evidence(args.evidence)
    code = source_tree_digest(Path(__file__).resolve().parents[1]/'storm_tracker_worker')
    assert archive_digest(args.evidence.parent/evidence['archives']['worker']['filename']) == code
    con = connection()
    try:
        con.execute("SET LOCAL statement_timeout='15s'")
        hashes = (evidence['archives']['worker']['sha256'], evidence['archives']['integration']['sha256'])
        if args.apply:
            con.execute('''INSERT INTO public.stv3_raw_code_revision_v1
                (code_revision,source_tree_digest,worker_archive_sha256,integration_archive_sha256,safe_artifact_reference)
                VALUES(%s,%s,%s,%s,%s::jsonb) ON CONFLICT DO NOTHING''',
                (code,code,*hashes,json.dumps({'evidence':args.evidence.name,
                    'worker_version':evidence['worker_version'],
                    'integration_version':evidence['integration_version']})))
            row = con.execute('''SELECT source_tree_digest,worker_archive_sha256,integration_archive_sha256
                FROM public.stv3_raw_code_revision_v1 WHERE code_revision=%s''',(code,)).fetchone()
            assert row == (code,*hashes)
            con.commit()
        print(json.dumps({'code_revision':code,'artifact_source_exact':True,
            'registered':args.apply,'schema_or_retention_changed':False}))
    finally:
        con.rollback(); con.close()


if __name__ == '__main__':
    main()
