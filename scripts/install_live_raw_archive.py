"""Apply qualified additive archive DDL using the existing admin route only."""
import argparse
import hashlib
import json
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from check_live_raw_archive_schema import existing_admin_connection
from storm_tracker_worker.release_artifacts import verify_release_candidate_evidence
from storm_tracker_worker.thin_postgis_raw_archive import apply_raw_archive_migration
from storm_tracker_worker.thin_raw_code import source_tree_digest


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--evidence',type=Path,required=True)
    parser.add_argument('--rollback',type=Path,required=True)
    parser.add_argument('--apply',action='store_true')
    args=parser.parse_args()
    evidence=verify_release_candidate_evidence(args.evidence)
    backup=json.loads(args.rollback.read_text())
    code=source_tree_digest(ROOT/'storm_tracker_worker')
    connection=existing_admin_connection()
    try:
        connection.execute("SET LOCAL statement_timeout='15s'")
        connection.execute("SET LOCAL lock_timeout='3s'")
        old=connection.execute("SELECT pg_get_functiondef('public.stv3_enforce_retention()'::regprocedure)").fetchone()[0]
        if hashlib.sha256(old.encode()).hexdigest()!=backup['retention_function_sha256']:
            raise ValueError('retention_changed_since_rollback_capture')
        if not args.apply:
            print(json.dumps({'qualified_evidence':True,'code_revision':code,'transaction':'rolled_back'}))
            return
        apply_raw_archive_migration(connection)
        # Register the exact verified source plus complete release archives.
        connection.execute('''INSERT INTO public.stv3_raw_code_revision_v1
          (code_revision,source_tree_digest,worker_archive_sha256,integration_archive_sha256,safe_artifact_reference)
          VALUES(%s,%s,%s,%s,%s::jsonb) ON CONFLICT DO NOTHING''',
          (code,code,evidence['archives']['worker']['sha256'],evidence['archives']['integration']['sha256'],
           json.dumps({'evidence':args.evidence.name,'worker_version':evidence['worker_version'],
                       'integration_version':evidence['integration_version']})))
        connection.commit()
        migrations=connection.execute("SELECT migration_id FROM stv3_meta.schema_migrations WHERE migration_id LIKE '0008%' OR migration_id LIKE '0009%' OR migration_id LIKE '0010%' ORDER BY migration_id").fetchall()
        print(json.dumps({'applied':[row[0] for row in migrations],'code_revision':code,'status':'installed'}))
    finally:
        connection.rollback()
        connection.close()


if __name__=='__main__':
    main()
