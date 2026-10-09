"""Transactional persistence for native inputs and exact consumed radar fields."""
from __future__ import annotations
from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping
from .thin_raw_archive import RawCapture
from .thin_raw_field_codec import RawFieldJob, SerializedRadarField, encode_radar_field


@dataclass(frozen=True, slots=True)
class RawArchiveBatch:
    cycle_id: str
    evaluated_at: float
    code_revision: str
    policy: Mapping[str, Any]
    captures: tuple[RawCapture, ...]
    fields: tuple[RawFieldJob, ...]
    issue_bindings: tuple[tuple[str, str], ...]  # actual issue ID, region revision
    gaps: tuple[str, ...]


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def apply_raw_archive_migration(connection):
    """Deployment-only, using an already authorized existing connection."""
    root = Path(__file__).parent / 'postgis_migrations'
    try:
        source = (root / '0008_scientific_passage_forecast.sql').read_text().strip()
        if not source.startswith('BEGIN;') or not source.endswith('COMMIT;'):
            raise ValueError('migration_transaction_wrapper_invalid')
        connection.execute(source[len('BEGIN;'):-len('COMMIT;')])
        connection.execute((root / '0009_raw_replay_archive.sql').read_text())
        connection.execute((root / '0010_one_month_weather_retention.sql').read_text())
        connection.commit()
    except BaseException:
        connection.rollback()
        raise


def write_archive_batch(cursor, batch):
    counts = {'raw_captures': 0, 'raw_fields': 0, 'raw_payload_bytes': 0, 'raw_complete_cycles': 0}
    payload_ids = set()
    def payload(body):
        digest = hashlib.sha256(body).hexdigest()
        if digest not in payload_ids:
            cursor.execute('INSERT INTO public.stv3_raw_payload_v1(sha256,payload,byte_count) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING', (digest,body,len(body)))
            payload_ids.add(digest)
            counts['raw_payload_bytes'] += len(body)
        return digest
    for receipt in batch.captures:
        digest = payload(receipt.payload) if receipt.payload is not None else None
        context = asdict(receipt.context)
        context['source'] = dict(receipt.safe_source)
        context['transport'] = receipt.transport
        context['http_status'] = receipt.http_status
        context['content_type'] = receipt.content_type
        cursor.execute('''INSERT INTO public.stv3_raw_capture_v1
          (capture_id,payload_sha256,provider_id,product_id,requested_at,received_at,product_valid_at,safe_context,disposition,reason_code)
          VALUES(%s,%s,%s,%s,to_timestamp(%s),to_timestamp(%s),to_timestamp(%s),%s::jsonb,%s,%s) ON CONFLICT DO NOTHING''',
          (receipt.capture_id,digest,receipt.context.provider_id,receipt.context.product_id,
           receipt.context.requested_at,receipt.received_at,receipt.product_valid_at,_json(context),receipt.disposition,receipt.reason_code))
        counts['raw_captures'] += 1
    cursor.execute('SELECT EXISTS(SELECT 1 FROM public.stv3_raw_code_revision_v1 WHERE code_revision=%s AND source_tree_digest=%s)',(batch.code_revision,batch.code_revision))
    code_ok = bool(cursor.fetchone()[0])
    gaps = set(batch.gaps)
    if not code_ok:
        gaps.add('code_reference_missing')
    fields = []
    for job in batch.fields:
        record = encode_radar_field(job.field, region_revision_id=job.region_revision_id,
                                    confidence=job.confidence, contributor_mask=job.contributor_mask)
        refs = {name: {'payload_sha256': payload(body), **dict(record.array_descriptors[name])}
                for name, body in record.arrays.items()}
        cursor.execute('''INSERT INTO public.stv3_raw_field_v1(field_id,metadata,array_payload_refs,observed_at)
          VALUES(%s,%s::jsonb,%s::jsonb,to_timestamp(%s)) ON CONFLICT DO NOTHING''',
          (record.field_id,_json(dict(record.metadata)),_json(refs),job.field.observed_at))
        for capture_id in job.source_capture_ids:
            cursor.execute('SELECT EXISTS(SELECT 1 FROM public.stv3_raw_capture_v1 WHERE capture_id=%s)', (capture_id,))
            if not cursor.fetchone()[0]:
                gaps.add('field_source_capture_missing')
                continue
            cursor.execute('SELECT received_at>to_timestamp(%s) FROM public.stv3_raw_capture_v1 WHERE capture_id=%s',
                           (batch.evaluated_at,capture_id))
            if cursor.fetchone()[0]:
                gaps.add('field_source_received_after_issue')
            cursor.execute('''INSERT INTO public.stv3_raw_field_source_v1(field_id,capture_id)
              SELECT %s,capture_id FROM public.stv3_raw_capture_v1 WHERE capture_id=%s ON CONFLICT DO NOTHING''',(record.field_id,capture_id))
        if not job.source_capture_ids:
            gaps.add('field_source_capture_missing')
        fields.append((job,record.field_id))
        counts['raw_fields'] += 1
    # Source-clock forecasts also consume the preceding native field, which
    # may be absent from this cycle's current provider cache. Resolve exact
    # immutable fingerprints in the same region, with causal receipt support.
    forecast_fields = []
    explicit_inputs = batch.policy.get('forecast_inputs', {})
    for issue_id, revision_id in batch.issue_bindings:
        if issue_id not in explicit_inputs:
            continue
        expected = set(explicit_inputs[issue_id])
        cursor.execute('''SELECT DISTINCT field.field_id,
          field.metadata->'identity'->>'content_fingerprint'
          FROM public.stv3_raw_field_v1 AS field
          JOIN public.stv3_raw_field_source_v1 AS source ON source.field_id=field.field_id
          JOIN public.stv3_raw_capture_v1 AS capture ON capture.capture_id=source.capture_id
          WHERE field.metadata->>'region_revision_id'=%s
            AND field.metadata->'identity'->>'content_fingerprint'=ANY(%s)
            AND field.observed_at<=to_timestamp(%s)
            AND capture.received_at<=to_timestamp(%s)
            AND capture.disposition='received' ''',
          (revision_id,sorted(expected),batch.evaluated_at,batch.evaluated_at))
        rows = cursor.fetchall()
        if expected - {fingerprint for _,fingerprint in rows}:
            gaps.add('forecast_input_missing_or_unavailable')
        forecast_fields.extend((issue_id,revision_id,field_id) for field_id,_ in rows)
    complete = not gaps
    cursor.execute('''INSERT INTO public.stv3_raw_cycle_v1(cycle_id,evaluated_at,code_revision,policy,complete,gaps)
      VALUES(%s,to_timestamp(%s),%s,%s::jsonb,%s,%s::jsonb) ON CONFLICT DO NOTHING''',
      (batch.cycle_id,batch.evaluated_at,batch.code_revision,_json(dict(batch.policy)),complete,_json(sorted(gaps))))
    for job, field_id in fields:
        cursor.execute('''INSERT INTO public.stv3_raw_cycle_field_v1(cycle_id,region_revision_id,field_id,role)
          VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING''',(batch.cycle_id,job.region_revision_id,field_id,job.role))
        for capture_id in job.source_capture_ids:
            cursor.execute('''INSERT INTO public.stv3_raw_cycle_field_source_v1(cycle_id,field_id,capture_id)
              SELECT %s,%s,capture_id FROM public.stv3_raw_capture_v1 WHERE capture_id=%s ON CONFLICT DO NOTHING''',
              (batch.cycle_id,field_id,capture_id))
        if job.role == 'used':
            for issue_id, revision_id in batch.issue_bindings:
                if revision_id == job.region_revision_id and issue_id not in explicit_inputs:
                    cursor.execute('''INSERT INTO public.stv3_raw_issue_field_v1(forecast_issue_id,cycle_id,region_revision_id,field_id)
                      VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING''',(issue_id,batch.cycle_id,revision_id,field_id))
    for issue_id,revision_id,field_id in forecast_fields:
        cursor.execute('''INSERT INTO public.stv3_raw_cycle_field_v1(cycle_id,region_revision_id,field_id,role)
          VALUES(%s,%s,%s,'forecast_input') ON CONFLICT DO NOTHING''',
          (batch.cycle_id,revision_id,field_id))
        cursor.execute('''INSERT INTO public.stv3_raw_cycle_field_source_v1(cycle_id,field_id,capture_id)
          SELECT %s,source.field_id,source.capture_id
          FROM public.stv3_raw_field_source_v1 AS source
          JOIN public.stv3_raw_capture_v1 AS capture ON capture.capture_id=source.capture_id
          WHERE source.field_id=%s AND capture.received_at<=to_timestamp(%s)
            AND capture.disposition='received' ON CONFLICT DO NOTHING''',
          (batch.cycle_id,field_id,batch.evaluated_at))
        cursor.execute('''INSERT INTO public.stv3_raw_issue_field_v1(forecast_issue_id,cycle_id,region_revision_id,field_id)
          VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING''',
          (issue_id,batch.cycle_id,revision_id,field_id))
    counts['raw_complete_cycles'] = int(complete)
    return counts


def read_archived_field(cursor, field_id, *, cycle_id, available_at):
    """Lossless input for ONE recorded issue/cycle; never use later receipts."""
    cursor.execute('''SELECT count(*),bool_or(capture.received_at>to_timestamp(%s))
      FROM public.stv3_raw_cycle_field_source_v1 AS link
      JOIN public.stv3_raw_capture_v1 AS capture ON capture.capture_id=link.capture_id
      WHERE link.cycle_id=%s AND link.field_id=%s''',(available_at,cycle_id,field_id))
    count, future = cursor.fetchone()
    if count == 0 or future:
        raise ValueError('raw_field_not_available_at_issue')
    cursor.execute('SELECT metadata,array_payload_refs FROM public.stv3_raw_field_v1 WHERE field_id=%s',(field_id,))
    record = cursor.fetchone()
    if record is None:
        raise ValueError('raw_field_missing')
    metadata, refs = record
    arrays = {}
    for name, descriptor in refs.items():
        digest = descriptor['payload_sha256']
        cursor.execute('SELECT payload FROM public.stv3_raw_payload_v1 WHERE sha256=%s',(digest,))
        body = cursor.fetchone()
        if body is None or hashlib.sha256(bytes(body[0])).hexdigest() != digest:
            raise ValueError('raw_payload_integrity_mismatch')
        arrays[name] = bytes(body[0])
    return SerializedRadarField(field_id,metadata,arrays,refs)
