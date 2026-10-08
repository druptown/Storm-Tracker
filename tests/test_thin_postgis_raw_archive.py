import importlib.util
import hashlib
from storm_tracker_worker.thin_raw_archive import RawArchiveBuffer, RawCaptureContext, capture_scope, capture_received


def api():
    spec=importlib.util.find_spec('storm_tracker_worker.thin_postgis_raw_archive')
    assert spec is not None, 'raw transactional archive missing'
    return __import__(spec.name,fromlist=[''])


class Cursor:
    def __init__(self): self.calls=[]
    def execute(self, sql, params=None): self.calls.append((sql,params))
    def fetchone(self): return (False,) if 'received_at>to_timestamp' in self.calls[-1][0] else (True,)


def test_receipts_deduplicate_payload_not_receipt_time():
    m=api(); archive=RawArchiveBuffer()
    body=b'{"body":[]}'
    context=RawCaptureContext('r','netatmo','weather',10,0,('Groningen',),(), 'code')
    with capture_scope(context,archive):
        capture_received('https://api.netatmo.com/api/getpublicdata',body,received_at=12)
        capture_received('https://api.netatmo.com/api/getpublicdata',body,received_at=14)
    batch=m.RawArchiveBatch('cycle',15,'code',{},archive.pending(),(),(),())
    cursor=Cursor(); counts=m.write_archive_batch(cursor,batch)
    receipts=[p for sql,p in cursor.calls if 'INSERT INTO public.stv3_raw_capture_v1' in sql]
    payloads=[p for sql,p in cursor.calls if 'INSERT INTO public.stv3_raw_payload_v1' in sql]
    assert len(payloads)==1
    assert payloads[0][0]==hashlib.sha256(body).hexdigest()
    assert [p[5] for p in receipts]==[12,14]
    assert len({p[0] for p in receipts})==2
    assert counts['raw_captures']==2
    assert len(archive.pending())==2  # SQL enqueue is not a durable acknowledgment.


def test_code_reference_missing_makes_cycle_explicitly_incomplete():
    m=api(); cursor=Cursor()
    cursor.fetchone=lambda: (False,)
    m.write_archive_batch(cursor,m.RawArchiveBatch('cycle',15,'code',{},(),(),(),()))
    cycle=next(p for sql,p in cursor.calls if 'INSERT INTO public.stv3_raw_cycle_v1' in sql)
    assert cycle[4] is False
    assert 'code_reference_missing' in cycle[5]


def test_nonexistent_receipt_cannot_make_used_field_complete():
    import numpy as np
    from storm_tracker_worker.thin_provider_adapters import ThinRadarField
    from storm_tracker_worker.thin_raw_field_codec import snapshot_field_job
    m = api()
    cursor = Cursor()
    cursor.fetchone = lambda: (False,) if 'stv3_raw_capture_v1' in cursor.calls[-1][0] else (True,)
    field = ThinRadarField('knmi_radar','qpe',100,'EPSG:28992',1,2,1000,1000,
                          np.array([[1.]]),np.array([[True]]))
    job = snapshot_field_job(field, region_revision_id='Groningen', source_capture_ids=('missing-receipt',))
    counts = m.write_archive_batch(cursor,m.RawArchiveBatch('cycle',100,'code',{},(),(job,),(),()))
    assert counts['raw_complete_cycles'] == 0
    cycle = next(p for sql,p in cursor.calls if 'INSERT INTO public.stv3_raw_cycle_v1' in sql)
    assert 'field_source_capture_missing' in cycle[5]


def test_source_received_after_issue_marks_cycle_incomplete():
    import numpy as np
    from storm_tracker_worker.thin_provider_adapters import ThinRadarField
    from storm_tracker_worker.thin_raw_field_codec import snapshot_field_job
    m=api();cursor=Cursor();cursor.fetchone=lambda:(True,)
    field=ThinRadarField('knmi_radar','qpe',100,'EPSG:28992',1,2,1000,1000,np.array([[1.]]),np.array([[True]]))
    job=snapshot_field_job(field,region_revision_id='Groningen',source_capture_ids=('receipt',))
    counts=m.write_archive_batch(cursor,m.RawArchiveBatch('cycle',100,'code',{},(),(job,),(),()))
    assert counts['raw_complete_cycles']==0
    cycle=next(p for sql,p in cursor.calls if 'INSERT INTO public.stv3_raw_cycle_v1' in sql)
    assert 'field_source_received_after_issue' in cycle[5]


def test_forecast_binds_previous_source_field_not_just_current_used_field():
    m=api(); cursor=Cursor()
    cursor.fetchall=lambda: [('previous-field-id','previous-fingerprint')]
    batch=m.RawArchiveBatch('cycle',100,'code',
        {'forecast_inputs':{'issue':['previous-fingerprint']}},(),(),(('issue','region'),),())
    m.write_archive_batch(cursor,batch)
    links=[p for sql,p in cursor.calls if 'INSERT INTO public.stv3_raw_issue_field_v1' in sql]
    assert links == [('issue','cycle','region','previous-field-id')]


def test_missing_forecast_input_is_explicit_incomplete_replay():
    m=api(); cursor=Cursor(); cursor.fetchall=lambda: []
    batch=m.RawArchiveBatch('cycle',100,'code',
        {'forecast_inputs':{'issue':['missing-fingerprint']}},(),(),(('issue','region'),),())
    counts=m.write_archive_batch(cursor,batch)
    assert counts['raw_complete_cycles'] == 0
    cycle=next(p for sql,p in cursor.calls if 'INSERT INTO public.stv3_raw_cycle_v1' in sql)
    assert 'forecast_input_missing_or_unavailable' in cycle[5]
