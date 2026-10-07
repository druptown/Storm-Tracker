import importlib
import pytest
from storm_tracker_worker.thin_raw_archive import RawArchiveBuffer,RawCaptureContext,capture_scope


@pytest.mark.parametrize('module,function,provider,url,kwargs',[
 ('satellite_radar','_bounded_url','goes19_rrqpe','https://noaa-goes19.s3.amazonaws.com/ABI-L2-RRQPEF/test.nc',{'maximum':100}),
 ('provider_radar','_download_with_headers','goes19_glm','https://noaa-goes19.s3.amazonaws.com/GLM-L2-LCFA/test.nc',{'maximum_bytes':100}),
 ('lightning','_bounded_request','eumetsat_li','https://api.eumetsat.int/data/search-products/1.0.0/os',{'maximum_bytes':100}),
])
def test_sync_native_response_saved_byte_exact(module,function,provider,url,kwargs,monkeypatch):
    m=importlib.import_module('storm_tracker_worker.'+module)
    body=b'weather-test-bytes'
    class Response:
        headers={};status=200
        def __enter__(self): return self
        def __exit__(self,*args): return False
        def read(self,size): return body
        def geturl(self): return url
    monkeypatch.setattr(m,'urlopen',lambda *a,**k:Response())
    archive=RawArchiveBuffer()
    context=RawCaptureContext('request',provider,'native',10,0,('Groningen',),(),'code')
    with capture_scope(context,archive):
        result=getattr(m,function)(url,**kwargs)
    assert result==body
    assert len(archive.pending())==1
    assert archive.pending()[0].payload==body
    assert archive.pending()[0].context.region_revision_ids==('Groningen',)


def test_hsaf_ftp_bytes_survive_decoder_rejection(monkeypatch):
    m=importlib.import_module('storm_tracker_worker.satellite_radar')
    from types import SimpleNamespace
    body = b'compressed-weather-invalid'
    name = '/h40B/h40_cur_mon_data/h40_20261007_2200.gz'
    class FTP:
        def __init__(self,*args,**kwargs): pass
        def __enter__(self): return self
        def __exit__(self,*args): pass
        def login(self,*args): pass
        def set_pasv(self,*args): pass
        def nlst(self,*args): return [name]
        def retrbinary(self,command,receive,**kwargs): receive(body)
    monkeypatch.setattr(m,'FTP',FTP)
    monkeypatch.setenv('STV3_HSAF_USERNAME','private-user')
    monkeypatch.setenv('STV3_HSAF_PASSWORD','private-password')
    monkeypatch.setattr(m,'_timestamp_from_hsaf_name',lambda name:100)
    monkeypatch.setattr(m,'_hsaf_parse',lambda *args: (_ for _ in ()).throw(ValueError('decoder')))
    monkeypatch.setattr(m,'_HSAF_CACHE',{})
    archive=RawArchiveBuffer()
    context=RawCaptureContext('request','hsaf_h40b','h40b',10,0,('Groningen',),(),'code')
    job=SimpleNamespace(payload={'areas':[{'center_lat':53.2194,'center_lon':6.5665,'horizon_km':200}]},job_id='test')
    with capture_scope(context,archive),pytest.raises(ValueError,match='decoder'):
        m.hsaf_h40b_fetch(job)
    assert archive.pending()[0].payload == body
    assert archive.pending()[0].transport == 'ftp'
    assert 'private-password' not in str(archive.pending()[0].safe_source)


def test_blitzortung_capture_precedes_event_decoder():
    from types import SimpleNamespace
    from storm_tracker_worker.thin_blitzortung import ThinBlitzortungFeed
    from storm_tracker_worker.thin_provider_adapters import BlitzortungAdapter
    archive=RawArchiveBuffer()
    context=RawCaptureContext('request','blitzortung','event',10,0,('Groningen',),(),'code')
    adapter=BlitzortungAdapter()
    feed=ThinBlitzortungFeed(client=object(),adapter=adapter,clock=lambda:12,
                           archive=archive,context_factory=lambda:context)
    feed._subscribed_topics={'blitzortung/1.1/u/1/2/#'}
    feed._on_message(None,None,SimpleNamespace(topic='blitzortung/1.1/u/1/2/event',payload=b'{"lat":malformed'))
    assert archive.pending()[0].payload == b'{"lat":malformed'
    assert archive.pending()[0].transport == 'mqtt'
    assert archive.pending()[0].received_at == 12


def test_noaa_err_complete_native_body_captured_before_identity_inspection(monkeypatch):
    from storm_tracker_worker import noaa_err_native as module
    url='https://noaa-enterprise-rainrate-pds.s3.amazonaws.com/BLEND/RainRate-Blend-INST/test.nc'
    body=b'native-weather-test'
    class Response:
        status=200
        headers={'Content-Type':'application/octet-stream','Content-Length':str(len(body))}
        def __init__(self): self.done=False
        def __enter__(self): return self
        def __exit__(self,*args): pass
        def geturl(self): return url
        def read(self,size):
            if self.done: return b''
            self.done=True
            return body
    monkeypatch.setattr(module,'_open_without_redirects',lambda *args,**kwargs:Response())
    archive=RawArchiveBuffer()
    context=RawCaptureContext('request','noaa_err','err',10,0,('Groningen',),(),'code')
    with capture_scope(context,archive):
        result=module._fetch_s3(url,expected_path='/BLEND/RainRate-Blend-INST/test.nc',expected_query='',
                               maximum_bytes=100,allowed_content_types=frozenset({'application/octet-stream'}),
                               object_metadata_required=False)
    assert archive.pending()[0].payload == body
    assert archive.pending()[0].received_at == result.received_at


def test_hsaf_cached_product_keeps_first_receipt_without_new_download(monkeypatch):
    from types import SimpleNamespace
    from storm_tracker_worker import satellite_radar as module
    from storm_tracker_worker.thin_raw_archive import current_capture_references
    calls=[]
    class FTP:
        def __init__(self,*args,**kwargs): pass
        def __enter__(self): return self
        def __exit__(self,*args): pass
        def login(self,*args): pass
        def set_pasv(self,*args): pass
        def nlst(self,*args): return ['/h40B/h40_cur_mon_data/test.gz']
        def retrbinary(self,command,receive,**kwargs): calls.append(command); receive(b'native-weather')
    monkeypatch.setattr(module,'FTP',FTP)
    monkeypatch.setenv('STV3_HSAF_USERNAME','private-user')
    monkeypatch.setenv('STV3_HSAF_PASSWORD','private-password')
    monkeypatch.setattr(module,'_timestamp_from_hsaf_name',lambda name:100)
    monkeypatch.setattr(module,'_validate_product_time',lambda *args:None)
    monkeypatch.setattr(module,'write_json_artifact',lambda *args:{})
    monkeypatch.setattr(module,'_hsaf_parse',lambda *args:{'frame_timestamp':100,'observations':[]})
    monkeypatch.setattr(module,'_HSAF_CACHE',{})
    archive=RawArchiveBuffer()
    job=SimpleNamespace(payload={'areas':[{'center_lat':53.2194,'center_lon':6.5665,'horizon_km':200}]},job_id='test')
    first=RawCaptureContext('first','hsaf_h40b','h40b',10,0,('Groningen',),(),'code')
    with capture_scope(first,archive): module.hsaf_h40b_fetch(job)
    original=archive.pending()[0]
    archive.ack((original.capture_id,))
    second=RawCaptureContext('second','hsaf_h40b','h40b',20,0,('Groningen',),(),'code')
    with capture_scope(second,archive):
        module.hsaf_h40b_fetch(job)
        refs=current_capture_references()
    assert refs==(original.capture_id,)
    assert archive.pending()==()
    assert len(calls)==1


def test_goes_cached_product_references_original_receipt(monkeypatch):
    from types import SimpleNamespace
    from storm_tracker_worker import satellite_radar as module
    from storm_tracker_worker.thin_raw_archive import capture_received,current_capture_references
    calls=[]
    def download(url,maximum):
        calls.append(url)
        capture_received(url,b'native-weather',received_at=12)
        return b'native-weather'
    monkeypatch.setattr(module,'_bounded_url',download)
    monkeypatch.setattr(module,'_latest_goes_key',lambda satellite:'ABI-L2-RRQPEF/test.nc')
    monkeypatch.setattr(module,'_validate_product_time',lambda *args:None)
    monkeypatch.setattr(module,'write_json_artifact',lambda *args:{})
    monkeypatch.setattr(module,'_goes_parse',lambda *args:{'frame_timestamp':100,'observations':[], 'overlay':{'runs':[]}})
    monkeypatch.setattr(module,'_GOES_CACHE',{})
    archive=RawArchiveBuffer()
    job=SimpleNamespace(payload={'satellites':[19],'areas':[{'center_lat':25.76,'center_lon':-80.19,'horizon_km':200}]},job_id='test')
    first=RawCaptureContext('first','goes19_rrqpe','rrqpe',10,0,('Miami',),(),'code')
    with capture_scope(first,archive): module.goes_rrqpe_fetch(job)
    original=archive.pending()[0]
    archive.ack((original.capture_id,))
    second=RawCaptureContext('second','goes19_rrqpe','rrqpe',20,0,('Miami',),(),'code')
    with capture_scope(second,archive):
        module.goes_rrqpe_fetch(job)
        assert current_capture_references()==(original.capture_id,)
    assert len(calls)==1
    assert archive.pending()==()


@pytest.mark.asyncio
async def test_blitzortung_consumed_event_references_actual_ingress_receipt():
    from types import SimpleNamespace
    from storm_tracker_worker.thin_blitzortung import ThinBlitzortungFeed
    from storm_tracker_worker.thin_provider_adapters import BlitzortungAdapter
    from storm_tracker_worker.thin_raw_archive import current_capture_references
    archive=RawArchiveBuffer()
    context=RawCaptureContext('ingress','blitzortung','event',10,0,('Groningen',),(),'code')
    adapter=BlitzortungAdapter(); adapter.set_transport_state(True)
    feed=ThinBlitzortungFeed(client=object(),adapter=adapter,clock=lambda:12,archive=archive,context_factory=lambda:context)
    feed._subscribed_topics={'blitzortung/1.1/u/1/2/#'}
    feed._on_message(None,None,SimpleNamespace(topic='blitzortung/1.1/u/1/2/event',payload=b'{"lat":53.2,"lon":6.5}'))
    original=archive.pending()[0]
    archive.ack((original.capture_id,))
    request=SimpleNamespace(provider_id='blitzortung',product_id='event',targets=[SimpleNamespace(target_id='test_tracker',latitude=53.2,longitude=6.5,observation_radius_km=200)])
    with capture_scope(RawCaptureContext('fetch','blitzortung','event',20,0,('Groningen',),(),'code'),archive):
        result=await adapter.fetch(request,None,now=20)
        assert current_capture_references()==(original.capture_id,)
    assert len(result)==1
    assert result[0].received_at==12
