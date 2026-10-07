import asyncio
import json
import pytest
from storm_tracker_worker.thin_provider_adapters import AioHttpFetcher
from storm_tracker_worker.thin_provider_runtime import ThinProviderRuntime
from storm_tracker_worker.thin_provider_catalog import ThinProviderRequest
from storm_tracker_worker.thin_raw_archive import RawArchiveBuffer,RawCaptureContext,capture_scope


class Content:
    def __init__(self,body): self.body=body
    async def iter_chunked(self,size): yield self.body


class Response:
    status=200
    def __init__(self,url,body):
        self.url=url;self.content=Content(body);self.headers={'Content-Type':'application/json'}
    async def __aenter__(self): return self
    async def __aexit__(self,*args): return False


class Session:
    def __init__(self,body): self.body=body;self.count=0
    def get(self,url,**kwargs):
        self.count+=1;return Response(url,self.body)
    post=get


@pytest.mark.asyncio
async def test_json_capture_precedes_decoder_failure_and_wrapper_captures_once():
    archive=RawArchiveBuffer();session=Session(b'{"body":')
    fetcher=AioHttpFetcher(session=session)
    context=RawCaptureContext('request','netatmo','weather',10,0,('Groningen',),(),'code')
    with capture_scope(context,archive):
        with pytest.raises(json.JSONDecodeError):
            await fetcher.get_json('https://api.netatmo.com/api/getpublicdata')
    assert len(archive.pending())==1
    assert archive.pending()[0].payload==b'{"body":'
    assert session.count==1


@pytest.mark.asyncio
async def test_provider_attempts_attach_receipts_even_after_decode_failure():
    archive=RawArchiveBuffer()
    class Adapter:
        async def fetch(self,request,fetcher,*,now):
            return await fetcher.get_json('https://api.netatmo.com/api/getpublicdata')
    runtime=ThinProviderRuntime(adapters={'netatmo':Adapter()},fetcher=AioHttpFetcher(session=Session(b'{"body":')),
        retries=1,retry_delay_seconds=0,archive=archive,code_revision='code')
    result=(await runtime.fetch((ThinProviderRequest('netatmo','weather',(),10),)))['netatmo']
    assert result.status=='error'
    assert len(result.raw_capture_ids)==2
    assert [r.context.attempt for r in archive.pending()]==[0,1]


@pytest.mark.asyncio
async def test_oauth_post_never_captures_tokens():
    archive=RawArchiveBuffer();fetcher=AioHttpFetcher(session=Session(b'{"access_token":"not_saved"}'))
    context=RawCaptureContext('request','netatmo','weather',10,0,(),(),'code')
    with capture_scope(context,archive):
        await fetcher.post_json('https://api.netatmo.com/oauth2/token')
    assert archive.pending()==()
    assert archive.status()['excluded_count']==1


@pytest.mark.asyncio
async def test_failed_transport_without_received_input_records_gap():
    archive=RawArchiveBuffer()
    class Adapter:
        async def fetch(self,*args,**kwargs): raise OSError('private remote error')
    runtime=ThinProviderRuntime(adapters={'netatmo':Adapter()},fetcher=None,retries=0,archive=archive,code_revision='code')
    result=await runtime._one(ThinProviderRequest('netatmo','weather',(),20))
    assert result.status=='error'
    assert 'provider_input_not_received' in archive.status()['reason_codes']


@pytest.mark.asyncio
async def test_cancel_after_received_body_retains_original_capture():
    from storm_tracker_worker.thin_raw_archive import capture_received
    archive=RawArchiveBuffer();received=asyncio.Event()
    class Adapter:
        async def fetch(self,*args,**kwargs):
            capture_received('https://api.netatmo.com/api/getpublicdata',b'{"body":[]}',received_at=12)
            received.set()
            await asyncio.Event().wait()
    runtime=ThinProviderRuntime(adapters={'netatmo':Adapter()},fetcher=None,retries=0,archive=archive,code_revision='code')
    task=asyncio.create_task(runtime._one(ThinProviderRequest('netatmo','weather',(),10)))
    await received.wait();task.cancel()
    with pytest.raises(asyncio.CancelledError): await task
    assert archive.pending()[0].received_at==12
    assert archive.pending()[0].payload==b'{"body":[]}'


@pytest.mark.asyncio
async def test_http_error_has_safe_gap_but_never_archives_error_body():
    class LimitedSession(Session):
        def get(self,url,**kwargs):
            response=Response(url,b'{"private":"error-body"}')
            response.status=429
            return response
    archive=RawArchiveBuffer()
    context=RawCaptureContext('request','netatmo','weather',10,0,(),(),'code')
    with capture_scope(context,archive),pytest.raises(Exception):
        await AioHttpFetcher(session=LimitedSession(b'')).get_json('https://api.netatmo.com/api/getpublicdata')
    assert archive.pending()==()
    assert 'http_status_429' in archive.status()['reason_codes']
    assert 'error-body' not in repr(archive.status())


@pytest.mark.asyncio
async def test_cache_refers_to_original_receipt_after_durable_ack_without_new_receive():
    from storm_tracker_worker import thin_raw_archive as module
    archive=RawArchiveBuffer(); context=RawCaptureContext('original','netatmo','weather',10,0,(),(),'code')
    with capture_scope(context,archive):
        original=module.capture_received('https://api.netatmo.com/api/getpublicdata',b'{"body":[]}',received_at=12)
    archive.ack((original,))
    class Adapter:
        async def fetch(self,request,fetcher,*,now):
            module.reference_captures((original,))
            return ()
    runtime=ThinProviderRuntime(adapters={'netatmo':Adapter()},fetcher=None,archive=archive,code_revision='code')
    result=await runtime._one(ThinProviderRequest('netatmo','weather',(),20))
    assert result.raw_capture_ids==(original,)
    assert archive.pending()==()
