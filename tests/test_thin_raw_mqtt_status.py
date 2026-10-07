import asyncio
import pytest
from test_thin_mqtt_service import FakeMqtt
from test_thin_operational_runtime import EmptyProviders, _state
from storm_tracker_worker.thin_mqtt_service import ThinMqttService
from storm_tracker_worker.thin_operational_runtime import ThinOperationalRuntime
from storm_tracker_worker.thin_raw_archive import RawArchiveBuffer
from storm_tracker_worker.thin_postgis import PostGISWriteResult


@pytest.mark.asyncio
async def test_raw_archive_status_has_separate_bounded_retained_topic():
    class Writer:
        async def write_cycle(self,*args,**kwargs):
            return PostGISWriteResult('current',None,{},0,0)
    providers = EmptyProviders()
    providers.archive = RawArchiveBuffer()
    providers.code_revision = 'code'
    runtime = ThinOperationalRuntime(provider_runtime=providers,postgis_writer=Writer(),clock=lambda:100)
    runtime.apply_desired_state(_state())
    mqtt = FakeMqtt([])
    service = ThinMqttService(client=mqtt,runtime=runtime,cycle_seconds=60)
    try:
        await service.start()
        for _ in range(100):
            if any(t.endswith('/operational/snapshot') for t,_,_,_ in mqtt.published): break
            await asyncio.sleep(.01)
        published = [(v,q,r) for t,v,q,r in mqtt.published if t.endswith('/operational/raw_archive_status')]
        assert published
        value,qos,retain = published[-1]
        assert qos == 1 and retain is True
        assert value['schema_version'] == 1
        assert 'payload' not in value
        snapshot = next(v for t,v,_,_ in mqtt.published if t.endswith('/operational/snapshot'))
        assert 'raw_archive_status' not in snapshot
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_worker_stop_with_uncommitted_receipts_cannot_leave_raw_status_current():
    from storm_tracker_worker.thin_raw_archive import RawCaptureContext,capture_scope,capture_received
    providers=EmptyProviders()
    providers.archive=RawArchiveBuffer()
    context=RawCaptureContext('request','netatmo','weather',10,0,(),(),'code')
    with capture_scope(context,providers.archive):
        capture_received('https://api.netatmo.com/api/getpublicdata',b'{"body":[]}',received_at=12)
    runtime=ThinOperationalRuntime(provider_runtime=providers,clock=lambda:100)
    mqtt=FakeMqtt([])
    service=ThinMqttService(client=mqtt,runtime=runtime)
    await service.start()
    await service.stop()
    values=[v for t,v,_,_ in mqtt.published if t.endswith('/operational/raw_archive_status')]
    assert values
    assert values[-1]['status']=='degraded'
    assert 'worker_stop_unconfirmed' in values[-1]['reason_codes']
    assert providers.archive.pending()


@pytest.mark.asyncio
async def test_worker_stop_with_only_queued_field_cycle_reports_degraded():
    class Writer:
        def diagnostics(self): return {'queued_cycles':1}
    providers=EmptyProviders();providers.archive=RawArchiveBuffer()
    runtime=ThinOperationalRuntime(provider_runtime=providers,postgis_writer=Writer(),clock=lambda:100)
    mqtt=FakeMqtt([]);service=ThinMqttService(client=mqtt,runtime=runtime)
    await service.stop()
    values=[v for t,v,_,_ in mqtt.published if t.endswith('/operational/raw_archive_status')]
    assert values and values[-1]['status']=='degraded'
