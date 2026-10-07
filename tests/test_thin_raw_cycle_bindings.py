from dataclasses import replace
import pytest
from storm_tracker_worker.thin_raw_archive import RawArchiveBuffer
from storm_tracker_worker.thin_operational_runtime import ThinOperationalRuntime
from storm_tracker_worker.thin_postgis import PostGISWriteResult
from test_thin_operational_runtime import EmptyProviders,_state,_NativeRadarProviders,_home_state


@pytest.mark.asyncio
async def test_empty_cycle_keeps_original_position_policy_and_archive_status():
    class Writer:
        def __init__(self): self.batch=None
        async def write_cycle(self,*args,archive_batch=None,**kwargs):
            self.batch=archive_batch
            return PostGISWriteResult('current',None,{},0,0)
    archive=RawArchiveBuffer();providers=EmptyProviders()
    providers.archive=archive;providers.code_revision='actual-code-digest'
    writer=Writer();runtime=ThinOperationalRuntime(provider_runtime=providers,postgis_writer=writer,clock=lambda:100)
    runtime.apply_desired_state(_state())
    result=await runtime.run_cycle()
    assert writer.batch is not None
    assert writer.batch.code_revision=='actual-code-digest'
    assert writer.batch.policy['targets'][0]['latitude']==50.85
    assert writer.batch.policy['targets'][0]['longitude']==4.35
    assert result.raw_archive_status['current_cycle_id']==writer.batch.cycle_id
    assert result.raw_archive_status['queued_bytes']==0


@pytest.mark.asyncio
async def test_degraded_archive_keeps_weather_snapshot():
    class Writer:
        async def write_cycle(self,*args,archive_batch=None,**kwargs):
            return PostGISWriteResult('degraded','postgis_unavailable',{},1,0)
    archive=RawArchiveBuffer();archive.mark_gap('capture_missing')
    providers=EmptyProviders();providers.archive=archive;providers.code_revision='code'
    runtime=ThinOperationalRuntime(provider_runtime=providers,postgis_writer=Writer(),clock=lambda:100)
    runtime.apply_desired_state(_state())
    result=await runtime.run_cycle()
    assert result.snapshot['snapshot_revision'] >= 1 if 'snapshot_revision' in result.snapshot else bool(result.snapshot)
    assert result.raw_archive_status['status']=='degraded'
    assert 'capture_missing' in result.raw_archive_status['reason_codes']
    assert 'payload' not in result.raw_archive_status


@pytest.mark.asyncio
async def test_field_overflow_does_not_erase_weather_snapshot():
    class Writer:
        async def write_cycle(self,*args,archive_batch=None,**kwargs):
            return PostGISWriteResult('degraded','raw_archive_incomplete',{},0,0)
    providers = _NativeRadarProviders()
    providers.archive = RawArchiveBuffer(maximum_bytes=1)
    providers.code_revision = 'code'
    runtime = ThinOperationalRuntime(provider_runtime=providers,postgis_writer=Writer(),clock=lambda:100)
    runtime.apply_desired_state(_home_state())
    try:
        result = await runtime.run_cycle()
        assert result.nowcast.storm_cells
        assert 'raw_field_budget_exceeded' in result.raw_archive_status['reason_codes']
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_visual_source_pixels_are_archived_without_quantitative_claim():
    import numpy as np
    from storm_tracker_worker.thin_provider_adapters import ThinVisualRadarField
    from storm_tracker_worker.thin_provider_runtime import ProviderResult
    class Providers(EmptyProviders):
        async def fetch(self,requests):
            field=ThinVisualRadarField('rainviewer','visual',100,4.,52.,.01,.01,np.ones((200,200),dtype='u1'))
            return {'rainviewer':ProviderResult('rainviewer',True,'empty',None,(),100,100,visual_radar_fields=(field,),raw_capture_ids=('original',))}
    class Writer:
        async def write_cycle(self,*args,archive_batch=None,**kwargs):
            self.batch=archive_batch
            return PostGISWriteResult('current',None,{},0,0)
    providers=Providers();providers.archive=RawArchiveBuffer();providers.code_revision='code'
    writer=Writer();runtime=ThinOperationalRuntime(provider_runtime=providers,postgis_writer=writer,clock=lambda:100)
    runtime.apply_desired_state(_home_state())
    try:
        await runtime.run_cycle()
        visual=[job for job in writer.batch.fields if job.role=='visual_input']
        assert visual
        assert visual[0].source_capture_ids==('original',)
        assert visual[0].field.provider_id=='rainviewer'
    finally:
        runtime.close()
