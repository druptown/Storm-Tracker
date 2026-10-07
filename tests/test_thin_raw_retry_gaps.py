import pytest
from test_thin_postgis import _settings,_cycle_and_nowcast
from storm_tracker_worker.thin_postgis import ThinPostGISWriter
from storm_tracker_worker.thin_postgis_raw_archive import RawArchiveBatch


@pytest.mark.asyncio
async def test_archive_retry_overflow_is_recorded_in_surviving_cycle(monkeypatch):
    writer=ThinPostGISWriter(_settings())
    saved=[]
    def fail(batch): raise OSError('offline')
    monkeypatch.setattr(writer,'_write_batch',fail)
    for index in range(9):
        cycle,nowcast=_cycle_and_nowcast(100+index)
        archive=RawArchiveBatch(str(index),102+index,'code',{},(),(),(),())
        await writer.write_cycle(cycle,nowcast,archive_batch=archive)
    assert writer.diagnostics()['dropped_cycles']==1
    assert any('archive_retry_cycle_dropped' in entry[5].gaps for entry in writer._queue)
