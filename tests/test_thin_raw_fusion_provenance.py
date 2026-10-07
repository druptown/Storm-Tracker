from dataclasses import replace
import threading
import time
from test_thin_regional_radar_runtime import _plan, _providers
from storm_tracker_worker.thin_regional_radar_runtime import ThinRegionalRadarFusionCoordinator
from storm_tracker_worker.thin_regional_radar_fusion import fuse_regional_radar_fields


def test_completed_job_binds_original_receipts_not_newer_provider_result():
    entered, release = threading.Event(), threading.Event()
    old = _providers()
    old['radar'] = replace(old['radar'], raw_capture_ids=('receipt-old',))
    new = {'radar': replace(old['radar'], raw_capture_ids=('receipt-new',),
                          radar_fields=(replace(old['radar'].radar_fields[0], observed_at=1001,identity=None),))}
    def fuse(region, providers, *, evaluated_at):
        entered.set()
        assert release.wait(2)
        return fuse_regional_radar_fields(region,providers,evaluated_at=evaluated_at)
    coordinator = ThinRegionalRadarFusionCoordinator(fuse=fuse)
    try:
        coordinator.submit(_plan(),old,evaluated_at=1000)
        assert entered.wait(1)
        coordinator.submit(_plan(),new,evaluated_at=1001)
        release.set()
        deadline = time.monotonic()+2
        result = {}
        while not result and time.monotonic()<deadline:
            result = coordinator.poll()
            if not result: time.sleep(.01)
        inputs = result['region:home'].archive_inputs
        assert inputs[0][1] == ('receipt-old',)
        assert inputs[0][0].observed_at == 1000
    finally:
        release.set()
        coordinator.close()
