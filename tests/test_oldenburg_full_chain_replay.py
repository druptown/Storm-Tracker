"""Pin the offline I/O boundary, not replacements for weather algorithms."""
import hashlib
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).parents[1]/'scripts'))
from oldenburg_full_chain_replay import (ReceiptArchive, RecordedFetcher, decode_provider,
    RecordedIngress, CompletionBarrierCoordinator, document, plain)
from storm_tracker_worker.desired_state import Target
from storm_tracker_worker.thin_provider_catalog import ThinProviderRequest


def archive(tmp_path, payloads):
    captures = []; refs = {}
    for index, (provider, path, body, received) in enumerate(payloads):
        digest = hashlib.sha256(body).hexdigest()
        (tmp_path/digest).write_bytes(body)
        refs[digest] = {'file':digest,'sha256':digest,'bytes':len(body)}
        captures.append({'capture_id':str(index),'provider_id':provider,
            'sha256':digest,'received_at':received,'requested_at':received-1,
            'context':{'source':{'path':path,'host':'example.test'},'request_id':'recorded'},
            'disposition':'received','product_id':'test'})
    return ReceiptArchive(tmp_path, {'captures':captures,'payloads':refs})


def test_future_receipt_is_never_supplied_to_historical_decoder(tmp_path):
    a = archive(tmp_path, [('knmi_radar','/file.h5',b'native',101.)])
    with pytest.raises(ValueError, match='future_receipt'):
        a.body('0',100.)


def test_receipt_payload_tampering_fails_before_decode(tmp_path):
    a = archive(tmp_path, [('knmi_radar','/file.h5',b'native',90.)])
    path = tmp_path/a.captures['0']['sha256']; path.write_bytes(b'changed')
    with pytest.raises(ValueError, match='payload_hash'):
        a.body('0',100.)


def test_frozen_censored_passage_serializes_without_inventing_end():
    from types import MappingProxyType
    from storm_tracker_worker.thin_footprint_forecast import TargetPassage
    p = TargetPassage('test_tracker',100.,'cell','knmi_radar',120.,None,
        'labelled_qpe',1.,MappingProxyType({'type':'Polygon','coordinates':()}),
        right_censored=True)
    out = json.loads(json.dumps(plain(p),allow_nan=False))
    assert out['exit_at'] is None and out['right_censored']
    assert out['footprint']['coordinates'] == []


@pytest.mark.asyncio
async def test_unknown_url_does_not_fall_back_to_another_record_or_network(tmp_path):
    a = archive(tmp_path, [('knmi_radar','/recorded.h5',b'native',90.)])
    fetcher = RecordedFetcher(a, ['0'],100.)
    with pytest.raises(ValueError, match='archived_response_missing'):
        await fetcher.get_bytes('https://example.test/not-recorded.h5')
    assert await fetcher.get_bytes('https://example.test/recorded.h5') == b'native'


@pytest.mark.asyncio
async def test_all_archived_metar_responses_reach_real_normalizer(tmp_path):
    body = lambda station, lat, lon, temp: json.dumps([{
        'icaoId':station,'lat':lat,'lon':lon,'obsTime':80.,'temp':temp}]).encode()
    a = archive(tmp_path, [
        ('metar','/api/data/metar',body('EDXX',53.14,8.21,11.),90.),
        ('metar','/api/data/metar',body('EBXX',51.05,4.42,17.),91.)])
    targets = (Target(target_id='test_tracker',kind='test',display_name='Fictieve',
        latitude=53.1435,longitude=8.2146,location_accuracy_m=None,
        position_observed_at=80.,observation_radius_km=250.,available=True,country_code=None),
        Target(target_id='home',kind='home',display_name='Thuis',latitude=51.0498,
        longitude=4.4186,location_accuracy_m=None,position_observed_at=80.,
        observation_radius_km=250.,available=True,country_code='BE'))
    request = ThinProviderRequest('metar','ground_observation',targets,89.)
    result = await decode_provider(a, request, ['0','1'],100.)
    observations = result.observations
    assert {(o.target_id,o.value,o.metadata['station_id']) for o in observations} == {
        ('test_tracker',11.,'EDXX'),('home',17.,'EBXX')}


@pytest.mark.asyncio
async def test_native_receipt_runs_actual_fusion_motion_tracking_nowcast_and_snapshot(tmp_path):
    from datetime import datetime, timezone
    from test_thin_provider_adapters import _knmi_rtcor_hdf
    from storm_tracker_worker.thin_operational_runtime import ThinOperationalRuntime
    from storm_tracker_worker.thin_region_planner import ThinRegionPlanner
    at = datetime(2026,10,8,0,0,tzinfo=timezone.utc).timestamp()
    a = archive(tmp_path,[('knmi_radar','/radar.h5',
        _knmi_rtcor_hdf(datetime.fromtimestamp(at-300,timezone.utc)),at-1)])
    a.captures['0']['context']['target_positions'] = [['home',51.05,4.42,at-10]]
    policy = {'targets':[{'target_id':'home','kind':'home','display_name':'Thuis',
        'available':True,'country_code':'BE','latitude':51.05,'longitude':4.42,
        'position_observed_at':at-10,'location_accuracy_m':10.,'observation_radius_km':50.}],
        'settings':{'ground_radius_km':20.,'region_sharing_distance_km':150.,
            'model_guidance_enabled':True,'lightning_source_mode':'auto'},
        'provider_inputs':{'knmi_radar':['0']}}
    ingress = RecordedIngress(a); coordinator = CompletionBarrierCoordinator()
    runtime = ThinOperationalRuntime(provider_runtime=ingress,clock=lambda:at,
        regional_radar_coordinator=coordinator)
    try:
        runtime.apply_desired_state(document(policy,at,1,None))
        ingress.cycle = {'evaluated_at':datetime.fromtimestamp(at,timezone.utc).isoformat(),'policy':policy}
        ingress.plan = ThinRegionPlanner().plan(runtime._state,now=at)
        result = await runtime.run_cycle()
        # 100 PV * .01 mm/PV per five minutes = 12 mm/h.
        assert result.fusion.targets['home'].precipitation_rate_mm_h == pytest.approx(12.)
        assert coordinator.source_advances == 1
        assert not ingress.failures and not coordinator.fusion_errors
        assert result.regional_radar
        assert result.nowcast.target_results['home'].as_dict()['radar_field_forecast'] is not None
        assert result.snapshot['revision'] >= 1
        assert runtime.geojson_collection()['features']
    finally:
        runtime.close()
