"""Full numerical rain chain from native receipts, with no online I/O.

Recorded arrivals replace transport only. Decode, geographic scoping, fusion,
source motion, kernel tracking, target nowcast/holds, validation and snapshot
construction use the current production runtime. A completion barrier replaces
wall-clock thread timing; original request failures and cold-start limits are
reported, never represented as an exact historical scheduler reproduction.
"""
import argparse
import asyncio
from collections import Counter
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from datetime import datetime
import hashlib
import json
import logging
import os
from pathlib import Path
import socket
import sys
import time
from types import SimpleNamespace
from urllib.parse import urlsplit
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from storm_tracker_worker import thin_provider_adapters as adapters
from storm_tracker_worker import thin_satellite_adapters as satellites
from storm_tracker_worker import lightning, satellite_radar
from storm_tracker_worker.desired_state import Target
from storm_tracker_worker.thin_operational_runtime import ThinOperationalRuntime
from storm_tracker_worker.thin_provider_catalog import ThinProviderRequest
from storm_tracker_worker.thin_provider_runtime import ProviderResult
from storm_tracker_worker.thin_region_planner import ThinRegionPlanner
from storm_tracker_worker.thin_regional_radar_runtime import ThinRegionalRadarFusionCoordinator
from storm_tracker_worker.thin_source_radar_forecast import SourceRadarForecastEngine
from storm_tracker_worker.thin_radar_tracking import ThinRadarTracker
from storm_tracker_worker.thin_raw_code import source_tree_digest


def plain(value):
    """Serialize frozen runtime results without deepcopying MappingProxy."""
    if is_dataclass(value):
        return {f.name:plain(getattr(value,f.name)) for f in fields(value)}
    if isinstance(value,Mapping): return {k:plain(v) for k,v in value.items()}
    if isinstance(value,(tuple,list)): return [plain(v) for v in value]
    return value


class ReceiptArchive:
    def __init__(self, root, manifest):
        self.root = Path(root)
        self.captures = {r['capture_id']:r for r in manifest['captures']}
        self.payloads = manifest['payloads']
        self.verified = set()

    def body(self, cid, at):
        receipt = self.captures[cid]
        if receipt['received_at'] > at or receipt['requested_at'] > at:
            raise ValueError('future_receipt')
        ref = self.payloads.get(receipt['sha256'])
        if ref is None or ref.get('missing'):
            raise ValueError('archived_payload_missing')
        body = (self.root/ref['file']).read_bytes()
        if len(body) != ref['bytes'] or hashlib.sha256(body).hexdigest() != receipt['sha256']:
            raise ValueError('payload_hash_mismatch')
        self.verified.add(receipt['sha256'])
        return body


class RecordedFetcher:
    """Exact safe path selection; no opportunistic network fallback."""
    def __init__(self, archive, ids, at):
        self.archive, self.ids, self.at = archive, tuple(ids), at
        self.used = set()

    def matching(self, url, *, json_body=False):
        parsed = urlsplit(url)
        choices = []
        for cid in self.ids:
            r = self.archive.captures[cid]; source = r['context']['source']
            if source.get('host') != parsed.hostname or source.get('path') != parsed.path:
                continue
            body = self.archive.body(cid,self.at)
            if json_body:
                try: json.loads(body)
                except (ValueError,UnicodeDecodeError): continue
            choices.append((r['received_at'],cid,body))
        if not choices:
            raise ValueError('archived_response_missing')
        unused = [c for c in choices if c[1] not in self.used]
        # For shared APPV4 path, its image must not be mistaken for forecast JSON.
        if not json_body and any(c[2].startswith(b'\x89PNG') for c in choices):
            unused = [c for c in choices if c[2].startswith(b'\x89PNG')]
        _,cid,body = min(unused or choices)
        self.used.add(cid)
        return body

    async def get_bytes(self,url,**kwargs):
        body = self.matching(url)
        if len(body) > kwargs.get('maximum_bytes',len(body)):
            raise ValueError('archived_response_too_large')
        return body

    async def get_json(self,url,**kwargs):
        # Station catalog query parameters were deliberately not archived.
        # Supply the union of this recorded request's METAR responses, then
        # leave point normalization and proximity checks to the actual adapter.
        if self.ids and self.archive.captures[self.ids[0]]['provider_id'] == 'metar':
            items = []
            for cid in self.ids:
                value = json.loads(self.archive.body(cid,self.at))
                if not isinstance(value,list): raise ValueError('metar_payload_invalid')
                items.extend(value)
            return items
        return json.loads(self.matching(url,json_body=True))


def native_body(archive, ids, at, suffix):
    candidates = [cid for cid in ids
        if archive.captures[cid]['context']['source']['path'].endswith(suffix)]
    if len(candidates) != 1:
        raise ValueError('native_product_not_unique')
    return archive.body(candidates[0],at)


async def decode_provider(archive, request, ids, at):
    provider = request.provider_id
    receipts = [archive.captures[cid] for cid in ids]
    assert all(r['provider_id'] == provider for r in receipts)
    for cid in ids: archive.body(cid,at)  # Check causality/hash before any decoder.
    received = max(r['received_at'] for r in receipts)
    now = request.requested_at
    fetcher = RecordedFetcher(archive,ids,at)
    if provider == 'knmi_radar':
        decoded = await asyncio.to_thread(adapters._decode_knmi_rtcor,
            native_body(archive,ids,at,'.h5'),request,received_at=now)
    elif provider == 'dwd_radolan':
        decoded = await asyncio.to_thread(adapters._decode_dwd_radolan,
            native_body(archive,ids,at,'.tar'),request,received_at=now)
    elif provider == 'meteofrance_radar':
        decoded = await asyncio.to_thread(adapters._decode_meteofrance_radar,
            archive.body(ids[0],at),request,received_at=now)
    elif provider == 'opera_radar':
        decoded = await asyncio.to_thread(adapters._decode_opera_targets,
            native_body(archive,ids,at,'.h5'),request,received_at=now)
    elif provider in {'open_meteo','netatmo','metar','kmi_aws','kmi_appv4','rainviewer'}:
        factory = {'open_meteo':adapters.OpenMeteoAdapter,
            'netatmo':lambda:adapters.NetatmoAdapter(access_token='offline-placeholder-not-a-credential'),
            'metar':adapters.MetarAdapter,'kmi_aws':adapters.KmiAwsAdapter,
            'kmi_appv4':adapters.KmiRadarAdapter,'rainviewer':adapters.RainViewerRadarAdapter}[provider]
        decoded = await factory().fetch(request,fetcher,now=now)
    elif provider == 'goes19_glm':
        # Native products actually downloaded by this historical request,
        # through the production age-filter/bounded aggregation function.
        keys = sorted(archive.captures[cid]['context']['source']['path'].lstrip('/')
            for cid in ids if archive.captures[cid]['context']['source']['path'].endswith('.nc'))
        class AcquisitionDatetime(datetime):
            @classmethod
            def now(cls,tz=None): return datetime.fromtimestamp(now,tz)
        with patch.object(lightning,'datetime',AcquisitionDatetime), \
                patch.object(lightning,'_recent_keys',return_value=keys), \
                patch.object(lightning,'_download_with_headers',side_effect=lambda url,**kw:fetcher.matching(url)):
            raw = await asyncio.to_thread(lightning.goes_glm_fetch,
                SimpleNamespace(payload={'satellites':[19],'processed_keys':[]}))
        decoded = satellites._lightning_observations(raw['observations'],request,now=now,
            maximum_age_seconds=satellites.GOES_GLM_MAX_AGE_SECONDS,
            expected_source='noaa_goes19_glm')
    elif provider == 'eumetsat_li':
        observations = []
        for cid in ids:
            path = archive.captures[cid]['context']['source']['path']
            if provider == 'eumetsat_li' and '/download/' in path:
                observations.extend(lightning.parse_eumetsat_flashes(archive.body(cid,at)))
            elif provider == 'goes19_glm' and path.endswith('.nc'):
                observations.extend(lightning.parse_goes_flashes(archive.body(cid,at),19))
        expected = 'eumetsat_li' if provider == 'eumetsat_li' else 'noaa_goes19_glm'
        age = satellites.EUMETSAT_MAX_AGE_SECONDS if provider == 'eumetsat_li' else satellites.GOES_GLM_MAX_AGE_SECONDS
        decoded = satellites._lightning_observations(observations,request,now=now,
            maximum_age_seconds=age,expected_source=expected)
    elif provider in {'hsaf_h40b','goes19_rrqpe'}:
        areas = tuple(satellites._areas(request))
        if provider == 'hsaf_h40b':
            cid = next(cid for cid in ids if archive.captures[cid]['context']['source']['path'].endswith('.nc.gz'))
            path = archive.captures[cid]['context']['source']['path']
            timestamp = satellite_radar._timestamp_from_hsaf_name(path)
            raw = await asyncio.to_thread(satellite_radar._hsaf_parse,archive.body(cid,at),areas,timestamp)
            age,expected = satellites.HSAF_H40B_MAX_AGE_SECONDS,'hsaf_h40b'
        else:
            raw = await asyncio.to_thread(satellite_radar._goes_parse,
                native_body(archive,ids,at,'.nc'),19,areas)
            age,expected = satellites.GOES_RRQPE_MAX_AGE_SECONDS,'noaa_goes19_rrqpe'
        decoded = satellites._rain_observations(raw,request,now=now,
            maximum_age_seconds=age,expected_source=expected)
    elif provider == 'noaa_err':
        # The real NOAA reader also selects/validates the complete native
        # product. Patch only its network responses and listing time prefix.
        native = satellites.noaa_err_native
        cid = next(cid for cid in ids if archive.captures[cid]['context']['source']['path'].endswith('.nc'))
        listing = next(cid for cid in ids if archive.captures[cid]['context']['source']['path'] == '/')
        class AcquisitionDatetime(datetime):
            @classmethod
            def now(cls,tz=None): return datetime.fromtimestamp(now,tz)
        with patch.object(satellites,'datetime',AcquisitionDatetime), \
                patch.object(native,'_fetch_listing',return_value=SimpleNamespace(payload=archive.body(listing,at))), \
                patch.object(native,'_fetch_object',return_value=SimpleNamespace(payload=archive.body(cid,at))):
            raw = await asyncio.to_thread(satellites._default_noaa_err_fetch,{'areas':satellites._areas(request)})
        decoded = satellites._rain_observations(raw,request,now=now,
            maximum_age_seconds=satellites.NOAA_ERR_MAX_AGE_SECONDS,expected_source='noaa_err')
    else:
        raise ValueError('recorded_provider_decoder_missing')
    result = decoded if isinstance(decoded,adapters.ThinAdapterResult) else adapters.ThinAdapterResult(tuple(decoded))
    return ProviderResult(provider,True,'operational' if result.observations else 'empty',
        None if result.observations else 'no_observations',result.observations,
        now,received,result.radar_fields,result.visual_radar_fields,tuple(ids))


class CompletionBarrierCoordinator(ThinRegionalRadarFusionCoordinator):
    """Actual two-thread numerical coordinator; deterministic completion gate."""
    def __init__(self):
        self.source_advances = 0
        self.fusion_errors = []
        owner = self
        class CountedSourceEngine(SourceRadarForecastEngine):
            def advance(self,*args,**kwargs):
                owner.source_advances += 1
                return super().advance(*args,**kwargs)
        super().__init__(max_workers=2,source_engine_factory=CountedSourceEngine)

    def submit(self,*args,**kwargs):
        super().submit(*args,**kwargs)
        for job in tuple(self._jobs.values()):
            try: job.future.result()
            except Exception as error:
                self.fusion_errors.append({'at':job.evaluated_at,'error':type(error).__name__})


class RecordedIngress:
    """Deliver historical completed inputs, not cached scientific outputs."""
    def __init__(self,archive):
        self.receipts = archive
        self.cycle = None; self.plan = None
        self.last_ids = {}; self.decoded = Counter(); self.failures = []

    async def fetch(self,requests):
        cycle = self.cycle; at = datetime.fromisoformat(cycle['evaluated_at']).timestamp()
        targets = {t.target_id:t for t in self.plan.targets}
        output = {}
        for provider, ids in cycle['policy']['provider_inputs'].items():
            ids = tuple(sorted(ids))
            if self.last_ids.get(provider) == ids:
                continue
            self.last_ids[provider] = ids
            if not ids:
                output[provider] = ProviderResult(provider,False,'unknown','archive_no_provider_input',(),at,at)
                continue
            receipts = [self.receipts.captures[cid] for cid in ids]
            request_ids = {r['context']['request_id'] for r in receipts}
            if len(request_ids) != 1:
                raise ValueError('provider_inputs_span_multiple_requests')
            ordered = [targets[position[0]] for position in receipts[0]['context']['target_positions']]
            request = ThinProviderRequest(provider,receipts[0]['product_id'],tuple(ordered),
                receipts[0]['requested_at'],self.plan.regions,
                cycle['policy']['settings']['ground_radius_km'])
            try:
                # Restore only the implicit acquisition clock of native
                # satellite readers; weather calculations remain unchanged.
                with patch.object(satellite_radar,'time',SimpleNamespace(time=lambda:request.requested_at)), \
                        patch.object(lightning,'time',SimpleNamespace(time=lambda:request.requested_at)):
                    result = await decode_provider(self.receipts,request,ids,at)
                self.decoded[provider] += 1
            except Exception as error:
                self.failures.append({'at':at,'provider':provider,'error':type(error).__name__,
                    'reason':str(error)[:100] if isinstance(error,ValueError) else None})
                result = ProviderResult(provider,True,'error',type(error).__name__,(),
                    request.requested_at,at,raw_capture_ids=ids)
            output[provider] = result
        return output


def document(policy,at,generation,previous):
    return {'contract':'stv3-worker-desired-state-v1','producer_id':'offline-oldenburg107',
        'authority_epoch':1,'generation':generation,'issued_at':at,
        'previous_state_sha256':previous,'targets':policy['targets'],'settings':policy['settings']}


async def simulate(data,output,expected_code,limit=None):
    code = source_tree_digest()
    assert code == expected_code, 'Not the deployed107 worker source'
    output.mkdir(exist_ok=False)
    audit_path = data/'event-audit.json'; inputs_path = data/'full-chain107-inputs/manifest.json'
    audit = json.loads(audit_path.read_text()); inputs = json.loads(inputs_path.read_text())
    assert hashlib.sha256(audit_path.read_bytes()).hexdigest() == inputs['audit_sha256']
    archive = ReceiptArchive(data,inputs); ingress = RecordedIngress(archive)
    cycles = sorted(audit['cycles'],key=lambda c:datetime.fromisoformat(c['evaluated_at']).timestamp())
    if limit is not None: cycles = cycles[:limit]
    clock = [datetime.fromisoformat(cycles[0]['evaluated_at']).timestamp()]
    coordinator = CompletionBarrierCoordinator()
    runtime = ThinOperationalRuntime(provider_runtime=ingress,clock=lambda:clock[0],
        regional_radar_coordinator=coordinator)
    tracking_calls = []
    class AuditedTracker(ThinRadarTracker):
        def advance(self,field,target):
            transition = super().advance(field,target)
            tracking_calls.append({'provider':field.provider_id,'product_at':field.observed_at,
                'scope':target.target_id,'status':getattr(transition,'status','duplicate'),
                'detections':None if transition is None else len(transition.detection_frame.detections),
                'candidate_edges':None if transition is None else transition.candidate_edge_count,
                'reason_codes':[] if transition is None else list(transition.reason_codes)})
            return transition
    runtime._radar_tracker = AuditedTracker()
    progress = {'status':'running','pid':os.getpid(),'code':code,'total':len(cycles),
        'processed':0,'started_at':datetime.now().astimezone().isoformat()}
    def save(name,value):
        (output/name).write_text(json.dumps(value,indent=2,allow_nan=False))
    policy = {'code':code,'worker_version':'1.18.107','stages':[
        'native receipt hashes/causal availability','production provider decoders',
        'ThinOperationalRuntime geographic scoping/fusion','production regional raster projection/fusion',
        'SourceRadarForecastEngine motion recalculated','ThinRadarTracker kernels recalculated',
        'ThinNowcastRuntime episodes/intensity/holds','GroundValidationRuntime','operational snapshot/map'],
        'transport':'recorded historical completed-input deliveries; no provider HTTP/MQTT',
        'completion_policy':'barrier after real two-thread coordinator submit, before runtime poll',
        'start_state':'cold at first saved cycle; no invented prior tracker/cache state',
        'limits':['Numerical full rain chain, not exact live network/thread scheduling reproduction',
            'Catalog-selected station query parameters were not archived; recorded METAR responses unioned',
            'Ground target truth unknown; later KNMI reference is radar, not ground',
            'Historical capture gaps/errors retained; no fabricated successful input',
            'Two-hour horizon; one event; no calibrated probability'],
        'input_hashes':{str(p.relative_to(data)):hashlib.sha256(p.read_bytes()).hexdigest()
            for p in (audit_path,inputs_path)},
        'runner_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    save('policy.json',policy); save('progress.json',progress)
    records = []; previous = None; started = time.monotonic()
    def deny_network(*args,**kwargs): raise RuntimeError('offline_network_forbidden')
    try:
        with patch.object(socket.socket,'connect',deny_network):
            for index,cycle in enumerate(cycles):
                clock[0] = datetime.fromisoformat(cycle['evaluated_at']).timestamp()
                applied = runtime.apply_desired_state(document(cycle['policy'],clock[0],index+1,previous))
                previous = applied['state_sha256']
                ingress.cycle = cycle
                ingress.plan = ThinRegionPlanner().plan(runtime._state,now=clock[0])
                tracking_calls.clear()
                result = await runtime.run_cycle()
                forecast = result.nowcast.target_results['test_tracker'].as_dict()
                assert (forecast['target_latitude'],forecast['target_longitude']) == (53.1435,8.2146)
                records.append({'original_cycle_id':cycle['cycle_id'],'issued_at':clock[0],
                    'historical_complete':cycle['complete'],'historical_gaps':cycle['gaps'],
                    'forecast':forecast,'all_target_forecasts':{k:v.as_dict() for k,v in result.nowcast.target_results.items()},
                    'passages':[plain(p) for p in result.nowcast.target_passages if p.target_id == 'test_tracker'],
                    'ground_validations':[plain(v) for v in result.validations if v.target_id == 'test_tracker'],
                    'regional_fusion':[s.as_dict() for s in result.regional_radar],
                    'snapshot_revision':result.snapshot['revision'],
                    'snapshot':dict(result.snapshot),
                    'current_basis':result.fusion.targets['test_tracker'].as_dict(),
                    'tracked_cells':len(result.nowcast.storm_cells),
                    'tracking_calls':list(tracking_calls),
                    'provider_observations':len(result.fusion.observations),
                    'map_features':len(runtime.geojson_collection()['features']),
                    'decoder_failures':list(ingress.failures)})
                progress.update(processed=len(records),decoded=dict(ingress.decoded),
                    source_advances=coordinator.source_advances,elapsed_seconds=time.monotonic()-started,
                    decoder_failures=len(ingress.failures),fusion_errors=coordinator.fusion_errors)
                save('predictions.json',{'complete':False,'records':records}); save('progress.json',progress)
                print(json.dumps({k:progress[k] for k in ('processed','total','elapsed_seconds','decoder_failures')}),flush=True)
        save('predictions.json',{'complete':True,'records':records})
        progress.update(status='completed',payload_hashes_verified=len(archive.verified),
            elapsed_seconds=time.monotonic()-started)
        save('progress.json',progress)
    except Exception as error:
        progress.update(status='failed',error=type(error).__name__+': '+str(error))
        save('progress.json',progress)
        raise
    finally:
        runtime.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--expected-code',required=True)
    parser.add_argument('--limit',type=int)
    args = parser.parse_args()
    logging.basicConfig(level=logging.WARNING)
    asyncio.run(simulate(args.data,args.output,args.expected_code,args.limit))


if __name__ == '__main__': main()
