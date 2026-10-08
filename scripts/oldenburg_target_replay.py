"""One causal Oldenburg control using deployed field motion + target runtime.

No provider requests, database writes or changes to old exports. Recomputes
motion rather than reusing earlier predictions/vectors. Not a whole scheduler
or multi-provider fusion replay; cores are separately covered by regressions.
"""
import argparse
from collections import Counter
from datetime import datetime
import hashlib
from io import BytesIO
import json
import os
from pathlib import Path
import sys
import time
import zlib

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from storm_tracker_worker.thin_fusion import FusionCycle, FusedTargetObservation, MAX_FUSION_AGE_SECONDS
from storm_tracker_worker.thin_nowcast import ThinNowcastRuntime
from storm_tracker_worker.thin_provider_adapters import ThinRadarField, RadarFieldIdentity
from storm_tracker_worker.thin_regional_radar_fusion import project_radar_field
from storm_tracker_worker.thin_source_radar_forecast import SourceRadarForecastEngine, trace_target, _flow_config
from storm_tracker_worker.thin_raw_code import source_tree_digest
from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid


def causally_available(fields, issued):
    return sorted((f for f in fields if f['available_at'] <= issued
        and f['metadata']['observed_at'] <= issued),
        key=lambda f: (f['available_at'], f['metadata']['observed_at'], f['field_id']))


def classify(predicted, observed, threshold):
    wet = observed > 0 if threshold == 0 else observed >= threshold
    if predicted is None:
        return 'unknown_on_wet' if wet else 'unknown_on_dry'
    predicted_wet = predicted > 0 if threshold == 0 else predicted >= threshold
    return ('hit' if wet else 'false_wet') if predicted_wet else ('miss' if wet else 'correct_dry')


def restore(field, source):
    arrays = {}
    for name, ref in field['arrays'].items():
        raw = (source/ref['file']).read_bytes()
        assert hashlib.sha256(raw).hexdigest() == ref['sha256']
        value = np.load(BytesIO(zlib.decompress(raw)), allow_pickle=False)
        assert hashlib.sha256(value.tobytes()).hexdigest() == ref['descriptor']['sha256']
        arrays[name] = value
    m = field['metadata']; identity = dict(m['identity'])
    identity['source_composition'] = tuple(identity['source_composition'])
    return ThinRadarField(m['provider_id'],m['product_id'],m['observed_at'],m['crs'],
        *m['affine'],arrays['values'],arrays['valid_mask'],RadarFieldIdentity(**identity))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--expected-code', required=True)
    args = parser.parse_args()
    code = source_tree_digest()
    assert code == args.expected_code, 'Worker source differs from deployed artifact'
    # A new directory is a durable one-run guard, not a silent overwrite.
    args.output.mkdir(exist_ok=False)
    started = time.monotonic()
    progress = {'status':'running','pid':os.getpid(),'source_code':code,
        'started_at':datetime.now().astimezone().isoformat(),'processed_issues':0,
        'processed_native_fields':0,'errors':[]}
    def save(name, value):
        (args.output/name).write_text(json.dumps(value,indent=2,allow_nan=False))
    save('progress.json',progress)
    source = args.data/'source-clock-corrected'
    native = json.loads((source/'native-manifest.json').read_text())
    manifest = json.loads((args.data/'export-manifest.json').read_text())
    audit = json.loads((args.data/'event-audit.json').read_text())
    issues = sorted(audit['issues'],key=lambda i:datetime.fromisoformat(i['issued_at']).timestamp())
    fields = [f for f in native['fields'] if f['metadata']['provider_id']=='knmi_radar']
    m = manifest['frames'][0]['metadata']
    shape = manifest['frames'][0]['arrays']['values']['descriptor']['shape']
    grid = AnalysisGrid(m['identity']['grid_id'],m['crs'],*m['affine'],shape[1],shape[0])
    revision = issues[0]['region_revision_id']
    runtime = ThinNowcastRuntime(targets={'test_tracker':(53.1435,8.2146,.01)})
    runtime.update_targets({'test_tracker':(53.1435,8.2146,.01)},
        target_region_ids={'test_tracker':revision})
    engine = SourceRadarForecastEngine()
    policy = {'source_code':code,'worker_version':'1.18.106','provider_control':'knmi_radar',
        'historical_issues':len(issues),'native_fields':len(fields),
        'motion_recomputed':True,'motion_config_fingerprint':_flow_config().fingerprint,
        'forecast_method':'actual SourceRadarForecastEngine + ThinNowcastRuntime.advance',
        'availability':'actual archived linked receipt/cycle availability, not legacy identity clocks',
        'truth':'later KNMI quantitative target pixels; exact target ground onset unknown',
        'limits':['Single-source control, not complete live scheduler/fusion replay',
            'Intensity kernels not replayed here; separate focused regressions preserve them',
            'Two-hour horizon, stationary flow, no growth/decay or calibrated probability',
            'Unknown support and censored ends remain unknown, not dry',
            'Dependent pairs from one event are not an accuracy percentage'],
        'input_hashes':{str(p.relative_to(args.data)):hashlib.sha256(p.read_bytes()).hexdigest()
            for p in (source/'native-manifest.json',args.data/'event-audit.json',args.data/'export-manifest.json')},
        'runner_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    save('policy.json',policy)
    seen = set(); current = None; records = []
    try:
        for issue in issues:
            issued = datetime.fromisoformat(issue['issued_at']).timestamp()
            assert issue['region_revision_id'] == revision
            position = issue['forecast_metadata']['forecast']
            assert (position['target_latitude'],position['target_longitude']) == (53.1435,8.2146)
            for field in causally_available(fields,issued):
                if field['field_id'] in seen:
                    continue
                restored = restore(field,source)
                layer = project_radar_field(restored,grid)
                current = engine.advance(restored,layer,received_at=field['available_at'])
                seen.add(field['field_id']); progress['processed_native_fields'] = len(seen)
                save('progress.json',progress)
            rate = None
            if current is not None and issued-current.field.observed_at <= MAX_FUSION_AGE_SECONDS:
                if current.layer.valid_mask[170,170]:
                    rate = float(current.layer.rain_rate_mm_h[170,170])
            point = FusedTargetObservation('test_tracker','operational' if rate is not None else 'unknown',
                issued,rate,None,None,None,None,None,0,0.,
                ('knmi_radar',) if rate is not None else (),
                {} if current is None else {'knmi_radar':current.field.observed_at})
            result = runtime.advance(FusionCycle(issued,{'test_tracker':point},{},{},()),
                tracked_cells=(),source_motions={revision:() if current is None else (current,)})
            target = result.target_results['test_tracker']
            samples = [] if current is None else trace_target(current,53.1435,8.2146,issued)
            records.append({'original_issue_id':issue['forecast_issue_id'],'issued_at':issued,
                'product_at':None if current is None else current.field.observed_at,
                'forecast':target.as_dict(),'samples':samples})
            progress.update(processed_issues=len(records),elapsed_seconds=time.monotonic()-started)
            save('predictions.json',{'complete':False,'records':records})
            save('progress.json',progress)
            if len(records)%10 == 0:
                print(json.dumps({k:progress[k] for k in ('processed_issues','processed_native_fields','elapsed_seconds')}),flush=True)
        save('predictions.json',{'complete':True,'records':records})
        # Verification data only enters AFTER all predictions are immutable.
        reference = json.loads((source/'metrics.json').read_text())
        pairs = {}
        for threshold in (0.,.1):
            bands = {}
            for lower,upper in ((5,30),(30,60),(60,120)):
                counts = Counter()
                for record in records:
                    for obs in reference['source_timed_observations']:
                        at = obs['metadata']['observed_at']
                        if not obs['target_valid'] or not lower <= (at-record['issued_at'])/60 < upper:
                            continue
                        sample = min(record['samples'],key=lambda s:abs(s['at']-at)) if record['samples'] else None
                        predicted = None if sample is None or abs(sample['at']-at)>30.000001 else sample['rate_mm_h']
                        counts[classify(predicted,obs['target_rate_mm_h'],threshold)] += 1
                bands[f'{lower}-{upper}min'] = dict(counts)
            pairs[str(threshold)] = bands
        metrics = {'issues':len(records),'native_fields':len(seen),'errors':[],
            'elapsed_seconds':time.monotonic()-started,'classification':pairs,
            'forecast_states':dict(Counter(r['forecast']['forecast_state'] for r in records)),
            'support_states':dict(Counter(r['forecast']['radar_field_forecast']['support_state'] for r in records)),
            'observed_policy_events':reference['observed_policy_events'],
            'observed_weak_events':reference['observed_weak_events'],'limits':policy['limits']}
        save('metrics.json',metrics)
        progress.update(status='completed',elapsed_seconds=time.monotonic()-started)
        save('progress.json',progress)
        print(json.dumps({'status':'completed','issues':len(records),'seconds':progress['elapsed_seconds']}),flush=True)
    except Exception as error:
        progress.update(status='failed',errors=[type(error).__name__+': '+str(error)])
        save('progress.json',progress)
        raise


if __name__ == '__main__':
    main()
