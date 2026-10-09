"""Offline source-consistent prediction; never imported by production."""
import numpy as np
from oldenburg_field_probe import local_motion, reverse_target_series


def restore_motion_config(values):
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from storm_tracker_worker.tracking_core.engine.pyramidal_sparse_flow import SparsePyramidalConfig
    copied = dict(values)
    copied['velocity_process_noise_covariance_m2_s2'] = tuple(
        tuple(row) for row in copied['velocity_process_noise_covariance_m2_s2'])
    return SparsePyramidalConfig(**copied)


def compatibility(field):
    m = field['metadata']
    return (m['provider_id'], m['product_id'], m['unit'],
            m['identity']['quantity'], m['identity']['processing_version'],
            m.get('crs'), tuple(m.get('affine', ())))


def provider_pair(fields, provider, issued_at, maximum_gap=1200.):
    available = [f for f in fields if f['metadata']['provider_id'] == provider
                 and f['available_at'] <= issued_at
                 and f['metadata']['observed_at'] <= issued_at]
    # Repeated products do not create a zero-second optical-flow interval.
    unique = {}
    for f in sorted(available, key=lambda f: (f['available_at'], f['field_id'])):
        unique.setdefault(f['metadata']['observed_at'], f)
    ordered = sorted(unique.values(), key=lambda f: f['metadata']['observed_at'])
    if len(ordered) < 2:
        return None
    old, new = ordered[-2:]
    delta = new['metadata']['observed_at']-old['metadata']['observed_at']
    if compatibility(old) != compatibility(new) or not 0 < delta <= maximum_gap:
        return None
    return old, new


def motion_common(old, new, old_values, new_values, old_valid, new_valid):
    if compatibility(old) != compatibility(new):
        raise ValueError('provider_or_product_mismatch')
    if old_values.shape != new_values.shape or old_valid.shape != new_valid.shape:
        raise ValueError('provider_grid_mismatch')
    # Native provider identity is authoritative; dynamic fusion bits are unused.
    return old_valid & new_valid & np.isfinite(old_values) & np.isfinite(new_values)


def provider_series(values, valid, grid, vectors, *, product_at, issued_at,
                    target_x=0., target_y=0., horizon_seconds=7200., step_seconds=60.):
    points = np.array([[v['current_x_m'], v['current_y_m']] for v in vectors],
                      dtype=float).reshape(-1, 2)
    speeds = np.array([[v['vx_m_s'], v['vy_m_s']] for v in vectors],
                      dtype=float).reshape(-1, 2)
    return reverse_target_series(values, valid, grid,
                                 lambda x, y: local_motion(points, speeds, x, y),
                                 target_x, target_y, product_at, issued_at,
                                 horizon_seconds=horizon_seconds,
                                 step_seconds=step_seconds)


def combine_aligned_series(series, precedence):
    if not series:
        return []
    times = [s['at'] for s in next(iter(series.values()))]
    if any([s['at'] for s in samples] != times for samples in series.values()):
        raise ValueError('unaligned_prediction_times')
    result = []
    for index, at in enumerate(times):
        selected = next((p for p in precedence if p in series
                         and series[p][index]['rate_mm_h'] is not None), None)
        if selected is None:
            result.append({'at': at, 'rate_mm_h': None,
                           'reason': 'all_provider_support_unknown', 'provider_id': None})
        else:
            result.append({**series[selected][index], 'provider_id': selected})
    return result


def main():
    import argparse
    import collections
    import dataclasses
    import hashlib
    import json
    import math
    import os
    import sys
    import time
    import zlib
    from datetime import datetime
    from io import BytesIO
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from oldenburg_field_probe import passage_intervals, tiled_motion_vectors
    from storm_tracker_worker.thin_provider_adapters import ThinRadarField
    from storm_tracker_worker.thin_regional_radar_fusion import project_radar_field
    from storm_tracker_worker.thin_fusion import MAX_FUSION_AGE_SECONDS, PROVIDER_FUSION_AGE_SECONDS
    from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid
    from storm_tracker_worker.tracking_core.engine.pyramidal_sparse_flow import SparsePyramidalConfig

    parser = argparse.ArgumentParser()
    parser.add_argument('--providers', nargs='+', default=['knmi_radar'])
    args = parser.parse_args()
    root = Path('/home/ikke/storm-tracker/.analysis/oldenburg-replay-20261008')
    dest = root / 'source-clock-corrected'
    if (dest / 'predictions.json').exists() or (dest / 'policy.json').exists():
        raise RuntimeError('corrected_trial_already_started_do_not_duplicate')
    manifest = json.loads((root / 'export-manifest.json').read_text())
    native = json.loads((dest / 'native-manifest.json').read_text())
    previous = json.loads((root / 'replay-comparison.json').read_text())
    baseline = {r['field_id']: r for r in previous['records']}
    config_dict = json.loads((root / 'field-probe-policy.json').read_text())['motion_config']
    config = restore_motion_config(config_dict)
    m = manifest['frames'][0]['metadata']
    shape = manifest['frames'][0]['arrays']['values']['descriptor']['shape']
    grid = AnalysisGrid(m['identity']['grid_id'], m['crs'], *m['affine'], shape[1], shape[0])
    policy = {
        'scope': 'Offline source-consistent Oldenburg field prediction; no production changes',
        'providers_in_fixed_precedence': args.providers,
        'identity': 'Exact archived native provider/product/processing identity, NOT fusion bit codes',
        'clock': 'Motion from each provider own unique valid-time pair; RK2 includes product age to common historical issue time',
        'availability': 'Earliest original Oldenburg cycle with actual linked receipts all <= cycle evaluated_at; never legacy identity.available_at',
        'selection': 'Latest unique valid time, earliest available version per time, no incompatible products/processing/grids',
        'motion_config': dataclasses.asdict(config), 'config_fingerprint': config.fingerprint,
        'source_freshness_seconds': {p: PROVIDER_FUSION_AGE_SECONDS.get(p, MAX_FUSION_AGE_SECONDS) for p in args.providers},
        'pair_max_gap_seconds': 1200., 'forecast_horizon_seconds': 7200., 'step_seconds': 60.,
        'motion_transform': 'ln(1+rain_mm_h/0.1), output original quantitative QPE',
        'spatial_features': 'Identical 128px tiles / 96px stride / 24 features / deduplicated converged vectors',
        'local_motion': 'Unchanged nearest8 IDW, radius60km, at least3 vectors, maximum RMS8m/s, floor1500m',
        'combination': 'Only equal forecast valid times; fixed provider precedence; unknown never dry, no intensity sum or wet union',
        'thresholds': ['strictly >0', '>=0.1 mm/h'],
        'intensity_kernels': 'Earlier admitted object candidates retained separately; not a new intensity-kernel tracking replay',
        'limitations': ['No growth/decay model, ensemble or calibrated probability',
                        'Not full production scheduler/admission/hysteresis replay',
                        'Two-hour horizon and missing motion produce explicitly unknown ends',
                        'A single-source control does not qualify multi-provider forecast fusion'],
        'script_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'native_manifest_sha256': hashlib.sha256((dest / 'native-manifest.json').read_bytes()).hexdigest(),
        'reused_functions_sha256': hashlib.sha256((Path(__file__).parent / 'oldenburg_field_probe.py').read_bytes()).hexdigest(),
    }
    (dest / 'policy.json').write_text(json.dumps(policy, indent=2))
    source_dir = dest / 'executed-source'
    source_dir.mkdir(exist_ok=True)
    for source_name in ('oldenburg_source_clock_probe.py', 'oldenburg_field_probe.py',
                        'test_oldenburg_source_clock_probe.py'):
        source = Path(__file__).parent / source_name
        (source_dir / source_name).write_bytes(source.read_bytes())
    projected_cache = collections.OrderedDict()
    pair_cache = {}

    def projected(field):
        fid = field['field_id']
        if fid in projected_cache:
            projected_cache.move_to_end(fid)
            return projected_cache[fid]
        arrays = {}
        for name, ref in field['arrays'].items():
            raw = (dest / ref['file']).read_bytes()
            assert hashlib.sha256(raw).hexdigest() == ref['sha256']
            value = np.load(BytesIO(zlib.decompress(raw)), allow_pickle=False)
            assert hashlib.sha256(value.tobytes()).hexdigest() == ref['descriptor']['sha256']
            arrays[name] = value
        fm = field['metadata']
        source = ThinRadarField(fm['provider_id'], fm['product_id'], fm['observed_at'],
                                fm['crs'], *fm['affine'], arrays['values'], arrays['valid_mask'])
        layer = project_radar_field(source, grid)
        projected_cache[fid] = layer
        if len(projected_cache) > 8:
            projected_cache.popitem(last=False)
        return layer

    started = time.monotonic()
    records = []
    for index, frame in enumerate(manifest['frames']):
        stage = time.monotonic()
        issued = datetime.fromisoformat(frame['available_at']).timestamp()
        record = {'field_id': frame['field_id'], 'product_at': frame['metadata']['observed_at'],
                  'issued_at': issued, 'target_rate_mm_h': frame['target_rate_mm_h'],
                  'intensity_object_candidates': baseline[frame['field_id']]['baseline_candidates'],
                  'provider_predictions': {}, 'samples': [], 'passages': [], 'converged_vectors': []}
        aligned = {}
        for provider in args.providers:
            item = {'provider_id': provider, 'status': 'unknown_source_pair'}
            pair = provider_pair(native['fields'], provider, issued)
            if pair is not None:
                old, new = pair
                at = new['metadata']['observed_at']
                age = issued-at
                item.update(old_field_id=old['field_id'], new_field_id=new['field_id'],
                            product_at=at, product_age_seconds=age,
                            old_available_at=old['available_at'], new_available_at=new['available_at'],
                            pair_dt_seconds=at-old['metadata']['observed_at'])
                assert old['available_at'] <= issued and new['available_at'] <= issued
                if age > policy['source_freshness_seconds'][provider]:
                    item['status'] = 'unknown_source_stale'
                else:
                    try:
                        old_layer, new_layer = projected(old), projected(new)
                        key = (old['field_id'], new['field_id'])
                        if key not in pair_cache:
                            common = motion_common(old, new, old_layer.rain_rate_mm_h,
                                new_layer.rain_rate_mm_h, old_layer.valid_mask, new_layer.valid_mask)
                            vectors, audit = tiled_motion_vectors(old_layer.rain_rate_mm_h,
                                new_layer.rain_rate_mm_h, common, grid, old['field_id'], new['field_id'],
                                old['metadata']['observed_at'], at, config)
                            pair_cache[key] = (vectors, audit)
                        vectors, audit = pair_cache[key]
                        samples = provider_series(new_layer.rain_rate_mm_h, new_layer.valid_mask,
                                                  grid, vectors, product_at=at, issued_at=issued)
                        aligned[provider] = samples
                        item.update(status='candidate_with_support' if any(s['rate_mm_h'] is not None for s in samples) else 'unknown_motion_support',
                                    samples=samples, feature_audit=audit, converged_vectors=vectors,
                                    projected_target_rate_mm_h=float(new_layer.rain_rate_mm_h[170,170]) if new_layer.valid_mask[170,170] else None)
                        record['converged_vectors'].extend(vectors)
                    except (ValueError, RuntimeError) as error:
                        item.update(status='failed_experiment', error=str(error))
                        record.setdefault('errors', []).append({'provider': provider, 'error': str(error)})
            record['provider_predictions'][provider] = item
        record['samples'] = combine_aligned_series(aligned, args.providers)
        if not record['samples']:
            record['samples'] = [{'at': issued+offset, 'rate_mm_h': None,
                                  'reason': 'no_eligible_source_pair', 'provider_id': None}
                                 for offset in range(0, 7201, 60)]
        record['known_sample_count'] = sum(s['rate_mm_h'] is not None for s in record['samples'])
        record['prediction_status'] = 'candidate_with_support' if record['known_sample_count'] else 'unknown_source_or_motion_support'
        record['passages'] = passage_intervals(record['samples'])
        record['weak_passages'] = passage_intervals(record['samples'], math.nextafter(0., 1.))
        record['elapsed_seconds'] = time.monotonic()-stage
        records.append(record)
        output = {'scope': policy['scope'], 'policy': policy, 'records': records,
                  'complete': len(records) == len(manifest['frames']),
                  'processed_frames': len(records), 'planned_frames': len(manifest['frames']),
                  'total_seconds': time.monotonic()-started}
        (dest / 'predictions.json').write_text(json.dumps(output, separators=(',', ':')))
        progress = {'status': 'processing' if not output['complete'] else 'calculation_complete_pending_score',
                    'pid': os.getpid(), 'processed_frames': len(records), 'planned_frames': len(manifest['frames']),
                    'elapsed_seconds': output['total_seconds'], 'source_path': str(Path(__file__)),
                    'script_sha256': policy['script_sha256'], 'production_changed': False}
        (dest / 'progress.json').write_text(json.dumps(progress, indent=2))
        print(json.dumps({'frame': index+1, 'of': len(manifest['frames']),
                          'status': record['prediction_status'], 'known_samples': record['known_sample_count'],
                          'provider_status': {p: r['status'] for p,r in record['provider_predictions'].items()},
                          'seconds': round(record['elapsed_seconds'], 2)}), flush=True)


if __name__ == '__main__':
    main()
