"""Offline exact legacy core + explicit quantitative input boundary.

Not a full historical HA/WMS/scheduler reconstruction. Production never imports
this file. Future observations are read only by the separate scoring function.
"""
import asyncio
import collections
from datetime import datetime, timezone
import hashlib
import importlib
import json
import math
from pathlib import Path
import sys
import time
import types
from unittest.mock import patch

import numpy as np

ROOT = Path('/home/ikke/storm-tracker/.analysis/oldenburg-replay-20261008')
LEGACY_COMMIT = '0c9725dbb62c6fec3997348062bcc60478ee572b'


def verify_completed_hashes(root, manifest):
    entries = manifest['current_files']
    assert entries, 'empty completed-input manifest'
    for name, expected in entries.items():
        path = Path(root)/name
        assert path.is_file(), name
        assert hashlib.sha256(path.read_bytes()).hexdigest() == expected, name
    return len(entries)


def eligible_fields(fields, issued_at):
    unique = {}
    for f in sorted(fields, key=lambda f: (f['available_at'], f['field_id'])):
        m = f['metadata']
        if (m['provider_id'] == 'knmi_radar' and f['available_at'] <= issued_at
                and m['observed_at'] <= issued_at):
            unique.setdefault(m['observed_at'], f)
    return sorted(unique.values(), key=lambda f: (f['available_at'], f['metadata']['observed_at']))


def paired_label(predicted_wet, observed_wet):
    if predicted_wet is None:
        return 'unknown_on_wet' if observed_wet else 'unknown_on_dry'
    if predicted_wet:
        return 'hit' if observed_wet else 'false_wet'
    return 'miss' if observed_wet else 'correct_dry'


def compare_pairs(old_records, new_records, observations, threshold):
    assert len(old_records) == len(new_records)
    labels = ('hit', 'miss', 'false_wet', 'correct_dry', 'unknown_on_wet', 'unknown_on_dry')
    output = {}

    def nearest(samples, when, key):
        if not samples or when < samples[0]['at'] or when > samples[-1]['at']:
            return None
        sample = min(samples, key=lambda s: abs(s['at']-when))
        return sample[key] if abs(sample['at']-when) <= 30.000001 else None

    for lower, upper in ((5, 30), (30, 60), (60, 90), (90, 120)):
        counts = {k: dict.fromkeys(labels, 0) for k in (
            'old_all', 'new_all', 'old_on_common', 'new_on_common')}
        common = total = 0
        for old, new in zip(old_records, new_records):
            assert old['issued_at'] == new['issued_at']
            for observation in observations:
                when = observation['metadata']['observed_at']
                lead = (when-old['issued_at'])/60.
                if not lower <= lead < upper or not observation['target_valid']:
                    continue
                rate = observation['target_rate_mm_h']
                if rate is None:
                    continue
                wet = rate > 0 if threshold == 0 else rate >= threshold
                older = nearest(old['selected_geometry_samples'], when, 'wet')
                newer = nearest(new['samples'], when, 'rate_mm_h')
                newer = None if newer is None else (newer > 0 if threshold == 0 else newer >= threshold)
                old_label, new_label = paired_label(older, wet), paired_label(newer, wet)
                counts['old_all'][old_label] += 1
                counts['new_all'][new_label] += 1
                total += 1
                if older is not None and newer is not None:
                    counts['old_on_common'][old_label] += 1
                    counts['new_on_common'][new_label] += 1
                    common += 1
        output[f'{lower}-{upper}min'] = {**counts, 'forecast_observation_pairs': total,
                                         'common_known_pairs': common}
    return output


def geometry_intervals(samples):
    from oldenburg_field_probe import passage_intervals
    mapped = [{'at': s['at'], 'rate_mm_h': None if s['wet'] is None else float(s['wet']),
               'reason': s.get('reason')} for s in samples]
    intervals = passage_intervals(mapped, .5)
    for interval in intervals:
        # These inputs are Boolean indicators, NOT physical mm/hour.
        interval.pop('mean_mm_h', None)
        interval.pop('peak_mm_h', None)
        interval['basis'] = 'adapter_exposed_legacy_selected_contour_geometry'
    return intervals


class LegacyHarness:
    def __init__(self, source_root, issued_at):
        self.now = float(issued_at)
        prefix = f'legacy0498_{id(self)}'
        package = types.ModuleType(prefix)
        package.__path__ = [str(source_root)]
        sys.modules[prefix] = package
        # Capture historical factories BEFORE legacy Storm class definition.
        with patch('time.time', self.clock):
            self.observation = importlib.import_module(prefix + '.engine.observation')
            self.raster = importlib.import_module(prefix + '.providers.raster_components')
            self.odim = importlib.import_module(prefix + '.providers.odim_hdf5')
            self.engine_module = importlib.import_module(prefix + '.engine.storm_engine')
            self.nowcast = importlib.import_module(prefix + '.engine.nowcast')
            self.engine = self.engine_module.StormEngine()
        clock = self.clock

        class HistoricalDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return datetime.fromtimestamp(clock(), tz or timezone.utc)

        self.nowcast.datetime = HistoricalDateTime

    def clock(self):
        return self.now

    def observations(self, values, valid, corner_to_latlon, *, product_at,
                     pixel_area_km2):
        values = np.asarray(values)
        valid = np.asarray(valid, dtype=bool)
        intensity = np.zeros(values.shape, np.uint8)
        for row, col in np.argwhere(valid & np.isfinite(values) & (values >= .1)):
            intensity[row, col] = self.odim.rain_rate_to_intensity(float(values[row, col]))
        result = []
        for component in self.raster.extract_components(intensity, corner_to_latlon):
            lat, lon = corner_to_latlon(component.centroid_row, component.centroid_col)
            cid = f'knmi:{product_at:.0f}:c{component.index}'
            area = len(component.pixels) * pixel_area_km2
            result.append(self.observation.Observation(
                self.observation.ObservationType.RADAR, lat, lon, product_at,
                intensity=component.max_intensity, area_km2=area, quality=.98,
                footprint_points=component.boundary, radar_cell_id=cid,
                parent_system_id=cid, parent_area_km2=area,
                parent_footprint_points=component.boundary, source='knmi'))
        return result

    def process(self, observations, available_at):
        self.now = float(available_at)
        with patch('time.time', self.clock):
            # Preserve legacy empty-batch early return and lifecycle behavior.
            asyncio.run(self.engine.process_batch(observations))

    def evaluate(self, issued_at, lat, lon):
        self.now = float(issued_at)
        with patch('time.time', self.clock):
            storms = self.engine.get_active_storms()
            status = self.nowcast.build_precipitation_status(storms, lat, lon,
                                                            radar_source='knmi')
            selected = next((s for s in storms if s.storm_id == status.get('storm_id')), None)
            motion, samples, diagnostic = {}, [], None
            if selected is not None:
                point = selected.closest_radar_point(lat, lon)
                motion = selected.motion_to_target(lat, lon, distance_km=point[0] if point else None)
                diagnostic = {k: getattr(selected, k) for k in (
                    'storm_id', 'confidence', 'speed_kmh', 'heading_deg',
                    'motion_model', 'motion_sample_count', 'motion_history_minutes',
                    'motion_fit_quality', 'motion_prediction_error_km')}
                diagnostic.update(tracking_status=selected.tracking_status,
                                  last_radar_timestamp=selected.last_radar_timestamp)
                if (selected.confidence in {'Matig', 'Hoog'}
                        and selected.tracking_status == 'bevestigd'
                        and selected.heading_deg is not None
                        and selected.speed_kmh is not None and selected.speed_kmh > 0):
                    raw = selected._trajectory_passage_samples(
                        lat, lon, current_edge_distance_km=point[0] if point else 0.)
                    samples = [{'at': issued_at + s['minutes'] * 60.,
                                'wet': s['edge_distance_km'] <= .1,
                                'edge_distance_km': s['edge_distance_km'],
                                'envelope_hit': s['edge_distance_km'] <= s['uncertainty_km']}
                               for s in raw]
            if not samples:
                samples = [{'at': issued_at + minute * 60., 'wet': None,
                            'reason': 'legacy_no_admitted_selected_motion'}
                           for minute in range(91)]
            return {'published_status': status, 'published_end_at': None,
                    'selected_motion': motion, 'selected_storm': diagnostic,
                    'selected_geometry_samples': samples, 'active_storm_count': len(storms)}


def main():
    import argparse
    from io import BytesIO
    import zlib
    from pyproj import Transformer
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from storm_tracker_worker.thin_provider_adapters import ThinRadarField
    from storm_tracker_worker.thin_regional_radar_fusion import project_radar_field
    from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid

    parser = argparse.ArgumentParser()
    parser.add_argument('--legacy-root', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--limit', type=int)
    args = parser.parse_args()
    dest = args.output_dir
    dest.mkdir(parents=True, exist_ok=True)
    if (dest / 'predictions.json').exists():
        raise RuntimeError('legacy_job_already_started_do_not_duplicate')
    source = ROOT / 'source-clock-corrected'
    native = json.loads((source / 'native-manifest.json').read_text())
    new = json.loads((source / 'predictions.json').read_text())
    assert verify_completed_hashes(source, json.loads((source/'final-manifest.json').read_text())) == 9
    manifest = json.loads((ROOT / 'export-manifest.json').read_text())
    assert new['complete'] and len(new['records']) == 68
    meta = manifest['frames'][0]['metadata']
    grid = AnalysisGrid(meta['identity']['grid_id'], meta['crs'], *meta['affine'], 340, 340)
    transformer = Transformer.from_crs(grid.crs, 'EPSG:4326', always_xy=True)

    def corner(row, col):
        lon, lat = transformer.transform(grid.origin_x_m + col*grid.x_resolution_m,
                                         grid.origin_y_m - row*grid.y_resolution_m)
        return float(lat), float(lon)

    policy = {
        'scope': 'Controlled old core/native-QPE input versus completed new KNMI field trial',
        'legacy_commit': LEGACY_COMMIT, 'legacy_version': '0.4.98',
        'legacy_source_changed': False, 'production_changed': False,
        'target': [53.1435, 8.2146], 'cold_start': True,
        'input_boundary': 'Shared modern 340x340 1.5km projection, exact legacy generic ODIM rainfall categories >=0.1 mm/h, 4-connected components; NOT original KNMI WMS acquisition',
        'clock': 'Process every unique then-available valid product at its actual first cycle availability; historical issue clock for original publication; preserve original omission of product-age advection',
        'geometry_diagnostic': 'Selected-system private contour samples exposed, binary wet at edge<=0.1km. Derived intervals are NOT original published forecasts/end times',
        'original_forecast_end': 'Not implemented in old Nowcast; always unknown',
        'original_intensity': 'No invented dBZ; publication may abstain as intensiteit_onbekend',
        'sources_excluded': ['lightning', 'ground', 'pressure', 'bias', 'external KNMI issued forecasts', 'other radar providers'],
        'limits': ['KNMI pinned rather than original DE/DWD country policy',
                   'Not whole historical HA/scheduler or restored live-state replay',
                   'Legacy outer rings and convex hulls discard holes; preserve this behavior',
                   'New method is exploratory not production-qualified; this is one event',
                   'Weak radar >0 versus legacy >=0.1 evaluated separately; no exact ground truth'],
        'new_predictions_sha256': hashlib.sha256((source / 'predictions.json').read_bytes()).hexdigest(),
        'native_manifest_sha256': hashlib.sha256((source / 'native-manifest.json').read_bytes()).hexdigest(),
        'adapter_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }
    (dest / 'policy.json').write_text(json.dumps(policy, indent=2))
    (dest / 'executed-adapter.py').write_bytes(Path(__file__).read_bytes())
    source_hashes = {str(p.relative_to(args.legacy_root)): hashlib.sha256(p.read_bytes()).hexdigest()
                     for p in args.legacy_root.rglob('*.py')}
    (dest / 'legacy-source-hashes.json').write_text(json.dumps(source_hashes, indent=2))
    harness = LegacyHarness(args.legacy_root, new['records'][0]['issued_at'])
    processed, records = set(), []
    started = time.monotonic()
    for index, newer in enumerate(new['records'][:args.limit]):
        stage = time.monotonic()
        issued = newer['issued_at']
        consumed = []
        available = eligible_fields(native['fields'], issued)
        for f in available:
            if f['field_id'] in processed:
                continue
            arrays = {}
            for key, ref in f['arrays'].items():
                raw = (source / ref['file']).read_bytes()
                assert hashlib.sha256(raw).hexdigest() == ref['sha256']
                array = np.load(BytesIO(zlib.decompress(raw)), allow_pickle=False)
                assert hashlib.sha256(array.tobytes()).hexdigest() == ref['descriptor']['sha256']
                arrays[key] = array
            fm = f['metadata']
            layer = project_radar_field(ThinRadarField(fm['provider_id'], fm['product_id'],
                fm['observed_at'], fm['crs'], *fm['affine'], arrays['values'], arrays['valid_mask']), grid)
            obs = harness.observations(layer.rain_rate_mm_h, layer.valid_mask, corner,
                                       product_at=fm['observed_at'], pixel_area_km2=2.25)
            harness.process(obs, f['available_at'])
            processed.add(f['field_id'])
            consumed.append({'field_id': f['field_id'], 'product_at': fm['observed_at'],
                             'available_at': f['available_at'], 'component_count': len(obs),
                             'projected_values_sha256': hashlib.sha256(layer.rain_rate_mm_h.tobytes()).hexdigest(),
                             'projected_valid_sha256': hashlib.sha256(layer.valid_mask.tobytes()).hexdigest()})
        result = harness.evaluate(issued, 53.1435, 8.2146)
        samples = result['selected_geometry_samples']
        result.update(field_id=newer['field_id'], issued_at=issued,
                      new_field_product_at=newer['provider_predictions']['knmi_radar'].get('product_at'),
                      newly_consumed=consumed, eligible_native_fields=len(available),
                      adapter_derived_intervals=geometry_intervals(samples),
                      known_geometry_samples=sum(s['wet'] is not None for s in samples),
                      elapsed_seconds=time.monotonic()-stage)
        records.append(result)
        complete = len(records) == len(new['records'])
        output = {'policy': policy, 'records': records, 'complete': complete,
                  'planned_issues': len(new['records']), 'processed_issues': len(records),
                  'native_fields_consumed': len(processed), 'elapsed_seconds': time.monotonic()-started}
        (dest / 'predictions.json').write_text(json.dumps(output, separators=(',', ':')))
        (dest / 'progress.json').write_text(json.dumps({k: output[k] for k in (
            'complete', 'planned_issues', 'processed_issues', 'native_fields_consumed', 'elapsed_seconds')}, indent=2))
        print(json.dumps({'issue': index+1, 'of': len(new['records']),
                          'new_fields': len(consumed), 'storms': result['active_storm_count'],
                          'known_geometry': result['known_geometry_samples'],
                          'publication': result['published_status'].get('forecast_block_reason'),
                          'seconds': round(result['elapsed_seconds'], 2)}), flush=True)


if __name__ == '__main__':
    main()
