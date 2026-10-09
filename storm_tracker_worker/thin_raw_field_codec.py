"""Lossless, pickle-free radar field snapshots and content-addressed encoding."""
from __future__ import annotations
from dataclasses import asdict, dataclass, replace
import hashlib
from io import BytesIO
import json
from types import MappingProxyType
from typing import Any, Mapping
import zlib
import numpy as np
from .thin_provider_adapters import ThinRadarField, ThinVisualRadarField


@dataclass(frozen=True, slots=True)
class RawFieldJob:
    field: ThinRadarField | ThinVisualRadarField
    region_revision_id: str
    confidence: Any = None
    contributor_mask: Any = None
    source_capture_ids: tuple[str, ...] = ()
    role: str = 'used'


@dataclass(frozen=True, slots=True)
class SerializedRadarField:
    field_id: str
    metadata: Mapping[str, Any]
    arrays: Mapping[str, bytes]
    array_descriptors: Mapping[str, Any]


def _copy_array(value):
    if value is None:
        return None
    value = np.asarray(value)
    if value.dtype.hasobject:
        raise ValueError('object_array_forbidden')
    result = value.copy(order='C')
    result.setflags(write=False)
    return result


def snapshot_field_job(field, *, region_revision_id, confidence=None,
                       contributor_mask=None, source_capture_ids=(), role='used'):
    """Called off-loop before consumption; never changes original array flags."""
    if isinstance(field, ThinRadarField):
        copied = replace(field, rain_rate_mm_h=_copy_array(field.rain_rate_mm_h),
                         valid_mask=_copy_array(field.valid_mask))
    else:
        copied = replace(field, levels=_copy_array(field.levels))
    return RawFieldJob(copied, region_revision_id, _copy_array(confidence),
                       _copy_array(contributor_mask), tuple(source_capture_ids), role)


def encode_radar_field(field, *, region_revision_id, confidence=None,
                       contributor_mask=None):
    metadata = {'codec_version': 1, 'region_revision_id': region_revision_id,
                'provider_id': field.provider_id, 'product_id': field.product_id,
                'observed_at': field.observed_at, 'crs': field.crs}
    if isinstance(field, ThinRadarField):
        metadata.update(kind='quantitative', identity=asdict(field.identity),
                        quantity=field.identity.quantity, unit=field.identity.unit,
                        affine=[field.origin_x_m, field.origin_y_m,
                                field.x_resolution_m, field.y_resolution_m])
        values = {'values': field.rain_rate_mm_h, 'valid_mask': field.valid_mask}
    else:
        metadata.update(kind='visual', quantity='visual_classes', unit='class',
                        affine=[field.longitude_left, field.latitude_top,
                                field.longitude_scale, field.latitude_scale],
                        nominal_coverage=[asdict(x) for x in field.nominal_coverage])
        values = {'levels': field.levels}
    if confidence is not None:
        values['confidence'] = confidence
    if contributor_mask is not None:
        values['contributor_mask'] = contributor_mask
    arrays, descriptors = {}, {}
    for name, value in values.items():
        value = np.asarray(value)
        if value.dtype.hasobject:
            raise ValueError('object_array_forbidden')
        out = BytesIO()
        np.save(out, value, allow_pickle=False)
        arrays[name] = zlib.compress(out.getvalue())
        descriptors[name] = {'dtype': value.dtype.str, 'shape': list(value.shape),
                             'byte_count': value.nbytes,
                             'sha256': hashlib.sha256(value.tobytes(order='C')).hexdigest(),
                             'codec': 'npy-zlib-v1'}
    identity = json.dumps({'metadata': metadata, 'arrays': descriptors},
                          sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
    return SerializedRadarField(hashlib.sha256(identity).hexdigest(),
                                MappingProxyType(metadata), MappingProxyType(arrays),
                                MappingProxyType(descriptors))


def decode_radar_field(record):
    arrays = {}
    for name, payload in record.arrays.items():
        descriptor = record.array_descriptors[name]
        byte_count = int(descriptor['byte_count'])
        if not 0 <= byte_count <= 512*1024*1024:
            raise ValueError('raw_field_size_invalid')
        # The bounded extra allowance covers the NPY header, not arbitrary
        # decompression. Reject concatenated/truncated streams as well.
        decoder = zlib.decompressobj()
        decoded = decoder.decompress(payload,byte_count+65537)
        if len(decoded)>byte_count+65536 or not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:
            raise ValueError('raw_field_size_invalid')
        value = np.load(BytesIO(decoded), allow_pickle=False)
        if (value.dtype.str != descriptor['dtype'] or list(value.shape) != descriptor['shape']
            or value.nbytes != byte_count
            or hashlib.sha256(value.tobytes(order='C')).hexdigest() != descriptor['sha256']):
            raise ValueError('raw_field_integrity_mismatch')
        arrays[name] = value
    return {'metadata': dict(record.metadata), 'arrays': arrays}
