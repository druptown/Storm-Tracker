import importlib.util
from dataclasses import replace
import numpy as np
import pytest
from storm_tracker_worker.thin_provider_adapters import ThinRadarField, ThinVisualRadarField


def api():
    spec = importlib.util.find_spec('storm_tracker_worker.thin_raw_field_codec')
    assert spec is not None, 'lossless field archive missing'
    return __import__(spec.name, fromlist=[''])


@pytest.mark.parametrize('dtype', ['<f4', '>f8'])
def test_float_roundtrip_preserves_exact_bytes_mask_affine_and_contributions(dtype):
    m = api()
    values = np.array([[1.25, np.nan], [0, 8.5]], dtype=dtype)
    mask = np.array([[True, False], [True, True]])
    f = ThinRadarField('knmi_radar', 'qpe', 100, 'EPSG:28992', 1, 2, 1000, 900, values, mask)
    f = replace(f, identity=replace(f.identity, quantity='reflectivity', unit='dBZ'))
    record = m.encode_radar_field(f, region_revision_id='Groningen-revision',
        confidence=np.array([[.1,.2],[.3,.4]], dtype='f4'), contributor_mask=np.array([[1,2],[4,8]],dtype='u2'))
    decoded = m.decode_radar_field(record)
    assert decoded['arrays']['values'].dtype == values.dtype
    assert decoded['arrays']['values'].tobytes() == values.tobytes()
    assert decoded['arrays']['valid_mask'].tobytes() == mask.tobytes()
    assert decoded['metadata']['identity']['quantity'] == 'reflectivity'
    assert decoded['metadata']['crs'] == 'EPSG:28992'
    assert decoded['metadata']['affine'] == [1, 2, 1000, 900]
    assert decoded['arrays']['contributor_mask'].tolist() == [[1,2],[4,8]]


def test_visual_roundtrip_is_not_quantitative():
    m = api()
    f = ThinVisualRadarField('rainviewer','visual',100,6,54,.01,.02,np.array([[0,8]],dtype='u1'))
    decoded = m.decode_radar_field(m.encode_radar_field(f, region_revision_id='Groningen'))
    assert decoded['metadata']['quantity'] == 'visual_classes'
    assert decoded['metadata']['unit'] == 'class'
    assert decoded['arrays']['levels'].tolist() == [[0,8]]


def test_object_arrays_are_rejected_without_pickle():
    m = api()
    f = ThinVisualRadarField('rainviewer','visual',100,6,54,.01,.02,np.array([[0]],dtype='u1'))
    with pytest.raises(ValueError, match='object'):
        m.encode_radar_field(f,region_revision_id='Groningen',confidence=np.array([[object()]],dtype=object))


def test_snapshot_survives_source_mutation():
    m = api()
    f = ThinRadarField('knmi_radar','qpe',100,'EPSG:28992',1,2,1000,1000,np.array([[1.]],dtype='f4'),np.array([[True]]))
    job = m.snapshot_field_job(f,region_revision_id='Groningen',source_capture_ids=('receipt',))
    f.rain_rate_mm_h[0,0] = 9
    assert job.field.rain_rate_mm_h[0,0] == 1
    assert f.rain_rate_mm_h.flags.writeable


def test_decompression_rejects_output_larger_than_declared_array():
    from types import MappingProxyType
    import zlib
    m=api()
    f=ThinVisualRadarField('rainviewer','visual',100,6,54,.01,.02,np.array([[0]],dtype='u1'))
    record=m.encode_radar_field(f,region_revision_id='Groningen')
    poisoned=replace(record,arrays=MappingProxyType({'levels':zlib.compress(b'x'*100000)}))
    with pytest.raises(ValueError,match='raw_field_size_invalid'):
        m.decode_radar_field(poisoned)
