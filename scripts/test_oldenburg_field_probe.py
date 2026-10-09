"""Focused arithmetic/support checks for the disposable probe only."""
from datetime import datetime, timezone
from types import SimpleNamespace
import numpy as np
from oldenburg_field_probe import local_motion, reverse_target_series, passage_intervals, available_history, tiled_motion_vectors

def test_missing_local_support_never_becomes_zero_motion():
    points=np.array([[0.,0.],[1.,0.]])
    assert local_motion(points,np.ones((2,2)),0.,0.) is None

def test_motion_is_local_not_one_global_mean():
    points=np.array([[0.,0.],[1.,0.],[0.,1.],[1000.,0.],[1001.,0.],[1000.,1.]])
    vectors=np.array([[2.,0.]]*3+[[8.,0.]]*3)
    assert np.allclose(local_motion(points,vectors,0.,0.,radius_m=10.)['vector'],[2.,0.])
    assert np.allclose(local_motion(points,vectors,1000.,0.,radius_m=10.)['vector'],[8.,0.])

def test_inconsistent_motion_is_unavailable():
    points=np.array([[0.,0.],[1.,0.],[0.,1.]])
    vectors=np.array([[30.,0.],[-30.,0.],[0.,30.]])
    assert local_motion(points,vectors,.3,.3,maximum_spread_m_s=1.) is None

def grid():
    return SimpleNamespace(origin_x_m=0.,origin_y_m=5.,x_resolution_m=1.,y_resolution_m=1.)

def motion(x,y):
    return {'vector':(1.,0.),'maximum_distance_m':1.,'spread_m_s':0.}

def test_light_rain_is_included_without_one_mm_cell_mask():
    values=np.full((5,12),.2)
    rows=reverse_target_series(values,np.ones_like(values,dtype=bool),grid(),motion,5.5,2.5,0.,0.,horizon_seconds=1.,step_seconds=1.)
    assert [r['rate_mm_h'] for r in rows]==[.2,.2]

def test_age_since_product_is_included_before_future_offset():
    values=np.tile(np.arange(12,dtype=float),(5,1))
    rows=reverse_target_series(values,np.ones_like(values,dtype=bool),grid(),motion,8.5,2.5,0.,2.,horizon_seconds=2.,step_seconds=1.)
    assert [r['rate_mm_h'] for r in rows]==[6.,5.,4.]

def test_future_product_is_rejected():
    values=np.ones((5,12))
    try:
        reverse_target_series(values,np.ones_like(values,dtype=bool),grid(),motion,5.5,2.5,10.,9.)
    except ValueError:
        return
    assert False,'future radar product was used'

def test_nodata_never_becomes_dry():
    values=np.ones((5,12)); valid=np.ones_like(values,dtype=bool);valid[2,5]=False
    rows=reverse_target_series(values,valid,grid(),motion,5.5,2.5,0.,0.,horizon_seconds=0.,step_seconds=1.)
    assert rows[0]['rate_mm_h'] is None

def test_reverse_trace_integrates_variable_spatial_motion():
    def variable(x,y):
        return {'vector':(x,0.),'maximum_distance_m':1.,'spread_m_s':0.}
    values=np.tile(np.arange(12,dtype=float),(5,1))
    rows=reverse_target_series(values,np.ones_like(values,dtype=bool),grid(),variable,8.5,2.5,0.,0.,horizon_seconds=1.,step_seconds=1.)
    # dx/ds=-x -> x(1)=8.5/e=3.127, in source column 3; not 8.5-8.5=0.
    assert rows[-1]['rate_mm_h']==3.

def test_dry_gap_remains_a_gap():
    samples=[{'at':0.,'rate_mm_h':.2},{'at':60.,'rate_mm_h':.2},{'at':120.,'rate_mm_h':0.},{'at':180.,'rate_mm_h':.2},{'at':240.,'rate_mm_h':0.}]
    result=passage_intervals(samples)
    assert len(result)==2
    assert result[0]['exit_bracket']==[60.,120.]
    assert result[1]['entry_bracket']==[120.,180.]

def test_horizon_end_is_not_invented():
    result=passage_intervals([{'at':0.,'rate_mm_h':0.},{'at':60.,'rate_mm_h':.2},{'at':120.,'rate_mm_h':.2}])
    assert len(result)==1
    assert result[0]['exit_at'] is None
    assert result[0]['end_status']=='horizon_censored'
    assert result[0]['last_supported_at']==120.

def test_unknown_gap_is_not_bridged_or_claimed_dry():
    result=passage_intervals([{'at':0.,'rate_mm_h':.2},{'at':60.,'rate_mm_h':None},{'at':120.,'rate_mm_h':.2},{'at':180.,'rate_mm_h':0.}])
    assert len(result)==2
    assert result[0]['exit_at'] is None
    assert result[0]['end_status']=='support_censored'
    assert result[1]['entry_at'] is None

def test_predictor_history_never_uses_future_receipts():
    frames=[{'metadata':{'observed_at':10.},'available_at':'1970-01-01T00:00:12+00:00'},{'metadata':{'observed_at':11.},'available_at':'1970-01-01T00:00:20+00:00'},{'metadata':{'observed_at':16.},'available_at':'1970-01-01T00:00:15+00:00'}]
    assert available_history(frames,15.)==[frames[0]]

def test_spatial_selection_tracks_weak_zone_alongside_strong_zone():
    import sys
    from pathlib import Path
    sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
    from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid
    from storm_tracker_worker.tracking_core.engine.pyramidal_sparse_flow import SparsePyramidalConfig
    rows,cols=np.mgrid[:128,:240]
    old=.2*np.exp(-((cols-42)**2+(rows-64)**2)/70.)+20.*np.exp(-((cols-175)**2+(rows-64)**2)/70.)
    new=np.roll(old,2,axis=1);new[:,:2]=0.
    g=AnalysisGrid('test','EPSG:3857',0.,192000.,1500.,1500.,240,128)
    config=SparsePyramidalConfig(131072,384,6.,2,.8,1e-4,3,.8,3,20,20,.03,1e-4,1e6,0.,1e-6,((1.,0.),(0.,1.)))
    vectors,_=tiled_motion_vectors(old,new,np.ones_like(old,dtype=bool),g,'old','new',0.,300.,config)
    weak=[v for v in vectors if v['current_x_m']<150000.]
    strong=[v for v in vectors if v['current_x_m']>=150000.]
    assert len(weak)>=3 and len(strong)>=3
    # Physical displacement =2pixels*1500m/300s=10m/s east, no north movement.
    assert abs(np.median([v['vx_m_s'] for v in weak])-10.)<1.
    assert abs(np.median([v['vy_m_s'] for v in weak]))<1.
