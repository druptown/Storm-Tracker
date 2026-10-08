import numpy as np
from oldenburg_rear_edge_diagnosis import match_translation, changes


def test_registration_recovers_north_east_shift_with_intensity_change():
    # Reversing the displacement sign would turn advection into artificial growth.
    a = np.random.default_rng(17).normal(size=(13,15))
    b = np.zeros((21,23))
    b[2:15,7:22] = a*2+3
    result = match_translation(a,b,np.ones_like(b,dtype=bool),4)
    assert result is not None
    assert (result['dr'],result['dc']) == (-2,3)
    assert abs(result['ncc']-1) < 1e-10


def test_flat_pattern_abstains_instead_of_claiming_stationarity():
    a = np.ones((5,5)); b = np.ones((9,9))
    assert match_translation(a,b,np.ones_like(b,dtype=bool),2) is None


def test_missing_data_cannot_be_registered_as_zero_rain():
    a = np.random.default_rng(6).normal(size=(7,7))
    b = np.zeros((11,11)); b[2:9,2:9] = a
    assert match_translation(a,b,np.zeros_like(b,dtype=bool),2) is None


def test_growth_loss_and_existing_wet_are_separate():
    old = np.array([[0.,.2],[.2,.2]])
    new = np.array([[.2,.2],[0.,.2]])
    assert changes(old,new,np.ones((2,2),dtype=bool),.1) == dict(
        valid_pixels=4, old_wet=3,new_wet=3,newly_wet=1,lost_wet=1,retained_wet=2)


def test_nodata_is_excluded_and_weak_rain_is_not_policy_wet():
    old = np.array([[0.,0.],[.06,.2]])
    new = np.array([[.2,.2],[.06,.2]])
    valid = np.array([[False,False],[True,True]])
    assert changes(old,new,valid,.1)['new_wet'] == 1
    assert changes(old,new,valid,0.)['new_wet'] == 2
    assert changes(old,new,valid,.1)['newly_wet'] == 0
