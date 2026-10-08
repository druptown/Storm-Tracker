from oldenburg_rear_edge_subpixel import peak_offset


def test_refine_recovers_known_quadratic_peak_without_rounding_bias():
    assert abs(peak_offset(-.5625,.9375,.4375)-.25) < 1e-12
    assert abs(peak_offset(.4375,.9375,-.5625)+.25) < 1e-12


def test_flat_and_missing_neighbors_do_not_invent_offset():
    assert peak_offset(1.,1.,1.) == 0.
    assert peak_offset(float('nan'),.9,.8) == 0.
