import numpy as np


def test_connected_pixels_become_one_exact_closed_component(raster_components_module):
    values = np.zeros((5, 6), dtype=np.uint8)
    values[1:4, 2:5] = 3

    components = raster_components_module.extract_components(
        values,
        lambda row, col: (row, col),
    )

    assert len(components) == 1
    component = components[0]
    assert len(component.pixels) == 9
    assert component.centroid_row == 2.5
    assert component.centroid_col == 3.5
    assert component.boundary[0] == component.boundary[-1]
    assert set(component.boundary[:-1]) == {
        (1.0, 2.0), (1.0, 5.0), (4.0, 5.0), (4.0, 2.0)
    }


def test_separate_echoes_do_not_form_a_grid_chain(raster_components_module):
    values = np.zeros((6, 8), dtype=np.uint8)
    values[1:3, 1:3] = 2
    values[1:3, 5:7] = 5

    components = raster_components_module.extract_components(
        values,
        lambda row, col: (row, col),
    )

    assert len(components) == 2
    assert [component.max_intensity for component in components] == [2, 5]


def test_component_preserves_outer_ring_and_dry_hole(
    raster_components_module,
):
    values = np.ones((5, 5), dtype=np.uint8)
    values[1:4, 1:4] = 0

    components = raster_components_module.extract_components(
        values,
        lambda row, col: (row, col),
    )

    assert len(components) == 1
    component = components[0]
    assert len(component.polygons) == 1
    assert len(component.polygons[0]) == 2
    outer, hole = component.polygons[0]
    assert outer[0] == outer[-1]
    assert hole[0] == hole[-1]
    assert raster_components_module._signed_grid_area(outer) == 25.0
    assert raster_components_module._signed_grid_area(hole) == -9.0
    assert component.boundary == outer
