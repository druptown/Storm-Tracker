import pytest


class State:
    def __init__(self, **attributes):
        self.attributes = attributes


def test_home_is_always_primary_target(targets_module):
    specs = targets_module.build_target_specs(51.0, 4.0)
    assert len(specs) == 1
    assert specs[0].target_id == "home"
    assert specs[0].entity_id == "zone.home"
    assert specs[0].primary is True
    assert (specs[0].fallback_lat, specs[0].fallback_lon) == (51.0, 4.0)


def test_life360_person_gets_stable_sensor_suffix(targets_module):
    specs = targets_module.build_target_specs(51.0, 4.0, [{
        "id": "elke_life360",
        "name": "Elke",
        "location_entity": "device_tracker.elke_life360",
    }])
    assert specs[1].entity_id == "device_tracker.elke_life360"
    assert specs[1].entity_suffix == "elke_life360"
    assert specs[1].primary is False


def test_test_tracker_is_optional_and_secondary(targets_module):
    specs = targets_module.build_target_specs(
        51.0, 4.0, test_tracker_entity="device_tracker.fictieve_tracker"
    )
    assert [spec.target_id for spec in specs] == ["home", "test_tracker"]
    assert specs[1].name == "Fictieve tracker (test)"
    assert specs[1].primary is False


def test_live_coordinates_override_fixed_fallback(targets_module):
    spec = targets_module.TargetSpec(
        "elke", "Elke", "device_tracker.elke", 51.0, 4.0
    )
    coordinates = targets_module.coordinates_from_state(
        State(latitude=52.0, longitude=5.0), spec
    )
    assert coordinates == (52.0, 5.0)


def test_fixed_fallback_used_when_tracker_is_unavailable(targets_module):
    spec = targets_module.TargetSpec(
        "home", "Thuis", "zone.home", 51.0, 4.0, primary=True
    )
    assert targets_module.coordinates_from_state(None, spec) == (51.0, 4.0)


def test_duplicate_target_ids_are_rejected(targets_module):
    with pytest.raises(ValueError, match="Dubbel"):
        targets_module.build_target_specs(51.0, 4.0, [
            {"id": "a", "location_entity": "device_tracker.a"},
            {"id": "a", "location_entity": "device_tracker.b"},
        ])


def test_partial_fixed_coordinates_are_rejected(targets_module):
    with pytest.raises(ValueError, match="horen samen"):
        targets_module.build_target_specs(51.0, 4.0, [{
            "id": "a",
            "location_entity": "device_tracker.a",
            "latitude": 51.0,
        }])
