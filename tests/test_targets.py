"""Contracttests voor de backwards-compatible multi-targetconfiguratie."""

import pytest


class FakeState:
    def __init__(self, **attributes):
        self.attributes = attributes


def test_legacy_tracker_remains_primary_target(targets_module):
    specs = targets_module.build_target_specs(
        "device_tracker.fictieve_tracker", 51.0, 4.0
    )

    assert len(specs) == 1
    assert specs[0].target_id == "primary"
    assert specs[0].primary is True
    assert specs[0].fallback_lat == 51.0


def test_additional_person_gets_stable_sensor_suffix(targets_module):
    specs = targets_module.build_target_specs(
        "device_tracker.fictieve_tracker",
        51.0,
        4.0,
        [{
            "id": "Elke thuis",
            "name": "Elke",
            "location_entity": "person.elke",
        }],
    )

    assert len(specs) == 2
    assert specs[1].entity_suffix == "elke_thuis"
    assert specs[1].entity_id == "person.elke"
    assert specs[1].primary is False


def test_live_coordinates_override_fixed_fallback(targets_module):
    spec = targets_module.TargetSpec(
        "oma", "Oma", "device_tracker.oma", 51.0, 4.0
    )

    coordinates = targets_module.coordinates_from_state(
        FakeState(latitude=50.8, longitude=4.3), spec
    )

    assert coordinates == (50.8, 4.3)


def test_fixed_fallback_used_when_tracker_is_unavailable(targets_module):
    spec = targets_module.TargetSpec(
        "thuis", "Thuis", "device_tracker.thuis", 51.0, 4.0
    )
    assert targets_module.coordinates_from_state(None, spec) == (51.0, 4.0)


def test_duplicate_target_ids_are_rejected(targets_module):
    with pytest.raises(ValueError, match="target-id"):
        targets_module.build_target_specs(
            "device_tracker.fictieve_tracker",
            51.0,
            4.0,
            [
                {"id": "a", "location_entity": "person.a"},
                {"id": "a", "location_entity": "person.b"},
            ],
        )


def test_partial_fixed_coordinates_are_rejected(targets_module):
    with pytest.raises(ValueError, match="horen samen"):
        targets_module.build_target_specs(
            "device_tracker.fictieve_tracker",
            51.0,
            4.0,
            [{
                "id": "a",
                "location_entity": "person.a",
                "latitude": 51.0,
            }],
        )
