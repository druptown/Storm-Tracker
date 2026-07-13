"""Contracttests voor de echte dynamische RegionEngine-runtime."""


def test_nearby_targets_share_engine(region_manager_module):
    manager = region_manager_module.StormManager(
        sharing_distance_km=150, observation_radius_km=350
    )
    antwerp = manager.assign_target("home", 51.2194, 4.4025)
    brussels = manager.assign_target("person.a", 50.8503, 4.3517)

    assert brussels is antwerp
    assert antwerp.projection_targets == {"home", "person.a"}


def test_distant_target_creates_and_releases_second_engine(region_manager_module):
    removed = []
    manager = region_manager_module.StormManager(
        sharing_distance_km=150,
        observation_radius_km=350,
        on_engine_removed=lambda engine: removed.append(engine.engine_id),
    )
    belgium = manager.assign_target("home", 51.2194, 4.4025)
    brest = manager.assign_target("person.a", 48.3904, -4.4861)

    assert brest is not belgium
    assert len(manager.get_all_engines()) == 2
    manager.release("person.a")
    assert len(manager.get_all_engines()) == 1
    assert removed == [brest.engine_id]


def test_observation_radius_is_independent_from_sharing(region_manager_module):
    manager = region_manager_module.StormManager(
        sharing_distance_km=100, observation_radius_km=350
    )
    engine = manager.assign_target("home", 51.2194, 4.4025)

    assert engine.accepts_observation(52.3676, 4.9041)  # Amsterdam, ~135 km
    assert not engine.accepts_observation(48.3904, -4.4861)  # Brest, >350 km
