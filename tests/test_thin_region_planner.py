from __future__ import annotations

from storm_tracker_worker.desired_state import DesiredState, RuntimeSettings, Target
from storm_tracker_worker.thin_region_planner import ThinRegionPlanner


def _target(
    target_id: str,
    latitude: float,
    longitude: float,
    *,
    kind: str = "person",
    available: bool = True,
) -> Target:
    return Target(
        target_id=target_id,
        display_name=target_id.title(),
        kind=kind,
        available=available,
        latitude=latitude,
        longitude=longitude,
        position_observed_at=100.0 if available else None,
        location_accuracy_m=10.0,
        observation_radius_km=300.0,
        country_code="BE",
    )


def _state(*targets: Target) -> DesiredState:
    return DesiredState(
        producer_id="ha-nas",
        authority_epoch=1,
        generation=1,
        issued_at=100.0,
        previous_state_sha256=None,
        targets=targets,
        settings=RuntimeSettings(
            ground_radius_km=20.0,
            lightning_source_mode="direct",
            model_guidance_enabled=True,
            region_sharing_distance_km=150.0,
        ),
        canonical_bytes=b"{}",
        state_sha256="0" * 64,
    )


def test_home_anchor_and_distant_person_form_separate_regions():
    planner = ThinRegionPlanner()

    plan = planner.plan(
        _state(
            _target("home", 51.05, 4.42, kind="home"),
            _target("nearby", 51.20, 4.42),
            _target("remote", 50.20, 4.42),
        ),
        now=100.0,
    )

    assert len(plan.regions) == 2
    assert plan.region_id_for("home") == "region:home"
    assert plan.region_id_for("nearby") == "region:home"
    assert plan.region_id_for("remote").startswith("region:external:")
    home = next(region for region in plan.regions if region.region_id == "region:home")
    assert home.center_latitude == 51.05
    assert home.center_longitude == 4.42
    assert home.target_ids == ("home", "nearby")


def test_home_membership_uses_separate_exit_and_reentry_thresholds():
    planner = ThinRegionPlanner()
    home = _target("home", 51.0, 4.0, kind="home")

    first = planner.plan(_state(home, _target("person", 51.26, 4.0)), now=100.0)
    assert first.region_id_for("person") == "region:home"

    still_home = planner.plan(_state(home, _target("person", 51.30, 4.0)), now=200.0)
    assert still_home.region_id_for("person") == "region:home"

    outside = planner.plan(_state(home, _target("person", 51.33, 4.0)), now=300.0)
    assert outside.region_id_for("person").startswith("region:external:")

    still_outside = planner.plan(_state(home, _target("person", 51.27, 4.0)), now=400.0)
    assert still_outside.region_id_for("person").startswith("region:external:")

    returned = planner.plan(_state(home, _target("person", 51.21, 4.0)), now=500.0)
    assert returned.region_id_for("person") == "region:home"


def test_external_center_moves_only_after_recenter_threshold():
    planner = ThinRegionPlanner()
    home = _target("home", 51.0, 4.0, kind="home")

    initial = planner.plan(_state(home, _target("person", 48.0, 4.0)), now=100.0)
    region_id = initial.region_id_for("person")
    initial_region = initial.region(region_id)

    small_move = planner.plan(_state(home, _target("person", 48.09, 4.0)), now=200.0)
    assert small_move.region(region_id).center_latitude == initial_region.center_latitude

    large_move = planner.plan(_state(home, _target("person", 48.15, 4.0)), now=300.0)
    assert large_move.region(region_id).center_latitude == 48.15


def test_external_complete_link_clustering_does_not_join_a_distance_chain():
    planner = ThinRegionPlanner(external_sharing_distance_km=30.0)

    plan = planner.plan(
        _state(
            _target("a", 50.00, 4.0),
            _target("b", 50.18, 4.0),
            _target("c", 50.36, 4.0),
        ),
        now=100.0,
    )

    assert len(plan.regions) == 2
    assert plan.region_id_for("a") == plan.region_id_for("b")
    assert plan.region_id_for("c") != plan.region_id_for("b")


def test_unavailable_dynamic_target_keeps_last_region_for_bounded_grace():
    planner = ThinRegionPlanner(unavailable_grace_seconds=900.0)
    available = _target("person", 50.0, 4.0)
    unavailable = _target("person", 0.0, 0.0, available=False)

    planner.plan(_state(available), now=100.0)
    during_grace = planner.plan(_state(unavailable), now=999.0)

    assert during_grace.location_status_for("person") == "stale"
    assert during_grace.targets[0].latitude == 50.0
    assert during_grace.region_id_for("person") is not None

    expired = planner.plan(_state(unavailable), now=1001.0)
    assert expired.targets == ()
    assert expired.regions == ()
    assert expired.region_id_for("person") is None


def test_region_plan_accepts_only_positions_inside_a_warning_horizon():
    plan = ThinRegionPlanner().plan(
        _state(_target("home", 51.05, 4.42, kind="home")),
        now=100.0,
    )

    assert plan.accepts_position(51.50, 4.42) is True
    assert plan.accepts_position(48.86, 2.35) is False


def test_external_region_reuses_instance_until_it_disappears():
    planner = ThinRegionPlanner()
    home = _target("home", 51.0, 4.0, kind="home")

    first = planner.plan(_state(home, _target("remote", 48.0, 4.0)), now=100.0)
    first_region = first.region(first.region_id_for("remote"))
    moved = planner.plan(_state(home, _target("remote", 48.05, 4.0)), now=200.0)
    moved_region = moved.region(moved.region_id_for("remote"))

    assert moved_region.region_instance_id == first_region.region_instance_id
    assert moved_region.region_revision_id == first_region.region_revision_id


def test_external_region_reactivation_gets_new_instance():
    planner = ThinRegionPlanner()
    home = _target("home", 51.0, 4.0, kind="home")

    first = planner.plan(_state(home, _target("remote", 48.0, 4.0)), now=100.0)
    first_region = first.region(first.region_id_for("remote"))
    planner.plan(_state(home), now=200.0)
    returned = planner.plan(_state(home, _target("remote", 48.0, 4.0)), now=300.0)
    returned_region = returned.region(returned.region_id_for("remote"))

    assert returned_region.region_instance_id != first_region.region_instance_id


def test_recenter_creates_revision_inside_same_instance():
    planner = ThinRegionPlanner(recenter_distance_km=15.0)
    home = _target("home", 51.0, 4.0, kind="home")

    first = planner.plan(_state(home, _target("remote", 48.0, 4.0)), now=100.0)
    first_region = first.region(first.region_id_for("remote"))
    recentered = planner.plan(
        _state(home, _target("remote", 48.2, 4.0)),
        now=200.0,
    )
    recentered_region = recentered.region(recentered.region_id_for("remote"))

    assert recentered_region.region_instance_id == first_region.region_instance_id
    assert recentered_region.region_revision_id != first_region.region_revision_id
