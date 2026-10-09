"""Tests for immutable secondary WeatherSystem grouping."""
from __future__ import annotations

import ast
from dataclasses import FrozenInstanceError, replace
from pathlib import Path
import time

import pytest

from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid
from storm_tracker_worker.tracking_core.engine.weather_system_grouping import (
    WEATHER_SYSTEM_GROUPING_VERSION,
    WeatherSystemGroupingPolicy,
    WeatherSystemTrackInput,
    _footprint_gap_m,
    group_weather_systems,
)


ROOT = Path(__file__).resolve().parents[1]
HA_MODULE = (
    ROOT
    / "custom_components"
    / "storm_tracker_v3"
    / "engine"
    / "weather_system_grouping.py"
)
WORKER_MODULE = (
    ROOT
    / "storm_tracker_worker"
    / "tracking_core"
    / "engine"
    / "weather_system_grouping.py"
)


def _grid() -> AnalysisGrid:
    return AnalysisGrid(
        grid_id="synthetic-weather-system-grid",
        crs="EPSG:3035",
        origin_x_m=4_000_000.0,
        origin_y_m=3_100_000.0,
        x_resolution_m=1_000.0,
        y_resolution_m=1_000.0,
        width=80,
        height=60,
    )


def _policy() -> WeatherSystemGroupingPolicy:
    return WeatherSystemGroupingPolicy(
        policy_id="synthetic-explicit-weather-system-policy",
        pixel_connectivity=8,
        maximum_footprint_gap_m=2_000.0,
        maximum_motion_difference_m_s=5.0,
        maximum_motion_age_s=600.0,
        maximum_motion_time_difference_s=300.0,
    )


def _track(
    track_id: str,
    *,
    valid_at: float,
    footprint: tuple[tuple[int, int], ...],
    detection_id: str | None = None,
    kind: str = "rain_shield",
    shield_ids: tuple[str, ...] | None = None,
    vx_m_s: float | None = 10.0,
    vy_m_s: float | None = 0.0,
    motion_reference_valid_at: float | None = None,
    parent_track_ids: tuple[str, ...] = (),
    child_track_ids: tuple[str, ...] = (),
    lineage_event_ids: tuple[str, ...] = (),
    identity_alternative_track_ids: tuple[str, ...] = (),
) -> WeatherSystemTrackInput:
    selected_detection = detection_id or f"detection:{track_id}"
    if shield_ids is None:
        shield_ids = (
            (selected_detection,)
            if kind == "rain_shield"
            else ("shield:synthetic-parent",)
        )
    row, column = footprint[0]
    x_m, y_m = _grid().pixel_center(row, column)
    if vx_m_s is None:
        vy_m_s = None
        motion_reference_valid_at = None
        motion_method = None
    else:
        motion_reference_valid_at = (
            valid_at
            if motion_reference_valid_at is None
            else motion_reference_valid_at
        )
        motion_method = "synthetic-track-motion"
    return WeatherSystemTrackInput(
        track_snapshot_id=f"snapshot:{track_id}:{valid_at}",
        track_id=track_id,
        analysis_grid_id=_grid().grid_id,
        valid_at=valid_at,
        current_detection_id=selected_detection,
        object_kind=kind,
        current_footprint_pixels=footprint,
        rain_shield_detection_ids=shield_ids,
        x_m=x_m,
        y_m=y_m,
        vx_m_s=vx_m_s,
        vy_m_s=vy_m_s,
        motion_reference_valid_at=motion_reference_valid_at,
        motion_method=motion_method,
        lifecycle_state="confirmed",
        parent_track_ids=parent_track_ids,
        child_track_ids=child_track_ids,
        lineage_event_ids=lineage_event_ids,
        identity_alternative_track_ids=identity_alternative_track_ids,
    )


def _group(
    valid_at: float,
    tracks,
    *,
    previous=None,
):
    return group_weather_systems(
        valid_at=valid_at,
        grid=_grid(),
        tracks=tracks,
        policy=_policy(),
        previous=previous,
    )


def test_footprint_gap_stays_bounded_for_large_radar_footprints():
    """Catch a return to the quadratic all-pixel-pairs implementation."""

    grid = AnalysisGrid(
        grid_id="large-footprint-gap-grid",
        crs="EPSG:3035",
        origin_x_m=0.0,
        origin_y_m=100_000.0,
        x_resolution_m=1_000.0,
        y_resolution_m=1_000.0,
        width=100,
        height=100,
    )
    left = {
        (row, column)
        for row in range(30)
        for column in range(30)
    }
    right = {
        (row, column)
        for row in range(35, 65)
        for column in range(40, 70)
    }

    started = time.monotonic()
    gap = _footprint_gap_m(left, right, grid)
    elapsed = time.monotonic() - started

    assert gap == pytest.approx(11_180.339887498949)
    assert elapsed < 0.1


def test_shared_rain_shield_keeps_puppies_without_overwriting_local_motion():
    valid_at = 1_000.0
    shield_detection = "shield:central"
    shield = _track(
        "track:shield",
        valid_at=valid_at,
        detection_id=shield_detection,
        footprint=((10, 10), (10, 11), (11, 10), (11, 11)),
        vx_m_s=12.0,
        vy_m_s=-3.0,
    )
    puppy_a = _track(
        "track:puppy-a",
        valid_at=valid_at,
        footprint=((10, 10),),
        kind="embedded_core",
        shield_ids=(shield_detection,),
        vx_m_s=1_000_000.0,
        vy_m_s=-2_000_000.0,
    )
    puppy_b = _track(
        "track:puppy-b",
        valid_at=valid_at,
        footprint=((11, 11),),
        kind="embedded_core",
        shield_ids=(shield_detection,),
        vx_m_s=-1_000_000.0,
        vy_m_s=2_000_000.0,
    )

    result = _group(valid_at, (puppy_b, shield, puppy_a))

    assert len(result.systems) == 1
    system = result.systems[0]
    assert system.member_track_ids == (
        "track:puppy-a",
        "track:puppy-b",
        "track:shield",
    )
    assert system.aggregate_current_footprint == (
        (10, 10),
        (10, 11),
        (11, 10),
        (11, 11),
    )
    assert system.descriptive_motion.vx_m_s == 12.0
    assert system.descriptive_motion.vy_m_s == -3.0
    assert system.descriptive_motion.supporting_track_ids == ("track:shield",)
    assert system.descriptive_motion.method == (
        "componentwise_median_current_rain_shield_motion"
    )
    assert puppy_a.vx_m_s == 1_000_000.0
    assert puppy_b.vx_m_s == -1_000_000.0
    assert all(
        item.decision == "connected"
        for item in result.connections
        if "track:shield" in (item.left_track_id, item.right_track_id)
    )
    with pytest.raises(FrozenInstanceError):
        system.system_id = "mutated"  # type: ignore[misc]


def test_puppy_rearrangement_does_not_jump_system_identity_or_motion():
    first_time = 2_000.0
    shield_detection = "shield:dog-leash"
    first_tracks = (
        _track(
            "track:shield",
            valid_at=first_time,
            detection_id=shield_detection,
            footprint=((20, 20), (20, 21), (21, 20), (21, 21)),
            vx_m_s=8.0,
            vy_m_s=2.0,
        ),
        _track(
            "track:puppy-a",
            valid_at=first_time,
            footprint=((20, 20),),
            kind="embedded_core",
            shield_ids=(shield_detection,),
            vx_m_s=80.0,
            vy_m_s=-50.0,
        ),
        _track(
            "track:puppy-b",
            valid_at=first_time,
            footprint=((21, 21),),
            kind="embedded_core",
            shield_ids=(shield_detection,),
            vx_m_s=-90.0,
            vy_m_s=60.0,
        ),
    )
    first = _group(first_time, first_tracks)
    second_time = first_time + 300.0
    second_tracks = (
        _track(
            "track:puppy-b",
            valid_at=second_time,
            footprint=((20, 21),),
            kind="embedded_core",
            shield_ids=(shield_detection,),
            vx_m_s=400_000.0,
            vy_m_s=-500_000.0,
        ),
        _track(
            "track:shield",
            valid_at=second_time,
            detection_id=shield_detection,
            footprint=((20, 20), (20, 21), (21, 20), (21, 21)),
            vx_m_s=8.0,
            vy_m_s=2.0,
        ),
        _track(
            "track:puppy-a",
            valid_at=second_time,
            footprint=((21, 20),),
            kind="embedded_core",
            shield_ids=(shield_detection,),
            vx_m_s=-600_000.0,
            vy_m_s=700_000.0,
        ),
    )

    second = _group(second_time, second_tracks, previous=first)

    assert second.systems[0].system_id == first.systems[0].system_id
    assert second.systems[0].descriptive_motion.vx_m_s == 8.0
    assert second.systems[0].descriptive_motion.vy_m_s == 2.0
    assert second.systems[0].aggregate_current_footprint == (
        (20, 20),
        (20, 21),
        (21, 20),
        (21, 21),
    )
    assert second.membership_events[0].event_type == "continued"


def test_member_replacement_keeps_system_identity_and_audits_added_removed():
    first_time = 2_500.0
    shield_detection = "shield:stable-anchor"
    first = _group(
        first_time,
        (
            _track(
                "track:shield",
                valid_at=first_time,
                detection_id=shield_detection,
                footprint=((12, 12), (12, 13)),
            ),
            _track(
                "track:old-puppy",
                valid_at=first_time,
                footprint=((12, 12),),
                kind="embedded_core",
                shield_ids=(shield_detection,),
                lineage_event_ids=("lineage:old",),
            ),
        ),
    )
    second_time = first_time + 300.0
    second = _group(
        second_time,
        (
            _track(
                "track:new-puppy",
                valid_at=second_time,
                footprint=((12, 13),),
                kind="embedded_core",
                shield_ids=(shield_detection,),
                parent_track_ids=("track:old-puppy",),
                lineage_event_ids=("lineage:replacement",),
            ),
            _track(
                "track:shield",
                valid_at=second_time,
                detection_id=shield_detection,
                footprint=((12, 12), (12, 13)),
            ),
        ),
        previous=first,
    )

    assert second.systems[0].system_id == first.systems[0].system_id
    event = second.membership_events[0]
    assert event.event_type == "membership_changed"
    assert event.added_track_ids == ("track:new-puppy",)
    assert event.removed_track_ids == ("track:old-puppy",)
    assert event.member_lineage_event_ids == ("lineage:replacement",)


def test_near_tracks_with_unknown_motion_are_explicitly_uncertain_not_merged():
    valid_at = 3_000.0
    left = _track(
        "track:left",
        valid_at=valid_at,
        footprint=((5, 5),),
        vx_m_s=None,
    )
    right = _track(
        "track:right",
        valid_at=valid_at,
        footprint=((5, 7),),
        vx_m_s=10.0,
    )

    result = _group(valid_at, (left, right))

    assert len(result.systems) == 2
    assert {
        item for system in result.systems for item in system.member_track_ids
    } == {"track:left", "track:right"}
    connection = result.connections[0]
    assert connection.spatial_relation == "near"
    assert connection.motion_coherence == "unknown"
    assert connection.decision == "uncertain"
    assert connection.connection_id in result.systems[0].uncertain_connection_ids
    assert connection.connection_id in result.systems[1].uncertain_connection_ids


def test_multiple_uncertain_edges_are_retained_canonically_for_every_system():
    valid_at = 3_500.0
    tracks = tuple(
        _track(
            f"track:uncertain-{index}",
            valid_at=valid_at,
            footprint=((5, 10 + index),),
            vx_m_s=None,
        )
        for index in range(3)
    )

    result = _group(valid_at, reversed(tracks))

    assert len(result.systems) == 3
    assert len(result.connections) == 3
    assert all(item.decision == "uncertain" for item in result.connections)
    assert all(
        system.uncertain_connection_ids
        == tuple(sorted(system.uncertain_connection_ids))
        for system in result.systems
    )
    assert all(len(system.uncertain_connection_ids) == 2 for system in result.systems)


def test_current_overlap_or_distance_requires_explicit_motion_coherence():
    valid_at = 4_000.0
    coherent_left = _track(
        "track:a",
        valid_at=valid_at,
        footprint=((6, 6),),
        vx_m_s=100_000.0,
        vy_m_s=-200_000.0,
    )
    coherent_right = _track(
        "track:b",
        valid_at=valid_at,
        footprint=((6, 8),),
        vx_m_s=100_002.0,
        vy_m_s=-200_001.0,
    )
    coherent = _group(valid_at, (coherent_left, coherent_right))
    assert coherent.connections[0].decision == "connected"
    assert len(coherent.systems) == 1
    assert coherent.systems[0].descriptive_motion.vx_m_s == 100_001.0

    incoherent_right = replace(
        coherent_right,
        vx_m_s=-100_000.0,
        track_snapshot_id="snapshot:track:b:incoherent",
    )
    incoherent = _group(valid_at, (coherent_left, incoherent_right))
    assert incoherent.connections[0].motion_coherence == "incoherent"
    assert incoherent.connections[0].decision == "uncertain"
    assert len(incoherent.systems) == 2


def test_motion_reference_time_and_overlap_are_explicit_connection_evidence():
    valid_at = 4_500.0
    left = _track(
        "track:time-left",
        valid_at=valid_at,
        footprint=((7, 7), (7, 8)),
        vx_m_s=4.0,
        vy_m_s=1.0,
        motion_reference_valid_at=valid_at - 900.0,
    )
    right = _track(
        "track:time-right",
        valid_at=valid_at,
        footprint=((7, 8), (7, 9)),
        vx_m_s=4.0,
        vy_m_s=1.0,
        motion_reference_valid_at=valid_at,
    )

    result = _group(valid_at, (left, right))

    connection = result.connections[0]
    assert connection.overlap_pixel_count == 1
    assert connection.spatial_relation == "overlap"
    assert connection.valid_time_difference_s == 0.0
    assert connection.motion_time_difference_s == 900.0
    assert connection.motion_coherence == "stale"
    assert connection.decision == "uncertain"
    assert len(result.systems) == 2


def test_aggregate_footprint_is_current_union_never_historical_hull():
    first_time = 5_000.0
    first_track = _track(
        "track:moving",
        valid_at=first_time,
        footprint=((1, 1), (1, 2)),
    )
    first = _group(first_time, (first_track,))
    second_time = first_time + 300.0
    current_track = _track(
        "track:moving",
        valid_at=second_time,
        footprint=((40, 40), (40, 41)),
    )

    second = _group(second_time, (current_track,), previous=first)

    assert second.systems[0].aggregate_current_footprint == (
        (40, 40),
        (40, 41),
    )
    assert (1, 1) not in second.systems[0].aggregate_current_footprint
    assert len(second.systems[0].aggregate_current_footprint) == 2


def test_system_merge_and_split_preserve_every_track_lineage_and_alternative():
    first_time = 6_000.0
    first_a = _track(
        "track:a",
        valid_at=first_time,
        footprint=((8, 8),),
        lineage_event_ids=("lineage:birth-a",),
        identity_alternative_track_ids=("track:a-alt",),
    )
    first_b = _track(
        "track:b",
        valid_at=first_time,
        footprint=((8, 30),),
        lineage_event_ids=("lineage:birth-b",),
        identity_alternative_track_ids=("track:b-alt",),
    )
    first = _group(first_time, (first_a, first_b))
    assert len(first.systems) == 2

    merge_time = first_time + 300.0
    merged_a = _track(
        "track:a",
        valid_at=merge_time,
        footprint=((8, 15),),
        lineage_event_ids=("lineage:birth-a", "lineage:merge"),
        identity_alternative_track_ids=("track:a-alt",),
    )
    merged_b = _track(
        "track:b",
        valid_at=merge_time,
        footprint=((8, 17),),
        lineage_event_ids=("lineage:birth-b", "lineage:merge"),
        identity_alternative_track_ids=("track:b-alt",),
    )
    merged = _group(merge_time, (merged_b, merged_a), previous=first)

    assert len(merged.systems) == 1
    assert merged.systems[0].member_track_ids == ("track:a", "track:b")
    merge_event = merged.membership_events[0]
    assert merge_event.event_type == "merge"
    assert len(merge_event.predecessor_system_ids) == 2
    assert len(merge_event.alternative_predecessor_system_ids) == 1
    assert merge_event.member_lineage_event_ids == (
        "lineage:birth-a",
        "lineage:birth-b",
        "lineage:merge",
    )
    assert merge_event.member_identity_alternative_track_ids == (
        "track:a-alt",
        "track:b-alt",
    )
    assert len(merged.retired_system_ids) == 1

    split_time = merge_time + 300.0
    split_a = _track(
        "track:a",
        valid_at=split_time,
        footprint=((8, 8),),
        parent_track_ids=("old:a",),
        lineage_event_ids=("lineage:split",),
        identity_alternative_track_ids=("track:a-alt",),
    )
    split_b = _track(
        "track:b",
        valid_at=split_time,
        footprint=((8, 30),),
        parent_track_ids=("old:b",),
        lineage_event_ids=("lineage:split",),
        identity_alternative_track_ids=("track:b-alt",),
    )
    split = _group(split_time, (split_a, split_b), previous=merged)

    assert len(split.systems) == 2
    assert sorted(
        event.event_type for event in split.membership_events
    ) == ["split_child", "split_retained"]
    assert {
        item for system in split.systems for item in system.member_track_ids
    } == {"track:a", "track:b"}
    assert all(
        event.predecessor_system_ids == (merged.systems[0].system_id,)
        for event in split.membership_events
    )


def test_more_than_twenty_tracks_are_all_preserved_without_count_or_speed_cap():
    valid_at = 7_000.0
    shield_detection = "shield:large-system"
    shield = _track(
        "track:shield",
        valid_at=valid_at,
        detection_id=shield_detection,
        footprint=((30, 30), (30, 31), (31, 30), (31, 31)),
        vx_m_s=2_000_000_000.0,
        vy_m_s=-3_000_000_000.0,
    )
    puppies = tuple(
        _track(
            f"track:puppy-{index:02d}",
            valid_at=valid_at,
            footprint=((30 + index % 2, 30 + index % 2),),
            kind="embedded_core",
            shield_ids=(shield_detection,),
            vx_m_s=float(index * 100_000_000),
            vy_m_s=float(-index * 200_000_000),
        )
        for index in range(24)
    )

    result = _group(valid_at, tuple(reversed(puppies)) + (shield,))

    assert len(result.input_tracks) == 25
    assert len(result.systems) == 1
    assert len(result.systems[0].member_track_ids) == 25
    assert len(result.connections) == 25 * 24 // 2
    assert result.systems[0].descriptive_motion.vx_m_s == 2_000_000_000.0
    assert result.systems[0].descriptive_motion.vy_m_s == -3_000_000_000.0


def test_input_order_never_changes_grouping_or_identifiers():
    valid_at = 8_000.0
    tracks = (
        _track("track:c", valid_at=valid_at, footprint=((2, 6),)),
        _track("track:a", valid_at=valid_at, footprint=((2, 2),)),
        _track("track:b", valid_at=valid_at, footprint=((2, 4),)),
    )

    first = _group(valid_at, tracks)
    second = _group(valid_at, reversed(tracks))

    assert first == second
    assert first.grouping_snapshot_id == second.grouping_snapshot_id


def test_current_time_grid_and_motion_provenance_fail_closed():
    valid_at = 9_000.0
    current = _track(
        "track:current",
        valid_at=valid_at,
        footprint=((3, 3),),
    )
    stale_footprint = _track(
        "track:stale",
        valid_at=valid_at - 300.0,
        footprint=((3, 4),),
    )
    with pytest.raises(ValueError, match="exact hetzelfde valid_at"):
        _group(valid_at, (current, stale_footprint))

    wrong_grid = replace(current, analysis_grid_id="other-grid")
    with pytest.raises(ValueError, match="hetzelfde grid"):
        _group(valid_at, (wrong_grid,))

    with pytest.raises(ValueError, match="future"):
        replace(
            current,
            motion_reference_valid_at=valid_at + 1.0,
        )


def test_empty_current_frame_is_a_valid_zero_system_snapshot():
    result = _group(10_000.0, ())

    assert result.input_tracks == ()
    assert result.systems == ()
    assert result.connections == ()
    assert result.membership_events == ()


def test_policy_has_no_hidden_scientific_defaults():
    with pytest.raises(TypeError):
        WeatherSystemGroupingPolicy(  # type: ignore[call-arg]
            policy_id="missing-explicit-limits",
        )
    with pytest.raises(ValueError, match="algoritmeversie"):
        replace(
            _policy(),
            algorithm_version="unknown-weather-system-v2",
        )
    assert _policy().algorithm_version == WEATHER_SYSTEM_GROUPING_VERSION
    changed = replace(_policy(), maximum_footprint_gap_m=2_001.0)
    assert changed.fingerprint != _policy().fingerprint
    empty_default = group_weather_systems(
        valid_at=10_100.0,
        grid=_grid(),
        tracks=(),
        policy=_policy(),
    )
    empty_changed = group_weather_systems(
        valid_at=10_100.0,
        grid=_grid(),
        tracks=(),
        policy=changed,
    )
    assert empty_default.grouping_snapshot_id != (
        empty_changed.grouping_snapshot_id
    )


def test_ha_worker_modules_are_exact_mirrors_and_shadow_only():
    assert HA_MODULE.read_bytes() == WORKER_MODULE.read_bytes()
    tree = ast.parse(HA_MODULE.read_text(encoding="utf-8"))
    imported_modules = {
        node.module or "__future__"
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    } | {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    forbidden = {
        "homeassistant",
        "storm_engine",
        "storm",
        "service",
        "sensor",
        "mqtt",
    }
    assert not any(
        any(part == blocked for part in name.split("."))
        for name in imported_modules
        for blocked in forbidden
    )
    assert {
        "dataclasses",
        "hashlib",
        "math",
        "statistics",
        "typing",
        "analysis_grid",
        "__future__",
    } >= {name.rsplit(".", 1)[-1] for name in imported_modules}

    live_paths = (
        ROOT / "custom_components" / "storm_tracker_v3" / "__init__.py",
        ROOT / "custom_components" / "storm_tracker_v3" / "sensor.py",
        ROOT / "storm_tracker_worker" / "service.py",
    )
    for path in live_paths:
        assert "weather_system_grouping" not in path.read_text(encoding="utf-8")
