"""Storm Tracker V3 — tests/test_storm_engine.py v0.1.0

Tests voor engine/storm_engine.py — de kern van de architectuur
(clustering, merge, lifecycle, regressie) en tot nu toe volledig
ongetest, ondanks dat dit precies de laag is waar Blokker 1's zorg
("verre cellen vullen de lokale stormlimiet") zich zou manifesteren.
"""
from __future__ import annotations

import asyncio
from pathlib import Path
import time

import pytest


def _obs(observation_module, obs_type, lat, lon, ts=None, **kw):
    return observation_module.Observation(
        obs_type=obs_type, lat=lat, lon=lon,
        timestamp=ts if ts is not None else time.time(),
        **kw,
    )


# ── Clustering: nieuwe storm vs. toewijzing aan bestaande ─────────────────

def test_first_lightning_observation_creates_a_storm(storm_engine_module, observation_module):
    engine = storm_engine_module.StormEngine()
    obs = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.026, 4.478)
    asyncio.run(engine.process_batch([obs]))

    storms = engine.get_storms()
    assert len(storms) == 1
    assert storms[0].centroid_lat == pytest.approx(51.026)
    assert storms[0].strike_count == 1


def test_nearby_lightning_joins_existing_storm(storm_engine_module, observation_module):
    engine = storm_engine_module.StormEngine(cluster_radius_km=30.0)
    o1 = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.026, 4.478)
    o2 = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.030, 4.480)  # ~0.5km verderop
    asyncio.run(engine.process_batch([o1]))
    asyncio.run(engine.process_batch([o2]))

    storms = engine.get_storms()
    assert len(storms) == 1, "een strike binnen de cluster-radius moet bij dezelfde storm horen"
    assert storms[0].strike_count == 2


def test_far_lightning_creates_a_second_storm(storm_engine_module, observation_module):
    engine = storm_engine_module.StormEngine(cluster_radius_km=30.0)
    o1 = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.026, 4.478)  # Heffen
    o2 = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 41.0, 2.0)       # Barcelona
    asyncio.run(engine.process_batch([o1]))
    asyncio.run(engine.process_batch([o2]))

    storms = engine.get_storms()
    assert len(storms) == 2, "twee ver uit elkaar liggende strikes horen twee storms te worden"


def test_retain_within_removes_systems_from_previous_region(
    storm_engine_module, observation_module
):
    engine = storm_engine_module.StormEngine(cluster_radius_km=10.0)
    brest = _obs(
        observation_module,
        observation_module.ObservationType.LIGHTNING,
        48.3904,
        -4.4861,
    )
    bordeaux = _obs(
        observation_module,
        observation_module.ObservationType.LIGHTNING,
        44.8378,
        -0.5792,
    )
    asyncio.run(engine.process_batch([brest, bordeaux]))

    removed = engine.retain_within(48.3904, -4.4861, 350.0)
    storms = engine.get_storms()

    assert removed == 1
    assert len(storms) == 1
    assert storms[0].centroid_lat == pytest.approx(48.3904)
    assert set(engine._history) == {storms[0].storm_id}


# ── Capaciteitsrichtwaarde mag nooit observaties weggooien ──────────────────

def test_storm_budget_never_drops_lightning_observations(
    storm_engine_module, observation_module
):
    """
    max_storms blijft een capaciteitsrichtwaarde voor compatibiliteit, niet
    langer een detectiefilter. Verre echte waarnemingen moeten dus allemaal
    als afzonderlijk systeem behouden blijven.
    """
    engine = storm_engine_module.StormEngine(cluster_radius_km=5.0, max_storms=3)
    locations = [(51.0, 4.0), (52.0, 5.0), (53.0, 6.0), (54.0, 7.0), (55.0, 8.0)]
    for lat, lon in locations:
        obs = _obs(observation_module, observation_module.ObservationType.LIGHTNING, lat, lon)
        asyncio.run(engine.process_batch([obs]))

    assert len(engine.get_storms()) == len(locations)


def test_storm_budget_never_drops_radar_cells(
    storm_engine_module, observation_module
):
    """Ook radarcellen boven de richtwaarde blijven operationeel behouden."""
    engine = storm_engine_module.StormEngine(cluster_radius_km=5.0, max_storms=2)
    locations = [(51.0, 4.0), (52.0, 5.0), (53.0, 6.0), (54.0, 7.0)]
    for lat, lon in locations:
        obs = _obs(observation_module, observation_module.ObservationType.RADAR, lat, lon, intensity=5)
        asyncio.run(engine.process_batch([obs]))

    assert len(engine.get_storms()) == len(locations)


def test_target_relevant_sixteenth_storm_is_scheduled_first_without_eviction(
    storm_engine_module, observation_module
):
    """De legacywaarde 15 is advies; een 16e targetbui gaat vóór verre ruis."""
    engine = storm_engine_module.StormEngine(
        cluster_radius_km=2.0,
        max_storms=15,
    )
    engine._last_merge_check = time.time()
    timestamp = time.time()
    observations = []
    for index in range(15):
        observations.append(_obs(
            observation_module,
            observation_module.ObservationType.RADAR,
            -70.0 + index * 6.0,
            -160.0 + index * 20.0,
            ts=timestamp,
            intensity=1,
            radar_cell_id=f"noise-cell-{index}",
            parent_system_id=f"noise-system-{index}",
        ))
    observations.append(_obs(
        observation_module,
        observation_module.ObservationType.RADAR,
        51.0,
        4.0,
        ts=timestamp,
        intensity=1,
        radar_cell_id="target-cell-16",
        parent_system_id="target-system-16",
    ))

    asyncio.run(engine.process_batch(observations))
    target_storm = next(
        storm for storm in engine.get_storms()
        if "target-system-16" in storm.source_system_ids
    )
    diagnostics = engine.build_compute_schedule(
        {"home": (51.0, 4.0)},
        relevance_radius_km=25.0,
    )

    assert len(engine.get_storms()) == 16
    assert len(engine.export_tracking_state()) == 16
    assert set(engine._history) == {
        storm.storm_id for storm in engine.get_storms()
    }
    assert diagnostics["max_storms_role"] == "scheduling_guideline_only"
    assert diagnostics["tracked_count"] == 16
    assert diagnostics["scheduled_track_count"] == 16
    assert diagnostics["exported_track_count"] == 16
    assert diagnostics["forecast_input_count"] == 16
    assert diagnostics["schedule"][0]["storm_id"] == target_storm.storm_id
    assert diagnostics["schedule"][0]["tier"] == "relevant"
    assert diagnostics["schedule"][0]["rank"] == 1
    assert diagnostics["schedule"][0]["preferred_pass"] == 0
    assert diagnostics["deferred_advisory_count"] == 1
    assert all(diagnostics["invariants"].values())
    assert all(
        entry["execution_required"]
        and entry["tracking_state_retained"]
        and entry["forecast_input_retained"]
        for entry in diagnostics["schedule"]
    )


def test_compute_tier_is_recomputed_when_target_moves_without_state_mutation(
    storm_engine_module, observation_module
):
    """Background is geen opslagstatus en kan bij een nieuw target relevant zijn."""
    engine = storm_engine_module.StormEngine(
        cluster_radius_km=2.0,
        max_storms=1,
    )
    engine._last_merge_check = time.time()
    timestamp = time.time()
    asyncio.run(engine.process_batch([
        _obs(
            observation_module,
            observation_module.ObservationType.RADAR,
            51.0,
            4.0,
            ts=timestamp,
            radar_cell_id="west",
            parent_system_id="west",
        ),
        _obs(
            observation_module,
            observation_module.ObservationType.RADAR,
            51.0,
            8.0,
            ts=timestamp,
            radar_cell_id="east",
            parent_system_id="east",
        ),
    ]))
    west = min(engine.get_storms(), key=lambda storm: storm.centroid_lon)
    east = max(engine.get_storms(), key=lambda storm: storm.centroid_lon)
    west.confidence = "Matig"
    east.confidence = "Matig"
    state_before = engine.export_tracking_state()

    west_plan = engine.build_compute_schedule(
        {"tracker": (51.0, 4.0)},
        relevance_radius_km=25.0,
    )
    east_plan = engine.build_compute_schedule(
        {"tracker": (51.0, 8.0)},
        relevance_radius_km=25.0,
    )

    west_tiers = {
        entry["storm_id"]: entry["tier"] for entry in west_plan["schedule"]
    }
    east_tiers = {
        entry["storm_id"]: entry["tier"] for entry in east_plan["schedule"]
    }
    assert west_tiers == {
        west.storm_id: "relevant",
        east.storm_id: "background",
    }
    assert east_tiers == {
        west.storm_id: "background",
        east.storm_id: "relevant",
    }
    assert west_plan["schedule"][0]["storm_id"] == west.storm_id
    assert east_plan["schedule"][0]["storm_id"] == east.storm_id
    assert west_plan["schedule_fingerprint"] != east_plan[
        "schedule_fingerprint"
    ]
    assert engine.export_tracking_state() == state_before


def test_p0_l03_deterministic_peak_resource_microreplay(
    storm_engine_module, observation_module, monkeypatch
):
    """Deterministische 96-objectreplay; geen wall-clock-, RAM- of NUC-claim."""
    fixed_timestamp = 1_800_000_000.0
    monkeypatch.setattr(
        storm_engine_module.time,
        "time",
        lambda: fixed_timestamp,
    )
    engine = storm_engine_module.StormEngine(
        cluster_radius_km=2.0,
        max_storms=15,
    )
    engine._last_merge_check = time.time()
    timestamp = fixed_timestamp
    observations = []
    for index in range(96):
        if index < 24:
            lat = 49.5 + (index // 6) * 0.4
            lon = 3.0 + (index % 6) * 0.4
            prefix = "relevant"
        else:
            noise_index = index - 24
            lat = -60.0 + (noise_index // 12) * 6.0
            lon = -150.0 + (noise_index % 12) * 25.0
            prefix = "noise"
        observations.append(_obs(
            observation_module,
            observation_module.ObservationType.RADAR,
            lat,
            lon,
            ts=timestamp,
            intensity=index % 8,
            radar_cell_id=f"{prefix}-cell-{index:03d}",
            parent_system_id=f"{prefix}-system-{index:03d}",
        ))

    asyncio.run(engine.process_batch(observations))
    first = engine.build_compute_schedule(
        {"home": (51.0, 4.0), "east": (50.0, 5.0)},
        relevance_radius_km=300.0,
    )
    reordered_targets = engine.build_compute_schedule(
        {"east": (50.0, 5.0), "home": (51.0, 4.0)},
        relevance_radius_km=300.0,
    )

    assert first == reordered_targets
    assert first["tracked_count"] == 96
    assert first["history_track_count"] == 96
    assert first["exported_track_count"] == 96
    assert first["scheduled_track_count"] == 96
    assert first["track_target_pairs_evaluated"] == 192
    assert first["preferred_foreground_count"] == 15
    assert first["preferred_pass_count"] == 7
    assert first["tier_counts"]["relevant"] == 24
    assert first["schedule_serialized_bytes"] < 96 * 1024
    relevant_ranks = [
        entry["rank"] for entry in first["schedule"]
        if entry["tier"] == "relevant"
    ]
    other_ranks = [
        entry["rank"] for entry in first["schedule"]
        if entry["tier"] != "relevant"
    ]
    assert max(relevant_ranks) < min(other_ranks)
    assert all(first["invariants"].values())
    assert first["limitations"] == {
        "wall_clock_benchmark": False,
        "process_memory_measurement": False,
        "nuc_thermal_claim": False,
        "meteorological_probability_claim": False,
    }


def test_worker_storm_engine_is_exact_byte_mirror():
    root = Path(__file__).resolve().parents[1]
    integration = (
        root / "custom_components/storm_tracker_v3/engine/storm_engine.py"
    )
    worker = root / "storm_tracker_worker/tracking_core/engine/storm_engine.py"

    assert worker.read_bytes() == integration.read_bytes()


def test_invalid_worker_snapshot_cannot_erase_current_tracking_state(
    storm_engine_module, observation_module
):
    engine = storm_engine_module.StormEngine()
    observation = _obs(
        observation_module,
        observation_module.ObservationType.LIGHTNING,
        51.0,
        4.0,
        intensity=12,
    )
    asyncio.run(engine.process_batch([observation]))
    original_ids = [storm.storm_id for storm in engine.get_storms()]

    with pytest.raises(ValueError, match="ongeldige of verlopen"):
        engine.replace_tracking_state([{"storm_id": "onvolledig"}])

    assert [storm.storm_id for storm in engine.get_storms()] == original_ids


def test_duplicate_batch_is_noop_before_revision_conflict(
    storm_engine_module, observation_module
):
    engine = storm_engine_module.StormEngine()
    observation = _obs(
        observation_module,
        observation_module.ObservationType.RADAR,
        51.0,
        4.0,
        intensity=12,
    )
    batch_id = "a" * 32

    committed = asyncio.run(engine.process_batch(
        [observation],
        expected_revision=0,
        batch_id=batch_id,
    ))
    state_after_commit = engine.export_tracking_state()

    duplicate = asyncio.run(engine.process_batch(
        [observation],
        expected_revision=0,
        batch_id=batch_id,
    ))

    assert committed is True
    assert duplicate is False
    assert engine.revision == 1
    assert engine.export_tracking_state() == state_after_commit


def test_stale_worker_replace_cannot_overwrite_newer_batch(
    storm_engine_module, observation_module
):
    engine = storm_engine_module.StormEngine(cluster_radius_km=10.0)
    first = _obs(
        observation_module,
        observation_module.ObservationType.RADAR,
        51.0,
        4.0,
        intensity=12,
    )
    asyncio.run(engine.process_batch(
        [first],
        expected_revision=0,
        batch_id="1" * 32,
    ))
    stale_worker_state = engine.export_tracking_state()

    newer = _obs(
        observation_module,
        observation_module.ObservationType.RADAR,
        55.0,
        9.0,
        intensity=8,
    )
    asyncio.run(engine.process_batch(
        [newer],
        expected_revision=1,
        batch_id="2" * 32,
    ))
    current_state = engine.export_tracking_state()

    with pytest.raises(storm_engine_module.TrackingRevisionConflict):
        engine.replace_tracking_state(
            stale_worker_state,
            expected_revision=1,
            batch_id="3" * 32,
        )

    assert engine.revision == 2
    assert engine.export_tracking_state() == current_state


def test_stale_worker_replace_cannot_undo_retain_within(
    storm_engine_module, observation_module
):
    engine = storm_engine_module.StormEngine(cluster_radius_km=10.0)
    observations = [
        _obs(
            observation_module,
            observation_module.ObservationType.RADAR,
            51.0,
            4.0,
            intensity=12,
        ),
        _obs(
            observation_module,
            observation_module.ObservationType.RADAR,
            60.0,
            10.0,
            intensity=8,
        ),
    ]
    asyncio.run(engine.process_batch(
        observations,
        expected_revision=0,
        batch_id="4" * 32,
    ))
    stale_worker_state = engine.export_tracking_state()

    assert engine.retain_within(51.0, 4.0, 150.0) == 1
    state_after_retain = engine.export_tracking_state()

    with pytest.raises(storm_engine_module.TrackingRevisionConflict):
        engine.replace_tracking_state(
            stale_worker_state,
            expected_revision=1,
            batch_id="5" * 32,
        )

    assert engine.revision == 2
    assert engine.export_tracking_state() == state_after_retain


def test_invalid_replace_does_not_consume_revision_or_receipt(
    storm_engine_module, observation_module
):
    engine = storm_engine_module.StormEngine()
    observation = _obs(
        observation_module,
        observation_module.ObservationType.LIGHTNING,
        51.0,
        4.0,
        intensity=12,
    )
    asyncio.run(engine.process_batch([observation]))
    valid_state = engine.export_tracking_state()
    revision = engine.revision
    batch_id = "6" * 32

    with pytest.raises(ValueError):
        engine.replace_tracking_state(
            [{"storm_id": "onvolledig"}],
            expected_revision=revision,
            batch_id=batch_id,
        )

    assert engine.revision == revision
    assert engine.has_applied_batch(batch_id) is False
    assert engine.replace_tracking_state(
        valid_state,
        expected_revision=revision,
        batch_id=batch_id,
    ) == 1
    assert engine.revision == revision + 1
    assert engine.has_applied_batch(batch_id) is True


def test_duplicate_snapshot_ids_are_rejected_atomically(
    storm_engine_module, observation_module
):
    engine = storm_engine_module.StormEngine()
    observation = _obs(
        observation_module,
        observation_module.ObservationType.RADAR,
        51.0,
        4.0,
        intensity=12,
    )
    asyncio.run(engine.process_batch([observation]))
    state = engine.export_tracking_state()
    revision = engine.revision

    with pytest.raises(ValueError, match="unieke"):
        engine.replace_tracking_state(
            [state[0], state[0]],
            expected_revision=revision,
            batch_id="7" * 32,
        )

    assert engine.revision == revision
    assert engine.export_tracking_state() == state


def test_process_batch_rolls_back_precommit_exception(
    storm_engine_module, observation_module
):
    engine = storm_engine_module.StormEngine(cluster_radius_km=10.0)
    observations = [
        _obs(
            observation_module,
            observation_module.ObservationType.RADAR,
            51.0,
            4.0,
            intensity=12,
        ),
        _obs(
            observation_module,
            observation_module.ObservationType.RADAR,
            55.0,
            9.0,
            intensity=8,
        ),
    ]
    original_assign = engine._assign_observation
    calls = 0

    def failing_assign(observation, may_create=True):
        nonlocal calls
        calls += 1
        original_assign(observation, may_create=may_create)
        if calls == 2:
            raise RuntimeError("fout na gedeeltelijke mutatie")

    engine._assign_observation = failing_assign
    batch_id = "8" * 32
    with pytest.raises(RuntimeError):
        asyncio.run(engine.process_batch(
            observations,
            expected_revision=0,
            batch_id=batch_id,
        ))

    assert engine.export_tracking_state() == []
    assert engine.revision == 0
    assert engine.has_applied_batch(batch_id) is False

    engine._assign_observation = original_assign
    assert asyncio.run(engine.process_batch(
        observations,
        expected_revision=0,
        batch_id=batch_id,
    )) is True
    assert engine.revision == 1
    assert len(engine.get_storms()) == 2


def test_transaction_rollback_preserves_published_storm_handle(
    storm_engine_module, observation_module
):
    engine = storm_engine_module.StormEngine(cluster_radius_km=10.0)
    timestamp = time.time()
    initial = _obs(
        observation_module,
        observation_module.ObservationType.RADAR,
        51.0,
        4.0,
        ts=timestamp,
        intensity=5,
        parent_system_id="existing-parent",
    )
    asyncio.run(engine.process_batch([initial]))
    published = engine.get_storms()[0]
    before = engine.export_tracking_state()
    revision = engine.revision

    mutation = _obs(
        observation_module,
        observation_module.ObservationType.RADAR,
        51.001,
        4.001,
        ts=timestamp + 60,
        intensity=99,
        parent_system_id="existing-parent",
    )
    original_assign = engine._assign_observation

    def failing_assign(observation, may_create=True):
        original_assign(observation, may_create=may_create)
        raise RuntimeError("fout na mutatie van bestaand object")

    engine._assign_observation = failing_assign
    batch_id = "b" * 32
    with pytest.raises(RuntimeError):
        asyncio.run(engine.process_batch(
            [mutation],
            expected_revision=revision,
            batch_id=batch_id,
        ))

    assert engine.get_storm(published.storm_id) is published
    assert published.max_radar_intensity == 5
    assert len(published.radar_cells) == 1
    assert engine.export_tracking_state() == before
    assert engine.revision == revision
    assert engine.has_applied_batch(batch_id) is False

    engine._assign_observation = original_assign
    assert asyncio.run(engine.process_batch(
        [mutation],
        expected_revision=revision,
        batch_id=batch_id,
    )) is True
    assert engine.get_storm(published.storm_id) is published
    assert published.max_radar_intensity == 99
    assert len(published.radar_cells) == 2


def test_callback_failure_occurs_after_atomic_commit(
    storm_engine_module, observation_module
):
    callback_revisions = []
    engine = None

    def failing_callback(storms):
        callback_revisions.append(engine.revision)
        raise RuntimeError("dashboardpublicatie faalde")

    engine = storm_engine_module.StormEngine(
        on_storms_updated=failing_callback
    )
    observation = _obs(
        observation_module,
        observation_module.ObservationType.RADAR,
        51.0,
        4.0,
        intensity=12,
    )
    batch_id = "9" * 32

    with pytest.raises(RuntimeError):
        asyncio.run(engine.process_batch(
            [observation],
            expected_revision=0,
            batch_id=batch_id,
        ))
    state_after_commit = engine.export_tracking_state()

    duplicate = asyncio.run(engine.process_batch(
        [observation],
        expected_revision=0,
        batch_id=batch_id,
    ))

    assert duplicate is False
    assert callback_revisions == [1]
    assert engine.revision == 1
    assert engine.has_applied_batch(batch_id) is True
    assert engine.export_tracking_state() == state_after_commit


def test_radar_children_share_parent_weather_system(
    storm_engine_module, observation_module
):
    engine = storm_engine_module.StormEngine(cluster_radius_km=10.0)
    timestamp = time.time()
    children = [
        _obs(
            observation_module, observation_module.ObservationType.RADAR,
            48.4, -4.4, ts=timestamp, intensity=7,
            radar_cell_id="opera:frame:p0:c0",
            parent_system_id="opera:frame:p0", area_km2=400.0,
        ),
        _obs(
            observation_module, observation_module.ObservationType.RADAR,
            48.5, -4.1, ts=timestamp, intensity=8,
            radar_cell_id="opera:frame:p0:c1",
            parent_system_id="opera:frame:p0", area_km2=600.0,
        ),
    ]

    asyncio.run(engine.process_batch(children))

    storms = engine.get_storms()
    assert len(storms) == 1
    assert len(storms[0].radar_cells) == 2
    assert storms[0].source_system_ids == {"opera:frame:p0"}


def test_spatially_separated_radar_children_create_a_split(
    storm_engine_module, observation_module
):
    engine = storm_engine_module.StormEngine(cluster_radius_km=10.0)
    timestamp = time.time()
    observations = [
        _obs(
            observation_module, observation_module.ObservationType.RADAR,
            51.0, 4.0, ts=timestamp, intensity=7,
            radar_cell_id="parent:c0", parent_system_id="parent",
            area_km2=80.0,
        ),
        _obs(
            observation_module, observation_module.ObservationType.RADAR,
            51.0, 4.8, ts=timestamp, intensity=7,
            radar_cell_id="parent:c1", parent_system_id="parent",
            area_km2=80.0,
        ),
    ]

    asyncio.run(engine.process_batch(observations))

    assert len(engine.get_storms()) == 2
    events = engine.consume_lineage_events()
    assert len(events) == 1
    assert events[0]["event"] == "split"
    assert events[0]["source_system_id"] == "parent"
    assert events[0]["separation_km"] > 35.0
    child = next(
        storm for storm in engine.get_storms()
        if storm.storm_id == events[0]["child_storm_id"]
    )
    assert child.parent_storm_ids == {events[0]["parent_storm_id"]}
    assert child.lineage_generation == 1
    restored = storm_engine_module.StormEngine(cluster_radius_km=10.0)
    assert restored.restore_mcs_history(engine.export_tracking_state()) == 2
    restored_child = restored.get_storm(child.storm_id)
    assert restored_child.parent_storm_ids == child.parent_storm_ids
    assert restored_child.lineage_generation == 1


def test_radar_association_keeps_physically_plausible_fast_motion(
    storm_engine_module, observation_module
):
    engine = storm_engine_module.StormEngine(cluster_radius_km=50.0)
    timestamp = time.time()
    first = _obs(
        observation_module, observation_module.ObservationType.RADAR,
        51.0, 4.0, ts=timestamp, intensity=6,
        radar_cell_id="frame-1", parent_system_id="frame-1",
    )
    second = _obs(
        observation_module, observation_module.ObservationType.RADAR,
        51.0, 4.4, ts=timestamp + 600, intensity=6,
        radar_cell_id="frame-2", parent_system_id="frame-2",
    )

    asyncio.run(engine.process_batch([first]))
    storm_id = engine.get_storms()[0].storm_id
    asyncio.run(engine.process_batch([second]))

    assert len(engine.get_storms()) == 1
    assert engine.get_storms()[0].storm_id == storm_id


def test_radar_association_preserves_motion_above_legacy_speed_limit(
    storm_engine_module, observation_module
):
    engine = storm_engine_module.StormEngine(cluster_radius_km=50.0)
    timestamp = time.time()
    first = _obs(
        observation_module, observation_module.ObservationType.RADAR,
        51.0, 4.0, ts=timestamp, intensity=6,
        radar_cell_id="frame-1", parent_system_id="frame-1",
    )
    second = _obs(
        observation_module, observation_module.ObservationType.RADAR,
        51.0, 4.6, ts=timestamp + 600, intensity=6,
        radar_cell_id="frame-2", parent_system_id="frame-2",
    )

    asyncio.run(engine.process_batch([first]))
    original_id = engine.get_storms()[0].storm_id
    asyncio.run(engine.process_batch([second]))

    storms = engine.get_storms()
    assert len(storms) == 1
    assert storms[0].storm_id == original_id
    assert storms[0].speed_kmh > 180.0
    assert storms[0].motion_estimate_status == "measured_interval"
    assert (
        storms[0].motion_forecast_speed_min_kmh
        <= storms[0].speed_kmh
        <= storms[0].motion_forecast_speed_max_kmh
    )


def test_two_positions_always_publish_the_measured_linear_vector(
    storm_engine_module, observation_module
):
    engine = storm_engine_module.StormEngine(cluster_radius_km=50.0)
    timestamp = time.time()
    observations = [
        _obs(
            observation_module, observation_module.ObservationType.RADAR,
            51.0, 4.0, ts=timestamp, intensity=4,
            radar_cell_id="first", parent_system_id="first",
        ),
        _obs(
            observation_module, observation_module.ObservationType.RADAR,
            51.0, 4.1, ts=timestamp + 600, intensity=4,
            radar_cell_id="second", parent_system_id="second",
        ),
    ]

    asyncio.run(engine.process_batch([observations[0]]))
    asyncio.run(engine.process_batch([observations[1]]))

    storm = engine.get_storms()[0]
    assert storm.motion_sample_count == 2
    assert storm.heading_deg == pytest.approx(90.0, abs=1.0)
    assert storm.speed_kmh is not None
    assert storm.speed_kmh > 0.0
    assert storm.motion_estimate_status == "measured_interval"
    assert storm.motion_forecast_speed_min_kmh <= storm.speed_kmh
    assert storm.motion_forecast_speed_max_kmh >= storm.speed_kmh


def test_closest_radar_point_uses_child_footprint(
    storm_engine_module, observation_module
):
    engine = storm_engine_module.StormEngine(cluster_radius_km=10.0)
    radar = _obs(
        observation_module, observation_module.ObservationType.RADAR,
        48.8, -3.2, intensity=8, radar_cell_id="cell-1",
        parent_system_id="parent-1",
        footprint_points=((48.4, -4.45), (48.8, -3.2)),
    )
    asyncio.run(engine.process_batch([radar]))

    distance, lat, lon = engine.get_storms()[0].closest_radar_point(
        48.3904, -4.4861
    )
    assert distance < 5.0
    assert (lat, lon) == (48.4, -4.45)


def _record_mcs_frame(
    storm, observation_module, timestamp, frame_number, *, intense=True
):
    parent_id = f"opera:{timestamp}:p0"
    footprint = ((48.5, -4.5), (48.5, -2.5), (48.7, -1.0))
    dbz_values = (55.0 if intense else 45.0, 44.0)
    for child_number, (lon, dbz) in enumerate(
        zip((-4.4, -2.7), dbz_values)
    ):
        obs = _obs(
            observation_module,
            observation_module.ObservationType.RADAR,
            48.6,
            lon,
            ts=timestamp,
            intensity=8 if dbz >= 50 else 6,
            max_dbz=dbz,
            radar_cell_id=f"frame-{frame_number}-cell-{child_number}",
            parent_system_id=parent_id,
            parent_area_km2=46_627.0,
            parent_footprint_points=footprint,
        )
        storm.record_radar_cell(obs)


def test_single_mcs_shaped_frame_is_only_candidate(
    storm_module, observation_module
):
    storm = storm_module.Storm()
    _record_mcs_frame(storm, observation_module, time.time(), 0)

    storm.update_radar_classification()

    assert storm.mcs_status == "candidate"
    assert storm.system_type == "mcs_candidate"
    assert storm.mcs_duration_minutes == 0.0
    assert storm.mcs_convective_span_km >= 100.0
    assert storm.mcs_intense_cells == 1
    diagnostics = storm.mcs_diagnostics()
    assert diagnostics["reason"] == "duration_pending"
    assert diagnostics["sequence_frames"] == 1
    assert diagnostics["checks"] == {
        "convective_cells": True,
        "convective_span": True,
        "intense_cells": True,
        "duration": False,
    }


def test_three_hours_of_qualifying_frames_confirms_mcs(
    storm_module, observation_module
):
    storm = storm_module.Storm()
    start = time.time() - 180 * 60
    for frame_number in range(37):
        _record_mcs_frame(
            storm,
            observation_module,
            start + frame_number * 5 * 60,
            frame_number,
        )

    storm.update_radar_classification()

    assert storm.mcs_status == "confirmed"
    assert storm.system_type == "mcs"
    assert storm.mcs_duration_minutes == pytest.approx(180.0)
    diagnostics = storm.mcs_diagnostics()
    assert diagnostics["reason"] == "duration_confirmed"
    assert diagnostics["sequence_frames"] == 37
    assert all(diagnostics["checks"].values())


def test_mcs_history_survives_engine_restart(
    storm_engine_module, storm_module, observation_module
):
    source = storm_engine_module.StormEngine()
    storm = storm_module.Storm(centroid_lat=48.6, centroid_lon=-3.0)
    start = time.time() - 180 * 60
    for frame_number in range(37):
        _record_mcs_frame(
            storm, observation_module, start + frame_number * 300, frame_number
        )
    storm.update_radar_classification()
    source._storms[storm.storm_id] = storm

    snapshots = source.export_mcs_history()
    restored_engine = storm_engine_module.StormEngine()
    assert restored_engine.restore_mcs_history(snapshots) == 1

    restored = restored_engine.get_storm(storm.storm_id)
    assert restored is not None
    assert restored.mcs_status == "confirmed"
    assert restored.mcs_duration_minutes == pytest.approx(180.0)
    assert len(restored.radar_system_frames) == 37


def test_motion_history_survives_engine_restart(
    storm_engine_module, storm_module
):
    now = time.time()
    source = storm_engine_module.StormEngine()
    storm = storm_module.Storm(last_update=now)
    storm.radar_system_frames["frame"] = storm_module.RadarSystemFrame(
        parent_system_id="frame", timestamp=now, area_km2=100.0,
        footprint_points=((51.0, 4.15),),
    )
    source._storms[storm.storm_id] = storm
    source._history[storm.storm_id] = [
        storm_engine_module.CentroidPoint(51.0, 4.00 + index * 0.05, now - 900 + index * 300)
        for index in range(4)
    ]

    snapshots = source.export_mcs_history()
    assert len(snapshots[0]["motion_history"]) == 4

    restored_engine = storm_engine_module.StormEngine()
    assert restored_engine.restore_mcs_history(snapshots) == 1
    restored = restored_engine.get_storm(storm.storm_id)
    assert restored.motion_sample_count == 4
    assert restored.heading_deg == pytest.approx(90.0, abs=5.0)
    assert restored.speed_kmh is not None
    assert restored.confidence == "Matig"


def test_recent_radar_system_remains_matchable_after_restart(
    storm_engine_module, storm_module, observation_module
):
    """Een tien minuten oud RainViewer-systeem behoudt ID, frames en vector."""
    now = time.time()
    source = storm_engine_module.StormEngine(
        expire_minutes=5.0, remove_minutes=15.0
    )
    storm = storm_module.Storm(
        storm_id="restart-track",
        centroid_lat=51.0,
        centroid_lon=4.15,
        last_update=now - 10 * 60,
    )
    for index in range(4):
        timestamp = now - (25 - index * 5) * 60
        lon = 4.0 + index * 0.05
        parent_id = f"rainviewer-{index}"
        storm.radar_system_frames[parent_id] = storm_module.RadarSystemFrame(
            parent_system_id=parent_id,
            timestamp=timestamp,
            area_km2=100.0,
            footprint_points=((51.0, lon),),
        )
        source._history.setdefault(storm.storm_id, []).append(
            storm_engine_module.CentroidPoint(51.0, lon, timestamp)
        )
    source._storms[storm.storm_id] = storm

    restored_engine = storm_engine_module.StormEngine(
        expire_minutes=5.0, remove_minutes=15.0
    )
    assert restored_engine.restore_mcs_history(
        source.export_mcs_history()
    ) == 1
    restored = restored_engine.get_storm("restart-track")
    assert restored is not None
    assert restored.is_dormant is False
    assert restored.motion_sample_count == 4

    next_frame = observation_module.Observation(
        obs_type=observation_module.ObservationType.RADAR,
        lat=51.0,
        lon=4.20,
        timestamp=now,
        source="rainviewer",
        intensity=3,
        parent_system_id="rainviewer-next",
    )
    asyncio.run(restored_engine.process_batch([next_frame]))

    assert len(restored_engine.get_storms()) == 1
    continued = restored_engine.get_storm("restart-track")
    assert continued is restored
    assert continued.consecutive_radar_frames == 5
    assert continued.motion_sample_count >= 4
    assert continued.heading_deg == pytest.approx(90.0, abs=10.0)


def test_install_committed_tracking_state_restores_revision_and_receipts(
    storm_engine_module, observation_module
):
    source = storm_engine_module.StormEngine()
    first_batch = "a" * 32
    second_batch = "b" * 32
    observation = _obs(
        observation_module,
        observation_module.ObservationType.LIGHTNING,
        40.6413,
        -73.7781,
    )
    asyncio.run(source.process_batch(
        [observation],
        expected_revision=0,
        batch_id=first_batch,
    ))
    snapshots = source.export_tracking_state()

    restored = storm_engine_module.StormEngine()
    assert restored.install_committed_tracking_state(
        snapshots,
        revision=17,
        committed_batch_ids=[first_batch, second_batch, first_batch],
        publish=False,
    ) == 1

    assert restored.revision == 17
    assert restored.export_tracking_state() == snapshots
    assert restored.has_applied_batch(first_batch) is True
    assert restored.has_applied_batch(second_batch) is True
    assert asyncio.run(restored.process_batch(
        [observation],
        expected_revision=17,
        batch_id=first_batch,
    )) is False
    assert restored.revision == 17


def test_install_committed_tracking_state_rejects_partial_snapshot(
    storm_engine_module,
):
    engine = storm_engine_module.StormEngine()
    with pytest.raises(ValueError):
        engine.install_committed_tracking_state(
            [{"storm_id": "missing-track-data"}],
            revision=1,
        )
    assert engine.revision == 0
    assert engine.export_tracking_state() == []


def test_calculate_batch_candidate_is_pure_until_explicit_install(
    storm_engine_module, observation_module
):
    engine = storm_engine_module.StormEngine()
    observation = _obs(
        observation_module,
        observation_module.ObservationType.LIGHTNING,
        51.02,
        4.47,
    )
    batch_id = "c" * 32

    candidate = asyncio.run(engine.calculate_batch_candidate(
        [observation],
        expected_revision=0,
        batch_id=batch_id,
    ))

    assert len(candidate) == 1
    assert engine.revision == 0
    assert engine.export_tracking_state() == []
    assert engine.has_applied_batch(batch_id) is False

    assert engine.install_committed_tracking_state(
        candidate,
        revision=1,
        committed_batch_ids=[batch_id],
    ) == 1
    assert engine.revision == 1
    assert engine.has_applied_batch(batch_id) is True


def test_legacy_snapshot_rebuilds_motion_from_radar_cells(
    storm_engine_module, storm_module
):
    now = time.time()
    storm = storm_module.Storm(last_update=now)
    for index in range(4):
        timestamp = now - 900 + index * 300
        lon = 4.00 + index * 0.05
        cell_id = f"cell-{index}"
        storm.radar_cells[cell_id] = storm_module.RadarCellSnapshot(
            cell_id=cell_id, timestamp=timestamp, lat=51.0, lon=lon,
            intensity=3, area_km2=100.0, max_dbz=35.0,
        )
        frame_id = f"frame-{index}"
        storm.radar_system_frames[frame_id] = storm_module.RadarSystemFrame(
            parent_system_id=frame_id, timestamp=timestamp, area_km2=100.0,
            footprint_points=((51.0, lon),),
        )

    legacy_snapshot = storm.to_mcs_snapshot()
    assert "motion_history" not in legacy_snapshot

    restored_engine = storm_engine_module.StormEngine()
    assert restored_engine.restore_mcs_history([legacy_snapshot]) == 1
    restored = restored_engine.get_storm(storm.storm_id)
    assert restored.motion_sample_count == 4
    assert restored.heading_deg == pytest.approx(90.0, abs=5.0)
    assert restored.confidence == "Matig"


def test_large_rain_area_without_intense_convection_is_not_mcs(
    storm_module, observation_module
):
    storm = storm_module.Storm()
    _record_mcs_frame(
        storm, observation_module, time.time(), 0, intense=False
    )

    storm.update_radar_classification()

    assert storm.mcs_status == "not_mcs"
    assert storm.system_type == "convective_cluster"
    assert storm.mcs_parent_area_km2 == 46_627.0
    diagnostics = storm.mcs_diagnostics()
    assert diagnostics["reason"] == "no_intense_cell"
    assert diagnostics["checks"]["intense_cells"] is False
    assert diagnostics["thresholds"]["min_intense_cells"] == 1


# ── RAIN: verifieert, creëert NOOIT een nieuwe storm ────────────────────────

def test_rain_alone_never_creates_a_storm(storm_engine_module, observation_module):
    engine = storm_engine_module.StormEngine()
    rain = _obs(observation_module, observation_module.ObservationType.RAIN,
                51.026, 4.478, station_id="netatmo-1", rain_mm=2.0)
    asyncio.run(engine.process_batch([rain]))

    assert engine.get_storms() == [], "een RAIN-observatie mag NOOIT zelfstandig een storm aanmaken"


def test_rain_confirms_nearby_existing_storm(storm_engine_module, observation_module):
    engine = storm_engine_module.StormEngine(cluster_radius_km=30.0)
    timestamp = time.time()
    radar = _obs(
        observation_module,
        observation_module.ObservationType.RADAR,
        51.026,
        4.478,
        ts=timestamp,
        parent_system_id="nearby-system",
        parent_footprint_points=(
            (51.0, 4.44), (51.0, 4.52), (51.05, 4.52), (51.05, 4.44),
        ),
    )
    asyncio.run(engine.process_batch([radar]))

    rain = _obs(observation_module, observation_module.ObservationType.RAIN,
                51.030, 4.480, ts=timestamp,
                station_id="netatmo-1", rain_mm=1.0)
    asyncio.run(engine.process_batch([rain]))

    storm = engine.get_storms()[0]
    assert storm.netatmo_confirmations == 1
    assert storm.netatmo_no_rain_count == 0


def test_rain_inside_large_footprint_confirms_even_when_vertices_are_far_away(
    storm_engine_module, observation_module
):
    engine = storm_engine_module.StormEngine(cluster_radius_km=30.0)
    radar = _obs(
        observation_module,
        observation_module.ObservationType.RADAR,
        51.0,
        4.0,
        intensity=20,
        parent_system_id="large-system",
        parent_footprint_points=(
            (50.9, 3.85),
            (50.9, 4.15),
            (51.1, 4.15),
            (51.1, 3.85),
        ),
    )
    asyncio.run(engine.process_batch([radar]))
    storm = engine.get_storms()[0]

    rain = _obs(
        observation_module,
        observation_module.ObservationType.RAIN,
        51.0,
        4.0,
        station_id="inside-footprint",
        rain_mm=1.0,
    )
    asyncio.run(engine.process_batch([rain]))

    assert storm.netatmo_confirmations == 1


def test_rain_inside_radar_hole_does_not_confirm(
    storm_engine_module, observation_module
):
    engine = storm_engine_module.StormEngine(cluster_radius_km=30.0)
    timestamp = time.time()
    polygons = ((
        (
            (50.5, 3.0), (50.5, 5.0), (51.5, 5.0),
            (51.5, 3.0), (50.5, 3.0),
        ),
        (
            (50.8, 3.7), (50.8, 4.3), (51.2, 4.3),
            (51.2, 3.7), (50.8, 3.7),
        ),
    ),)
    radar = _obs(
        observation_module,
        observation_module.ObservationType.RADAR,
        51.0,
        4.0,
        ts=timestamp,
        parent_system_id="donut-system",
        footprint_polygons=polygons,
        parent_footprint_polygons=polygons,
    )
    asyncio.run(engine.process_batch([radar]))
    storm = engine.get_storms()[0]

    hole_station = _obs(
        observation_module,
        observation_module.ObservationType.RAIN,
        51.0,
        4.0,
        ts=timestamp,
        station_id="dry-hole",
        rain_mm=1.0,
    )
    asyncio.run(engine.process_batch([hole_station]))
    assert storm.netatmo_confirmations == 0

    occupied_station = _obs(
        observation_module,
        observation_module.ObservationType.RAIN,
        50.65,
        4.0,
        ts=timestamp,
        station_id="wet-outer",
        rain_mm=1.0,
    )
    asyncio.run(engine.process_batch([occupied_station]))
    assert storm.netatmo_confirmations == 1


def test_rain_can_confirm_second_same_time_component_of_one_storm(
    storm_engine_module, observation_module
):
    engine = storm_engine_module.StormEngine(cluster_radius_km=30.0)
    timestamp = time.time()
    west = _obs(
        observation_module,
        observation_module.ObservationType.RADAR,
        51.0,
        4.0,
        ts=timestamp,
        parent_system_id="west-parent",
        parent_footprint_points=(
            (50.98, 3.98), (50.98, 4.02),
            (51.02, 4.02), (51.02, 3.98),
        ),
    )
    asyncio.run(engine.process_batch([west]))
    storm = engine.get_storms()[0]
    east = _obs(
        observation_module,
        observation_module.ObservationType.RADAR,
        51.0,
        5.0,
        ts=timestamp,
        parent_system_id="east-parent",
        parent_footprint_points=(
            (50.98, 4.98), (50.98, 5.02),
            (51.02, 5.02), (51.02, 4.98),
        ),
    )
    storm.record_radar_cell(east)

    rain = _obs(
        observation_module,
        observation_module.ObservationType.RAIN,
        51.0,
        5.0,
        ts=timestamp,
        station_id="inside-east-component",
        rain_mm=1.0,
    )
    asyncio.run(engine.process_batch([rain]))

    assert storm.netatmo_confirmations == 1


def test_rain_between_same_time_components_does_not_confirm(
    storm_engine_module, observation_module
):
    engine = storm_engine_module.StormEngine(cluster_radius_km=30.0)
    timestamp = time.time()
    west = _obs(
        observation_module,
        observation_module.ObservationType.RADAR,
        51.0,
        4.0,
        ts=timestamp,
        parent_system_id="west-parent",
        parent_footprint_points=(
            (50.98, 3.98), (50.98, 4.02),
            (51.02, 4.02), (51.02, 3.98),
        ),
    )
    asyncio.run(engine.process_batch([west]))
    storm = engine.get_storms()[0]
    east = _obs(
        observation_module,
        observation_module.ObservationType.RADAR,
        51.0,
        5.0,
        ts=timestamp,
        parent_system_id="east-parent",
        parent_footprint_points=(
            (50.98, 4.98), (50.98, 5.02),
            (51.02, 5.02), (51.02, 4.98),
        ),
    )
    storm.record_radar_cell(east)

    rain = _obs(
        observation_module,
        observation_module.ObservationType.RAIN,
        51.0,
        4.5,
        ts=timestamp,
        station_id="between-components",
        rain_mm=1.0,
    )
    asyncio.run(engine.process_batch([rain]))

    assert storm.netatmo_confirmations == 0


def test_rain_does_not_confirm_against_stale_historical_hull(
    storm_engine_module, observation_module
):
    engine = storm_engine_module.StormEngine()
    old_timestamp = time.time() - 30 * 60
    radar = _obs(
        observation_module,
        observation_module.ObservationType.RADAR,
        51.0,
        4.0,
        ts=old_timestamp,
        parent_system_id="old-system",
        parent_footprint_points=(
            (50.9, 3.9), (50.9, 4.1), (51.1, 4.1), (51.1, 3.9),
        ),
    )
    asyncio.run(engine.process_batch([radar]))
    storm = engine.get_storms()[0]
    storm.hull = [
        (50.9, 3.9), (50.9, 4.1), (51.1, 4.1), (51.1, 3.9),
    ]

    rain = _obs(
        observation_module,
        observation_module.ObservationType.RAIN,
        51.0,
        4.0,
        station_id="current-station",
        rain_mm=1.0,
    )
    asyncio.run(engine.process_batch([rain]))

    assert storm.netatmo_confirmations == 0


def test_rain_verification_is_assigned_to_only_the_nearest_storm(
    storm_engine_module, observation_module
):
    engine = storm_engine_module.StormEngine(cluster_radius_km=1.0)
    engine._last_merge_check = time.time()
    timestamp = time.time()
    radar_storms = [
        _obs(
            observation_module,
            observation_module.ObservationType.RADAR,
            51.0,
            longitude,
            ts=timestamp,
            parent_system_id=parent,
            parent_footprint_points=footprint,
        )
        for longitude, parent, footprint in (
            (
                4.0,
                "left-parent",
                (
                    (50.99, 3.99), (50.99, 4.01),
                    (51.01, 4.01), (51.01, 3.99),
                ),
            ),
            (
                4.06,
                "right-parent",
                (
                    (50.99, 4.05), (50.99, 4.07),
                    (51.01, 4.07), (51.01, 4.05),
                ),
            ),
        )
    ]
    asyncio.run(engine.process_batch(radar_storms))
    left = min(engine.get_storms(), key=lambda storm: storm.centroid_lon)
    right = max(engine.get_storms(), key=lambda storm: storm.centroid_lon)

    rain = _obs(
        observation_module,
        observation_module.ObservationType.RAIN,
        51.0,
        4.0,
        ts=timestamp,
        station_id="nearest-left",
        rain_mm=1.0,
    )
    asyncio.run(engine.process_batch([rain]))

    assert left.netatmo_confirmations == 1
    assert right.netatmo_confirmations == 0


def test_rain_verification_does_not_choose_between_equal_overlapping_storms(
    storm_engine_module, observation_module
):
    engine = storm_engine_module.StormEngine(cluster_radius_km=1.0)
    engine._last_merge_check = time.time()
    timestamp = time.time()
    footprint = [
        (50.99, 3.99), (50.99, 4.01), (51.01, 4.01), (51.01, 3.99),
    ]
    asyncio.run(engine.process_batch([
        _obs(
            observation_module,
            observation_module.ObservationType.RADAR,
            51.0,
            4.0,
            ts=timestamp,
            parent_system_id=parent,
            parent_footprint_points=tuple(footprint),
        )
        for parent in ("first-parent", "second-parent")
    ]))
    first, second = engine.get_storms()

    rain = _obs(
        observation_module,
        observation_module.ObservationType.RAIN,
        51.0,
        4.0,
        ts=timestamp,
        station_id="ambiguous",
        rain_mm=1.0,
    )
    asyncio.run(engine.process_batch([rain]))

    assert first.netatmo_confirmations == 0
    assert second.netatmo_confirmations == 0


def test_rain_without_precipitation_increments_no_rain_count(storm_engine_module, observation_module):
    engine = storm_engine_module.StormEngine(cluster_radius_km=30.0)
    timestamp = time.time()
    radar = _obs(
        observation_module,
        observation_module.ObservationType.RADAR,
        51.026,
        4.478,
        ts=timestamp,
        parent_system_id="dry-check",
        parent_footprint_points=(
            (51.0, 4.44), (51.0, 4.52), (51.05, 4.52), (51.05, 4.44),
        ),
    )
    asyncio.run(engine.process_batch([radar]))

    dry_station = _obs(observation_module, observation_module.ObservationType.RAIN,
                        51.030, 4.480, ts=timestamp,
                        station_id="netatmo-2", rain_mm=0.0)
    asyncio.run(engine.process_batch([dry_station]))

    storm = engine.get_storms()[0]
    assert storm.netatmo_no_rain_count == 1
    assert storm.netatmo_confirmations == 0


# ── Merge ────────────────────────────────────────────────────────────────

def test_nearby_storms_are_merged(storm_engine_module, observation_module):
    """
    Twee storms die binnen MERGE_RADIUS_KM van elkaar komen, moeten
    samengevoegd worden zodra de merge-throttle dat toelaat.
    """
    engine = storm_engine_module.StormEngine(cluster_radius_km=5.0)
    # Twee punten >5km uit elkaar (aparte storms) maar <60km (binnen MERGE_RADIUS_KM)
    o1 = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.00, 4.00)
    o2 = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.08, 4.08)  # ~10km
    asyncio.run(engine.process_batch([o1]))
    asyncio.run(engine.process_batch([o2]))
    assert len(engine.get_storms()) == 2, "voorwaarde: moeten eerst 2 aparte storms zijn"

    # Forceer de merge-throttle open (normaal max 1x/15s)
    engine._last_merge_check = 0.0
    asyncio.run(engine.process_batch([
        _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.00, 4.00)
    ]))

    assert len(engine.get_storms()) == 1, "storms binnen MERGE_RADIUS_KM moeten samengevoegd worden"


def test_merge_combines_strike_counts(storm_engine_module, observation_module):
    engine = storm_engine_module.StormEngine(cluster_radius_km=5.0)
    o1 = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.00, 4.00)
    o2 = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.08, 4.08)
    asyncio.run(engine.process_batch([o1]))
    asyncio.run(engine.process_batch([o2]))

    engine._last_merge_check = 0.0
    asyncio.run(engine.process_batch([
        _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.00, 4.00)
    ]))

    storm = engine.get_storms()[0]
    assert storm.strike_count == 3  # 1 + 1 + 1 (de laatste observatie telt ook mee)


def test_diverging_radar_storms_are_not_merged(
    storm_engine_module, observation_module
):
    engine = storm_engine_module.StormEngine(cluster_radius_km=5.0)
    first = _obs(
        observation_module, observation_module.ObservationType.RADAR,
        51.0, 4.0, intensity=6, radar_cell_id="first",
        parent_system_id="first-parent", area_km2=20.0,
    )
    second = _obs(
        observation_module, observation_module.ObservationType.RADAR,
        51.2, 4.2, intensity=6, radar_cell_id="second",
        parent_system_id="second-parent", area_km2=20.0,
    )
    asyncio.run(engine.process_batch([first, second]))
    storms = engine.get_storms()
    assert len(storms) == 2
    storms[0].heading_deg = 45.0
    storms[1].heading_deg = 225.0

    engine._last_merge_check = 0.0
    asyncio.run(engine.process_batch([first]))

    assert len(engine.get_storms()) == 2


# ── Lifecycle: dormant / expired ──────────────────────────────────────────

def test_storm_becomes_dormant_after_expire_minutes(storm_engine_module, observation_module):
    engine = storm_engine_module.StormEngine(expire_minutes=5.0, remove_minutes=15.0)
    obs = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.026, 4.478)
    asyncio.run(engine.process_batch([obs]))

    storm = engine.get_storms()[0]
    # Simuleer dat de storm al 10 minuten niets meer deed
    storm.last_update = time.time() - 10 * 60

    # Triggert _expire_storms() via een batch met een observatie op een andere plek
    other = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 60.0, 10.0)
    asyncio.run(engine.process_batch([other]))

    original = engine.get_storm(storm.storm_id)
    assert original is not None
    assert original.is_dormant is True
    assert original not in engine.get_active_storms()


def test_radar_storm_stays_active_across_ten_minute_frame_interval(
    storm_engine_module, observation_module
):
    """Een 10-minutenradar mag zijn systeem-ID en bewegingshistorie behouden."""
    engine = storm_engine_module.StormEngine(
        expire_minutes=5.0, remove_minutes=15.0
    )
    radar = observation_module.Observation(
        obs_type=observation_module.ObservationType.RADAR,
        lat=51.0,
        lon=4.0,
        timestamp=time.time() - 10 * 60,
        source="rainviewer",
        intensity=3,
        parent_system_id="rain-area-1",
    )
    asyncio.run(engine.process_batch([radar]))
    storm = engine.get_storms()[0]
    storm.last_update = time.time() - 10 * 60

    engine._expire_storms()

    assert storm.is_dormant is False
    assert engine.get_storm(storm.storm_id) is storm


def test_storm_is_removed_after_remove_minutes(storm_engine_module, observation_module):
    engine = storm_engine_module.StormEngine(expire_minutes=5.0, remove_minutes=15.0)
    obs = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.026, 4.478)
    asyncio.run(engine.process_batch([obs]))

    storm = engine.get_storms()[0]
    storm.last_update = time.time() - 20 * 60  # ruim voorbij remove_minutes

    other = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 60.0, 10.0)
    asyncio.run(engine.process_batch([other]))

    assert engine.get_storm(storm.storm_id) is None, "storm had verwijderd moeten worden"


def test_dormant_storm_does_not_receive_new_lightning_assignment(storm_engine_module, observation_module):
    """Een sluimerende storm mag geen nieuwe strikes toegewezen krijgen (_assign_observation skipt dormant storms)."""
    engine = storm_engine_module.StormEngine(cluster_radius_km=30.0, expire_minutes=5.0)
    obs = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.026, 4.478)
    asyncio.run(engine.process_batch([obs]))

    storm = engine.get_storms()[0]
    storm.is_dormant = True
    storm.last_update = time.time()  # niet oud genoeg om verwijderd te worden, maar wel dormant

    # Nieuwe strike op dezelfde plek zou normaal bij deze storm horen
    new_obs = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.027, 4.479)
    asyncio.run(engine.process_batch([new_obs]))

    storms = engine.get_storms()
    # Een NIEUWE storm moet ontstaan zijn, de dormant storm blijft ongewijzigd op 1 strike
    assert len(storms) == 2
    dormant = engine.get_storm(storm.storm_id)
    assert dormant.strike_count == 1


# ── Regressie / beweging ────────────────────────────────────────────────

def test_movement_regression_detects_eastward_motion(storm_engine_module, observation_module):
    """
    Een storm die stap voor stap naar het oosten verschuift, moet na
    genoeg history-punten een heading rond 90 graden krijgen.
    """
    engine = storm_engine_module.StormEngine(cluster_radius_km=50.0)
    # Kleine marge op het vijfminutenvenster voorkomt dat punt nul tijdens
    # de test door enkele milliseconden klokverloop wordt weggepruned.
    base_ts = time.time() - 295
    lon = 4.0
    for i in range(6):
        obs = _obs(
            observation_module, observation_module.ObservationType.LIGHTNING,
            51.0, lon, ts=base_ts + i * 60,
        )
        asyncio.run(engine.process_batch([obs]))
        lon += 0.05  # ~3.5km oostwaarts per stap

    storm = engine.get_storms()[0]
    assert storm.heading_deg is not None
    assert 60 < storm.heading_deg < 120, f"verwacht ~oostwaarts (90°), kreeg {storm.heading_deg}"
    assert storm.speed_kmh is not None and storm.speed_kmh > 0
    assert storm.motion_sample_count == 6
    assert storm.motion_history_minutes == pytest.approx(5.0)
    assert storm.motion_fit_quality > 0.9
    assert storm.confidence == "Laag"


def test_same_radar_timestamp_replaces_centroid_instead_of_duplicating_it(
    storm_engine_module, observation_module
):
    engine = storm_engine_module.StormEngine(cluster_radius_km=50.0)
    timestamp = time.time()
    first = _obs(
        observation_module, observation_module.ObservationType.RADAR,
        51.0, 4.0, ts=timestamp, intensity=3,
    )
    second = _obs(
        observation_module, observation_module.ObservationType.RADAR,
        51.0, 4.05, ts=timestamp, intensity=3,
    )

    asyncio.run(engine.process_batch([first]))
    asyncio.run(engine.process_batch([second]))

    storm = engine.get_storms()[0]
    assert len(engine._history[storm.storm_id]) == 1
    assert engine._history[storm.storm_id][0].lon > 4.0


def test_radar_anchor_follows_shared_translation_not_child_weight(
    storm_engine_module,
):
    grouped = {
        1_000.0: [
            (51.0, 4.0, 50.0),
            (51.0, 4.2, 50.0),
        ],
        1_600.0: [
            (51.0, 4.01, 1_000.0),
            (51.0, 4.21, 1.0),
        ],
    }

    history = storm_engine_module.StormEngine._radar_anchor_history(grouped)

    assert len(history) == 2
    assert history[0].lon == pytest.approx(4.1)
    # Beide "puppies" verschoven 0.01 graden. De enorme gewichtsverandering
    # mag de gezamenlijke handgreep dus niet richting de eerste cel trekken.
    assert history[1].lon == pytest.approx(4.11)


def test_confidence_is_insufficient_with_too_little_history(storm_engine_module, observation_module):
    engine = storm_engine_module.StormEngine()
    obs = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.026, 4.478)
    asyncio.run(engine.process_batch([obs]))

    storm = engine.get_storms()[0]
    assert storm.confidence == "Onvoldoende data"
    assert storm.heading_deg is None


# ── Geocoding (lazy, alleen met places-database) ──────────────────────────

def test_geocoding_skipped_without_places_database(storm_engine_module, observation_module):
    engine = storm_engine_module.StormEngine(places=None)
    obs = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.026, 4.478)
    asyncio.run(engine.process_batch([obs]))

    storm = engine.get_storms()[0]
    assert storm.location_name == ""


def test_geocoding_finds_nearest_place_when_database_provided(storm_engine_module, observation_module, geocode_module):
    places = [geocode_module.PlaceEntry("Mechelen", "BE", 51.0259, 4.4776)]
    engine = storm_engine_module.StormEngine(places=places)
    obs = _obs(observation_module, observation_module.ObservationType.RADAR, 51.026, 4.478, intensity=5)
    asyncio.run(engine.process_batch([obs]))

    storm = engine.get_storms()[0]
    assert storm.location_name == "Mechelen"
