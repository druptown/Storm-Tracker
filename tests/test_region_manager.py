"""Contracttests voor de echte dynamische RegionEngine-runtime."""
import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


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


def test_region_runtime_refreshes_lossless_compute_schedule_after_batch(
    region_manager_module,
    observation_module,
):
    async def scenario():
        manager = region_manager_module.StormManager(
            sharing_distance_km=150,
            observation_radius_km=350,
            allow_local_processing=True,
        )
        engine = manager.assign_target("home", 51.2, 4.4)

        initial = engine.compute_schedule_snapshot
        assert initial["state"] == "ready"
        assert initial["target_count"] == 1
        assert initial["tracked_count"] == 0
        assert initial["scheduled_track_count"] == 0

        # Houd de mergecheck bewust buiten deze ene batch. De 30 ruimtelijk
        # gescheiden observaties modelleren onafhankelijke WeatherSystems en
        # bewijzen dat de legacy-richtwaarde van 15 geen runtimefilter is.
        engine.storm_engine._last_merge_check = time.time()
        observed_at = time.time()
        observations = []
        for row in range(5):
            for column in range(6):
                observations.append(observation_module.Observation(
                    obs_type=observation_module.ObservationType.RADAR,
                    lat=50.4 + row * 0.4,
                    lon=2.65 + column * 0.7,
                    timestamp=observed_at,
                    intensity=5,
                    max_dbz=42.0,
                    area_km2=25.0,
                    quality=0.9,
                    source="compute-schedule-test",
                ))

        batch = region_manager_module.ObservationBatch(observations)
        await engine.ofe._on_batch(batch)

        snapshot = engine.compute_schedule_snapshot
        assert batch.committed is True
        assert len(engine.storm_engine.get_storms()) == 30
        assert snapshot["state"] == "ready"
        assert snapshot["tracked_count"] == 30
        assert snapshot["scheduled_track_count"] == 30
        assert snapshot["forecast_input_count"] == 30
        assert snapshot["preferred_foreground_count"] == 15
        assert snapshot["deferred_advisory_count"] == 15
        assert snapshot["preferred_pass_count"] == 2
        assert snapshot["invariants"]["all_tracks_scheduled"] is True
        assert (
            snapshot["invariants"][
                "all_tracks_retained_for_forecast_input"
            ]
            is True
        )

        revision = engine.compute_schedule_revision
        first = engine.compute_schedule_diagnostics()
        second = engine.compute_schedule_diagnostics()
        assert first == second
        assert engine.compute_schedule_revision == revision
        assert first["schedule_entries_total"] == 30
        assert len(first["schedule_entries_preview"]) == 25
        assert first["schedule_entries_overflow"] == 5

    asyncio.run(scenario())


def test_compute_schedule_failure_falls_back_to_all_tracks(
    region_manager_module,
):
    manager = region_manager_module.StormManager()
    engine = manager.assign_target("home", 51.2, 4.4)
    engine.storm_engine._storms = {
        "storm-b": SimpleNamespace(storm_id="storm-b", strikes_60min=0),
        "storm-a": SimpleNamespace(storm_id="storm-a", strikes_60min=0),
    }

    def broken_schedule(*args, **kwargs):
        raise RuntimeError("synthetische schedulerfout")

    engine.storm_engine.build_compute_schedule = broken_schedule
    snapshot = engine.refresh_compute_schedule()

    assert snapshot["state"] == "fallback_all_tracks"
    assert snapshot["fallback_execution_required"] is True
    assert snapshot["error_category"] == "RuntimeError"
    assert snapshot["tracked_count"] == 2
    assert snapshot["scheduled_track_count"] == 2
    assert snapshot["forecast_input_count"] == 2
    assert [
        entry["storm_id"] for entry in snapshot["schedule"]
    ] == ["storm-a", "storm-b"]
    assert all(
        entry["execution_required"] is True
        and entry["forecast_input_retained"] is True
        for entry in snapshot["schedule"]
    )


def test_failed_worker_batch_is_processed_once_by_local_engine(
    region_manager_module,
):
    async def scenario():
        worker_calls = []

        async def worker_callback(
            region, observations, start_state, start_revision
        ):
            worker_calls.append(
                (
                    region.engine_id,
                    observations,
                    start_state,
                    start_revision,
                )
            )
            return False

        manager = region_manager_module.StormManager(
            observation_radius_km=350,
            on_observation_batch=worker_callback,
            allow_local_processing=True,
        )
        engine = manager.assign_target("home", 51.2194, 4.4025)
        engine.storm_engine.process_batch = AsyncMock()
        observation = SimpleNamespace(lat=51.0, lon=4.0)

        await engine.ofe._on_batch([observation])

        assert len(worker_calls) == 1
        delivered_batch = worker_calls[0][1]
        engine.storm_engine.process_batch.assert_awaited_once_with(
            delivered_batch,
            expected_revision=0,
            batch_id=delivered_batch.batch_id,
        )
        assert delivered_batch.committed is True

    asyncio.run(scenario())


def test_local_processing_is_never_inferred_from_missing_hass(
    region_manager_module,
):
    async def scenario():
        async def unavailable_worker(*_args):
            return False

        manager = region_manager_module.StormManager(
            on_observation_batch=unavailable_worker,
        )
        engine = manager.assign_target("home", 51.2, 4.4)
        observation = SimpleNamespace(lat=51.0, lon=4.0)

        with pytest.raises(
            region_manager_module.WorkerTrackingRequiredError
        ):
            await engine.ofe._on_batch([observation])

    asyncio.run(scenario())


def test_successful_worker_batch_is_not_processed_twice_locally(
    region_manager_module,
):
    async def scenario():
        async def worker_callback(
            region, observations, start_state, start_revision
        ):
            return True

        manager = region_manager_module.StormManager(
            observation_radius_km=350,
            on_observation_batch=worker_callback,
        )
        engine = manager.assign_target("home", 51.2194, 4.4025)
        engine.storm_engine.process_batch = AsyncMock()
        observation = SimpleNamespace(lat=51.0, lon=4.0)

        await engine.ofe._on_batch([observation])

        engine.storm_engine.process_batch.assert_not_awaited()

    asyncio.run(scenario())


def test_region_subset_preserves_shared_batch_receipt(
    region_manager_module,
):
    async def scenario():
        seen = {}

        async def worker_callback(
            region, observations, start_state, start_revision
        ):
            seen["batch"] = observations
            observations.acknowledge()
            return True

        manager = region_manager_module.StormManager(
            observation_radius_km=350,
            on_observation_batch=worker_callback,
        )
        engine = manager.assign_target("home", 51.2194, 4.4025)
        source = region_manager_module.ObservationBatch([
            SimpleNamespace(lat=51.0, lon=4.0)
        ])

        await engine.ofe._on_batch(source)

        assert seen["batch"].batch_id == source.batch_id
        assert seen["batch"] is not source
        assert seen["batch"].committed is True
        assert source.committed is True

    asyncio.run(scenario())


def test_worker_commit_then_throw_falls_back_as_idempotent_noop(
    region_manager_module, observation_module,
):
    async def scenario():
        callback_calls = 0

        async def worker_callback(
            region, observations, start_state, start_revision
        ):
            nonlocal callback_calls
            callback_calls += 1
            await region.storm_engine.process_batch(
                observations,
                expected_revision=start_revision,
                batch_id=observations.batch_id,
            )
            raise RuntimeError("diagnostiek faalde na commit")

        manager = region_manager_module.StormManager(
            observation_radius_km=350,
            on_observation_batch=worker_callback,
        )
        engine = manager.assign_target("home", 51.2194, 4.4025)
        observation = observation_module.Observation(
            obs_type=observation_module.ObservationType.RADAR,
            lat=51.0,
            lon=4.0,
            timestamp=time.time(),
            source="test",
            intensity=4,
        )
        source = region_manager_module.ObservationBatch([observation])

        await engine.ofe._on_batch(source)

        assert callback_calls == 1
        assert source.committed is True
        assert engine.storm_engine.revision == 1
        assert engine.storm_engine.has_applied_batch(source.batch_id)
        assert len(engine.storm_engine.get_storms()) == 1

    asyncio.run(scenario())


def test_async_bulk_route_waits_single_region_ingest_receipt(
    region_manager_module,
):
    async def scenario():
        manager = region_manager_module.StormManager(
            observation_radius_km=350
        )
        engine = manager.assign_target("home", 51.2, 4.4)
        persisted_groups = []

        async def persist(observations):
            persisted_groups.append(list(observations))
            return tuple(True for _ in observations)

        engine.ofe.configure_durable_ingest(persist)
        observations = [
            SimpleNamespace(
                lat=51.0, lon=4.0, timestamp=time.time(),
                obs_type="test",
            ),
            SimpleNamespace(
                lat=51.1, lon=4.1, timestamp=time.time(),
                obs_type="test",
            ),
        ]
        assert await manager.async_route_observations(observations) == 2
        assert persisted_groups == [observations]
        await engine.ofe.reset()

    asyncio.run(scenario())


def test_close_admission_rejects_new_sync_and_async_routes(
    region_manager_module,
):
    async def scenario():
        manager = region_manager_module.StormManager()
        manager.assign_target("home", 51.2, 4.4)
        observation = SimpleNamespace(
            lat=51.0, lon=4.0, timestamp=time.time(),
            obs_type="test",
        )
        manager.close_admission()
        assert manager.route_observation(observation) == 0
        assert await manager.async_route_observation(observation) == 0

    asyncio.run(scenario())


def test_global_admission_rejects_direct_route_until_recovery_finishes(
    region_manager_module,
):
    async def scenario():
        manager = region_manager_module.StormManager()
        manager.begin_durable_recovery()
        engine = manager.assign_target("home", 51.2, 4.4)
        observation = SimpleNamespace(
            lat=51.0, lon=4.0, timestamp=time.time(),
            obs_type="test",
        )
        assert engine.recovery_ready is False
        assert manager.admission_ready is False
        assert manager.route_observation(observation) == 0
        assert (
            manager.route_observation_to_engine(
                engine.engine_id, observation
            )
            is False
        )
        assert await manager.async_route_observation(observation) == 0
        assert (
            await manager.async_route_observations_to_engine(
                engine.engine_id, [observation]
            )
            == 0
        )

        # Dit was het technische venster: lokaal herstel was klaar, maar de
        # globale journalintegriteit nog niet. De route moet gesloten blijven.
        engine.recovery_ready = True
        assert await manager.async_route_observation(observation) == 0

        manager.verify_durable_integrity(integrity_healthy=True)
        assert await manager.async_route_observation(observation) == 0
        manager.complete_durable_recovery()
        assert manager.admission_ready is True
        assert await manager.async_route_observation(observation) == 1
        await engine.ofe.reset()

    asyncio.run(scenario())


def test_integrity_failure_keeps_global_admission_closed(
    region_manager_module,
):
    async def scenario():
        manager = region_manager_module.StormManager()
        manager.begin_durable_recovery()
        engine = manager.assign_target("home", 51.2, 4.4)
        engine.recovery_ready = True
        observation = SimpleNamespace(
            lat=51.0, lon=4.0, timestamp=time.time(),
            obs_type="test",
        )

        with pytest.raises(RuntimeError, match="integrity_check"):
            manager.verify_durable_integrity(integrity_healthy=False)

        assert manager.admission_ready is False
        assert await manager.async_route_observation(observation) == 0
        await engine.ofe.reset()

    asyncio.run(scenario())


def test_global_admission_requires_every_existing_region_recovered(
    region_manager_module,
):
    manager = region_manager_module.StormManager(
        sharing_distance_km=50
    )
    manager.begin_durable_recovery()
    first = manager.assign_target("home", 51.2, 4.4)
    second = manager.assign_target("remote", 40.7, -74.0)
    first.recovery_ready = True
    manager.verify_durable_integrity(integrity_healthy=True)

    with pytest.raises(RuntimeError, match=second.engine_id):
        manager.complete_durable_recovery()

    assert manager.admission_ready is False


def test_drain_waits_for_scheduled_push_ingest(
    region_manager_module,
):
    async def scenario():
        manager = region_manager_module.StormManager()
        engine = manager.assign_target("home", 51.2, 4.4)
        release = asyncio.Event()
        persisted = []

        async def persist(observations):
            await release.wait()
            persisted.extend(observations)
            return tuple(True for _ in observations)

        engine.ofe.configure_durable_ingest(persist)
        observation = SimpleNamespace(
            lat=51.0, lon=4.0, timestamp=time.time(),
            obs_type="test",
        )
        assert manager.route_observation(observation) == 1
        drain = asyncio.create_task(manager.async_drain_ingest())
        await asyncio.sleep(0)
        assert drain.done() is False
        release.set()
        await drain
        assert persisted == [observation]
        await engine.ofe.reset()

    asyncio.run(scenario())
