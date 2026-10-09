import concurrent.futures
import asyncio
from contextlib import closing
from dataclasses import replace
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest


def _observation(module, *, timestamp=1_700_000_000.0, station="KJFK"):
    return module.Observation(
        obs_type=module.ObservationType.RAIN,
        lat=40.6413,
        lon=-73.7781,
        timestamp=timestamp,
        received_at=timestamp + 3,
        rain_mm=2.4,
        station_id=station,
        rain_capable=True,
        precipitation_present=True,
        source="metar",
        raw_text="KJFK 291851Z RA",
        raw_payload={"weather": ["RA"], "quality": 1.0},
        quality_flags=("fresh",),
    )


@pytest.fixture
def durable_modules(observation_module, durable_processing_module):
    return observation_module, durable_processing_module


def _prepared_store(tmp_path, durable_modules):
    observation, durable = durable_modules
    store = durable.DurableProcessingStore(
        tmp_path / "calibration.sqlite3",
        build_id="test-build",
    )
    stats = store.initialize()
    assert stats["batches"] == 0
    obs = _observation(observation)
    batch = store.prepare_batch(
        region_id="40.64,-73.78,200",
        batch_id="a" * 32,
        observations=[obs],
        start_revision=0,
        start_state=[],
    )
    return observation, durable, store, obs, batch


def test_initialize_is_additive_and_uses_separate_tables(
    tmp_path, durable_modules
):
    _, durable = durable_modules
    path = tmp_path / "calibration.sqlite3"
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("CREATE TABLE calibration_sentinel(value TEXT)")
        db.execute("INSERT INTO calibration_sentinel VALUES('keep')")
    store = durable.DurableProcessingStore(path)
    store.initialize()
    store.initialize()
    with closing(sqlite3.connect(path)) as db, db:
        assert db.execute(
            "SELECT value FROM calibration_sentinel"
        ).fetchone() == ("keep",)
        tables = {
            row[0] for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
    assert {
        "durable_observations",
        "durable_ingest_queue",
        "observation_batches",
        "processing_attempts",
        "batch_state_transitions",
        "tracking_state_heads",
        "tracking_state_versions",
    } <= tables


def test_initialize_rejects_unknown_schema_without_overwriting_it(
    tmp_path, durable_modules
):
    _, durable = durable_modules
    path = tmp_path / "calibration.sqlite3"
    with closing(sqlite3.connect(path)) as db, db:
        db.execute(
            """
            CREATE TABLE durable_processing_metadata(
                key TEXT PRIMARY KEY, value TEXT NOT NULL
            )
            """
        )
        db.execute(
            """
            INSERT INTO durable_processing_metadata
            VALUES('schema_version', '1')
            """
        )
    store = durable.DurableProcessingStore(path)
    with pytest.raises(
        durable.DurableProcessingError, match="schema-versie"
    ):
        store.initialize()
    with closing(sqlite3.connect(path)) as db, db:
        assert db.execute(
            """
            SELECT value FROM durable_processing_metadata
            WHERE key='schema_version'
            """
        ).fetchone() == ("1",)
        assert db.execute(
            """
            SELECT count(*) FROM sqlite_master
            WHERE type='table' AND name='observation_batches'
            """
        ).fetchone() == (0,)


def test_raw_sqlite_error_is_marked_no_fallback(
    tmp_path, durable_modules
):
    _, durable = durable_modules
    invalid_database_path = tmp_path / "is-a-directory"
    invalid_database_path.mkdir()
    store = durable.DurableProcessingStore(invalid_database_path)
    with pytest.raises(durable.DurableProcessingError) as raised:
        store.initialize()
    assert raised.value.durable_no_fallback is True
    assert isinstance(raised.value.__cause__, sqlite3.Error)


def test_durable_region_identity_survives_jitter_and_changes_when_far(
    tmp_path, durable_modules
):
    _, durable = durable_modules
    store = durable.DurableProcessingStore(
        tmp_path / "calibration.sqlite3"
    )
    store.initialize()
    first = store.resolve_region_id(
        center_lat=40.7128,
        center_lon=-74.0060,
        observation_radius_km=300,
        sharing_distance_km=150,
        target_ids=["device_tracker.test"],
    )
    reopened = durable.DurableProcessingStore(store.path)
    reopened.initialize()
    jittered = reopened.resolve_region_id(
        center_lat=40.7131,
        center_lon=-74.0057,
        observation_radius_km=300,
        sharing_distance_km=150,
        target_ids=["device_tracker.test"],
    )
    moved_far = reopened.resolve_region_id(
        center_lat=51.2194,
        center_lon=4.4025,
        observation_radius_km=300,
        sharing_distance_km=150,
        target_ids=["device_tracker.test"],
    )
    assert jittered == first
    assert moved_far != first


def test_prepare_is_idempotent_and_roundtrips_observation(
    tmp_path, durable_modules
):
    _, durable, store, obs, first = _prepared_store(
        tmp_path, durable_modules
    )
    second = store.prepare_batch(
        region_id=first.region_id,
        batch_id=first.batch_id,
        observations=[obs],
        start_revision=0,
        start_state=[],
    )
    assert first.state == durable.STATE_PREPARED
    assert second.input_digest == first.input_digest
    assert second.observations == (obs,)
    assert store.statistics() == {
        "observations": 1,
        "received": 0,
        "deduplicated": 0,
        "batches": 1,
        "prepared": 1,
        "executing": 0,
        "result_staged": 0,
        "committed": 0,
        "quarantined": 0,
        "partitioned": 0,
    }


def test_deduplicated_ingest_is_terminal_and_never_recovers(
    tmp_path, durable_modules
):
    observation, durable = durable_modules
    store = durable.DurableProcessingStore(
        tmp_path / "calibration.sqlite3"
    )
    store.initialize()
    first = _observation(observation, station="KJFK")
    duplicate = _observation(
        observation,
        timestamp=first.timestamp + 0.5,
        station="KJFK",
    )
    assert store.receive_observations(
        region_id="new-york",
        observations=[first, duplicate],
        deduplicated=[False, True],
    ) == (True, True)
    assert store.recover_received_observations(
        region_id="new-york"
    ) == (first,)
    assert store.statistics()["deduplicated"] == 1


def test_received_observation_survives_restart_before_batching(
    tmp_path, durable_modules
):
    observation, durable = durable_modules
    store = durable.DurableProcessingStore(
        tmp_path / "calibration.sqlite3"
    )
    store.initialize()
    obs = _observation(observation)

    assert store.receive_observations(
        region_id="new-york",
        observations=[obs],
    ) == (True,)
    assert store.receive_observations(
        region_id="new-york",
        observations=[obs],
    ) == (False,)
    redelivered = replace(obs, received_at=obs.received_at + 30)
    assert store.receive_observations(
        region_id="new-york",
        observations=[redelivered],
    ) == (False,)

    reopened = durable.DurableProcessingStore(store.path)
    reopened.initialize()
    assert reopened.recover_received_observations(
        region_id="new-york"
    ) == (obs,)
    assert reopened.statistics()["received"] == 1


def test_prepare_binds_received_input_and_commit_closes_receipt(
    tmp_path, durable_modules
):
    observation, durable = durable_modules
    store = durable.DurableProcessingStore(
        tmp_path / "calibration.sqlite3"
    )
    store.initialize()
    obs = _observation(observation)
    store.receive_observations(
        region_id="new-york",
        observations=[obs],
    )
    batch = store.prepare_batch(
        region_id="new-york",
        batch_id="e" * 32,
        observations=[obs],
        start_revision=0,
        start_state=[],
    )
    assert store.statistics()["received"] == 0
    attempt = store.begin_attempt(
        region_id=batch.region_id,
        batch_id=batch.batch_id,
        executor="local",
        lease_seconds=10,
    )
    store.stage_result(
        region_id=batch.region_id,
        batch_id=batch.batch_id,
        attempt_id=attempt,
        snapshots=[],
    )
    store.commit_staged(
        region_id=batch.region_id,
        batch_id=batch.batch_id,
    )
    assert store.recover_received_observations(
        region_id=batch.region_id
    ) == ()


def test_partial_split_crash_recovers_unassigned_sibling_input(
    tmp_path, durable_modules
):
    observation, durable = durable_modules
    store = durable.DurableProcessingStore(
        tmp_path / "calibration.sqlite3"
    )
    store.initialize()
    first = _observation(observation, station="KJFK")
    second = _observation(
        observation,
        timestamp=1_700_000_060.0,
        station="KLGA",
    )
    store.receive_observations(
        region_id="new-york",
        observations=[first, second],
    )
    parent = store.prepare_batch(
        region_id="new-york",
        batch_id="f" * 32,
        observations=[first, second],
        start_revision=0,
        start_state=[],
    )
    child = store.prepare_batch(
        region_id="new-york",
        batch_id="1" * 32,
        root_batch_id=parent.batch_id,
        parent_batch_id=parent.batch_id,
        observations=[first],
        start_revision=0,
        start_state=[],
    )
    assert child.state == durable.STATE_PREPARED
    assert store.load_batch(
        region_id=parent.region_id,
        batch_id=parent.batch_id,
    ).state == durable.STATE_PARTITIONED

    reopened = durable.DurableProcessingStore(store.path)
    reopened.initialize()
    backlog = reopened.recover_after_restart(region_id="new-york")
    assert [item.batch_id for item in backlog] == [child.batch_id]
    assert reopened.recover_received_observations(
        region_id="new-york"
    ) == (second,)


def test_prepare_rejects_batch_id_with_other_input(
    tmp_path, durable_modules
):
    observation, durable, store, _, first = _prepared_store(
        tmp_path, durable_modules
    )
    with pytest.raises(durable.DurableBatchConflict):
        store.prepare_batch(
            region_id=first.region_id,
            batch_id=first.batch_id,
            observations=[
                _observation(
                    observation,
                    timestamp=1_700_000_060.0,
                )
            ],
            start_revision=0,
            start_state=[],
        )


def test_state_and_receipt_commit_are_atomic_and_idempotent(
    tmp_path, durable_modules
):
    _, durable, store, _, batch = _prepared_store(
        tmp_path, durable_modules
    )
    attempt = store.begin_attempt(
        region_id=batch.region_id,
        batch_id=batch.batch_id,
        executor="worker",
        lease_seconds=10,
        job_id="job-1",
    )
    snapshots = [{
        "storm_id": "storm-1",
        "last_update": 1_700_000_000.0,
    }]
    staged = store.stage_result(
        region_id=batch.region_id,
        batch_id=batch.batch_id,
        attempt_id=attempt,
        snapshots=snapshots,
    )
    assert staged.state == durable.STATE_RESULT_STAGED
    head = store.commit_staged(
        region_id=batch.region_id,
        batch_id=batch.batch_id,
    )
    duplicate = store.commit_staged(
        region_id=batch.region_id,
        batch_id=batch.batch_id,
    )
    assert head.revision == 1
    assert head.last_batch_id == batch.batch_id
    assert list(head.snapshots) == snapshots
    assert duplicate == head
    assert store.load_batch(
        region_id=batch.region_id,
        batch_id=batch.batch_id,
    ).state == durable.STATE_COMMITTED
    assert store.integrity_check()["healthy"] is True


def test_failed_attempt_returns_same_batch_to_prepared(
    tmp_path, durable_modules
):
    _, durable, store, _, batch = _prepared_store(
        tmp_path, durable_modules
    )
    attempt = store.begin_attempt(
        region_id=batch.region_id,
        batch_id=batch.batch_id,
        executor="worker",
        lease_seconds=10,
    )
    failed = store.fail_attempt(
        region_id=batch.region_id,
        batch_id=batch.batch_id,
        attempt_id=attempt,
        error_category="transport",
        error_detail="mqtt disconnected",
    )
    assert failed.state == durable.STATE_PREPARED
    retry = store.begin_attempt(
        region_id=batch.region_id,
        batch_id=batch.batch_id,
        executor="local",
        lease_seconds=10,
        retry_of=attempt,
    )
    assert retry and retry != attempt


def test_restart_releases_execution_lease_without_losing_input(
    tmp_path, durable_modules
):
    _, durable, store, obs, batch = _prepared_store(
        tmp_path, durable_modules
    )
    store.begin_attempt(
        region_id=batch.region_id,
        batch_id=batch.batch_id,
        executor="worker",
        lease_seconds=3600,
    )
    reopened = durable.DurableProcessingStore(store.path)
    reopened.initialize()
    backlog = reopened.recover_after_restart(region_id=batch.region_id)
    assert len(backlog) == 1
    assert backlog[0].state == durable.STATE_PREPARED
    assert backlog[0].batch_id == batch.batch_id
    assert backlog[0].observations == (obs,)


def test_staged_result_survives_restart_and_can_commit_without_recompute(
    tmp_path, durable_modules
):
    _, durable, store, _, batch = _prepared_store(
        tmp_path, durable_modules
    )
    attempt = store.begin_attempt(
        region_id=batch.region_id,
        batch_id=batch.batch_id,
        executor="worker",
        lease_seconds=10,
    )
    snapshots = [{"storm_id": "storm-staged"}]
    store.stage_result(
        region_id=batch.region_id,
        batch_id=batch.batch_id,
        attempt_id=attempt,
        snapshots=snapshots,
    )
    reopened = durable.DurableProcessingStore(store.path)
    reopened.initialize()
    backlog = reopened.recover_after_restart(region_id=batch.region_id)
    assert backlog[0].state == durable.STATE_RESULT_STAGED
    assert list(backlog[0].result_snapshots) == snapshots
    assert reopened.commit_staged(
        region_id=batch.region_id,
        batch_id=batch.batch_id,
    ).revision == 1


def test_quarantine_and_operator_retry_preserve_identity_and_lineage(
    tmp_path, durable_modules
):
    _, durable, store, _, batch = _prepared_store(
        tmp_path, durable_modules
    )
    quarantined = store.quarantine_batch(
        region_id=batch.region_id,
        batch_id=batch.batch_id,
        reason="poison record",
        lineage={"root": batch.root_batch_id, "epoch": 4},
    )
    assert quarantined.state == durable.STATE_QUARANTINED
    retried = store.retry_quarantined(region_id=batch.region_id)
    assert len(retried) == 1
    assert retried[0].batch_id == batch.batch_id
    assert retried[0].state == durable.STATE_PREPARED
    assert retried[0].lineage["epoch"] == 4


def test_quarantined_batch_rebases_after_newer_batch_commits(
    tmp_path, durable_modules
):
    observation, durable, store, first_obs, first = _prepared_store(
        tmp_path, durable_modules
    )
    store.quarantine_batch(
        region_id=first.region_id,
        batch_id=first.batch_id,
        reason="tijdelijk poison",
    )
    second = store.prepare_batch(
        region_id=first.region_id,
        batch_id="b" * 32,
        observations=[
            _observation(
                observation,
                timestamp=first_obs.timestamp + 60,
                station="KLGA",
            )
        ],
        start_revision=0,
        start_state=[],
    )
    second_attempt = store.begin_attempt(
        region_id=second.region_id,
        batch_id=second.batch_id,
        executor="local",
        lease_seconds=10,
    )
    second_state = [{"storm_id": "newer"}]
    store.stage_result(
        region_id=second.region_id,
        batch_id=second.batch_id,
        attempt_id=second_attempt,
        snapshots=second_state,
    )
    assert store.commit_staged(
        region_id=second.region_id,
        batch_id=second.batch_id,
    ).revision == 1

    store.retry_quarantined(region_id=first.region_id)
    rebased = store.prepare_batch(
        region_id=first.region_id,
        batch_id=first.batch_id,
        observations=[first_obs],
        start_revision=1,
        start_state=second_state,
    )
    assert rebased.start_revision == 1
    attempt = store.begin_attempt(
        region_id=rebased.region_id,
        batch_id=rebased.batch_id,
        executor="local",
        lease_seconds=10,
    )
    final_state = [{"storm_id": "newer"}, {"storm_id": "retried"}]
    store.stage_result(
        region_id=rebased.region_id,
        batch_id=rebased.batch_id,
        attempt_id=attempt,
        snapshots=final_state,
    )
    assert store.commit_staged(
        region_id=rebased.region_id,
        batch_id=rebased.batch_id,
    ).revision == 2


def test_only_one_concurrent_batch_can_commit_same_revision(
    tmp_path, durable_modules
):
    observation, durable, store, _, first = _prepared_store(
        tmp_path, durable_modules
    )
    second = store.prepare_batch(
        region_id=first.region_id,
        batch_id="b" * 32,
        observations=[
            _observation(observation, timestamp=1_700_000_060.0)
        ],
        start_revision=0,
        start_state=[],
    )
    for batch in (first, second):
        attempt = store.begin_attempt(
            region_id=batch.region_id,
            batch_id=batch.batch_id,
            executor="worker",
            lease_seconds=10,
        )
        store.stage_result(
            region_id=batch.region_id,
            batch_id=batch.batch_id,
            attempt_id=attempt,
            snapshots=[{"storm_id": batch.batch_id}],
        )

    def commit(batch_id):
        return store.commit_staged(
            region_id=first.region_id,
            batch_id=batch_id,
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        results = [
            pool.submit(commit, batch.batch_id)
            for batch in (first, second)
        ]
    outcomes = []
    for result in results:
        try:
            outcomes.append(result.result())
        except durable.DurableRevisionConflict as err:
            outcomes.append(err)
    assert sum(
        isinstance(item, durable.DurableTrackingHead)
        for item in outcomes
    ) == 1
    assert sum(
        isinstance(item, durable.DurableRevisionConflict)
        for item in outcomes
    ) == 1
    assert store.integrity_check()["healthy"] is True


def test_old_duplicate_commit_returns_its_own_version(
    tmp_path, durable_modules
):
    observation, durable, store, _, first = _prepared_store(
        tmp_path, durable_modules
    )
    first_attempt = store.begin_attempt(
        region_id=first.region_id,
        batch_id=first.batch_id,
        executor="local",
        lease_seconds=10,
    )
    first_result = [{"storm_id": "first"}]
    store.stage_result(
        region_id=first.region_id,
        batch_id=first.batch_id,
        attempt_id=first_attempt,
        snapshots=first_result,
    )
    store.commit_staged(
        region_id=first.region_id,
        batch_id=first.batch_id,
    )

    second = store.prepare_batch(
        region_id=first.region_id,
        batch_id="d" * 32,
        observations=[
            _observation(observation, timestamp=1_700_000_120.0)
        ],
        start_revision=1,
        start_state=first_result,
    )
    second_attempt = store.begin_attempt(
        region_id=second.region_id,
        batch_id=second.batch_id,
        executor="local",
        lease_seconds=10,
    )
    second_result = [{"storm_id": "second"}]
    store.stage_result(
        region_id=second.region_id,
        batch_id=second.batch_id,
        attempt_id=second_attempt,
        snapshots=second_result,
    )
    assert store.commit_staged(
        region_id=second.region_id,
        batch_id=second.batch_id,
    ).revision == 2

    replay = store.commit_staged(
        region_id=first.region_id,
        batch_id=first.batch_id,
    )
    assert replay.revision == 1
    assert replay.last_batch_id == first.batch_id
    assert list(replay.snapshots) == first_result
    assert store.load_tracking_head(first.region_id).revision == 2


def test_crash_after_db_commit_before_memory_install_recovers_exact_state(
    tmp_path,
    durable_modules,
    storm_engine_module,
):
    observation, durable = durable_modules
    store = durable.DurableProcessingStore(
        tmp_path / "calibration.sqlite3"
    )
    store.initialize()
    engine = storm_engine_module.StormEngine()
    obs = observation.Observation(
        obs_type=observation.ObservationType.LIGHTNING,
        lat=51.2,
        lon=4.4,
        timestamp=time.time(),
        source="blitzortung",
    )
    batch_id = "9" * 32
    store.receive_observations(
        region_id="antwerp",
        observations=[obs],
    )
    store.prepare_batch(
        region_id="antwerp",
        batch_id=batch_id,
        observations=[obs],
        start_revision=engine.revision,
        start_state=engine.export_tracking_state(),
    )
    attempt = store.begin_attempt(
        region_id="antwerp",
        batch_id=batch_id,
        executor="local",
        lease_seconds=10,
    )
    candidate = asyncio.run(engine.calculate_batch_candidate(
        [obs],
        expected_revision=0,
        batch_id=batch_id,
    ))
    store.stage_result(
        region_id="antwerp",
        batch_id=batch_id,
        attempt_id=attempt,
        snapshots=candidate,
    )
    committed = store.commit_staged(
        region_id="antwerp",
        batch_id=batch_id,
    )

    # Simuleer kill -9 exact na COMMIT en vóór de in-memory publicatie.
    assert engine.revision == 0
    assert engine.export_tracking_state() == []

    reopened_store = durable.DurableProcessingStore(store.path)
    reopened_store.initialize()
    recovered_head = reopened_store.load_tracking_head("antwerp")
    recovered_engine = storm_engine_module.StormEngine()
    recovered_engine.install_committed_tracking_state(
        list(recovered_head.snapshots),
        revision=recovered_head.revision,
        committed_batch_ids=(
            reopened_store.recent_committed_batch_ids("antwerp")
        ),
        publish=False,
    )
    assert recovered_head == committed
    assert recovered_engine.revision == 1
    assert recovered_engine.export_tracking_state() == candidate
    assert recovered_engine.has_applied_batch(batch_id) is True
    assert asyncio.run(recovered_engine.process_batch(
        [obs],
        expected_revision=1,
        batch_id=batch_id,
    )) is False
    assert recovered_engine.revision == 1


@pytest.mark.parametrize("checkpoint", [
    "receive_after_begin",
    "receive_after_observation",
    "receive_after_receipt",
    "receive_before_commit",
    "receive_after_commit",
])
def test_receive_fault_is_either_fully_absent_or_fully_durable(
    tmp_path, durable_modules, checkpoint
):
    observation, durable = durable_modules
    path = tmp_path / "calibration.sqlite3"
    base = durable.DurableProcessingStore(path)
    base.initialize()
    obs = _observation(observation)

    def fail(name):
        if name == checkpoint:
            raise RuntimeError(f"fault:{name}")

    crashing = durable.DurableProcessingStore(
        path, fault_injector=fail
    )
    with pytest.raises(RuntimeError, match="fault:"):
        crashing.receive_observations(
            region_id="new-york",
            observations=[obs],
        )

    reopened = durable.DurableProcessingStore(path)
    reopened.initialize()
    recovered = reopened.recover_received_observations(
        region_id="new-york"
    )
    assert recovered in ((), (obs,))
    assert reopened.integrity_check()["healthy"] is True


@pytest.mark.parametrize("checkpoint", [
    "commit_after_begin",
    "commit_after_version",
    "commit_after_head",
    "commit_after_receipt",
    "commit_after_ingest",
    "commit_before_commit",
    "commit_after_commit",
])
def test_commit_fault_never_creates_hybrid_state(
    tmp_path, durable_modules, checkpoint
):
    _, durable, base, _, batch = _prepared_store(
        tmp_path, durable_modules
    )
    attempt = base.begin_attempt(
        region_id=batch.region_id,
        batch_id=batch.batch_id,
        executor="local",
        lease_seconds=10,
    )
    base.stage_result(
        region_id=batch.region_id,
        batch_id=batch.batch_id,
        attempt_id=attempt,
        snapshots=[{"storm_id": "atomic"}],
    )

    def fail(name):
        if name == checkpoint:
            raise RuntimeError(f"fault:{name}")

    crashing = durable.DurableProcessingStore(
        base.path, fault_injector=fail
    )
    with pytest.raises(RuntimeError, match="fault:"):
        crashing.commit_staged(
            region_id=batch.region_id,
            batch_id=batch.batch_id,
        )

    reopened = durable.DurableProcessingStore(base.path)
    reopened.initialize()
    record = reopened.load_batch(
        region_id=batch.region_id,
        batch_id=batch.batch_id,
    )
    head = reopened.load_tracking_head(batch.region_id)
    assert (
        (record.state == durable.STATE_RESULT_STAGED and head.revision == 0)
        or (record.state == durable.STATE_COMMITTED and head.revision == 1)
    )
    assert reopened.integrity_check()["healthy"] is True


@pytest.mark.parametrize("checkpoint", [
    "receive_after_begin",
    "receive_after_observation",
    "receive_after_receipt",
    "receive_before_commit",
    "receive_after_commit",
])
def test_abrupt_process_exit_during_receive_is_atomic(
    tmp_path, durable_modules, checkpoint
):
    _, durable = durable_modules
    path = tmp_path / f"receive-{checkpoint}.sqlite3"
    durable.DurableProcessingStore(path).initialize()
    helper = (
        Path(__file__).resolve().parent
        / "helpers"
        / "durable_crash_child.py"
    )
    result = subprocess.run(
        [
            sys.executable,
            str(helper),
            str(path),
            "receive",
            checkpoint,
        ],
        check=False,
        timeout=15,
    )
    assert result.returncode == 73
    reopened = durable.DurableProcessingStore(path)
    reopened.initialize()
    recovered = reopened.recover_received_observations(
        region_id="new-york"
    )
    assert len(recovered) == (
        1 if checkpoint == "receive_after_commit" else 0
    )
    assert reopened.integrity_check()["healthy"] is True


@pytest.mark.parametrize("checkpoint", [
    "commit_after_begin",
    "commit_after_version",
    "commit_after_head",
    "commit_after_receipt",
    "commit_after_ingest",
    "commit_before_commit",
    "commit_after_commit",
])
def test_abrupt_process_exit_during_commit_is_atomic(
    tmp_path, durable_modules, checkpoint
):
    _, durable, store, _, batch = _prepared_store(
        tmp_path, durable_modules
    )
    attempt = store.begin_attempt(
        region_id=batch.region_id,
        batch_id=batch.batch_id,
        executor="worker",
        lease_seconds=10,
    )
    store.stage_result(
        region_id=batch.region_id,
        batch_id=batch.batch_id,
        attempt_id=attempt,
        snapshots=[{"storm_id": "durable-crash"}],
    )
    helper = (
        Path(__file__).resolve().parent
        / "helpers"
        / "durable_crash_child.py"
    )
    result = subprocess.run(
        [
            sys.executable,
            str(helper),
            str(store.path),
            "commit",
            checkpoint,
        ],
        check=False,
        timeout=15,
    )
    assert result.returncode == 73
    reopened = durable.DurableProcessingStore(store.path)
    reopened.initialize()
    record = reopened.load_batch(
        region_id=batch.region_id,
        batch_id=batch.batch_id,
    )
    head = reopened.load_tracking_head(batch.region_id)
    if checkpoint == "commit_after_commit":
        assert record.state == durable.STATE_COMMITTED
        assert head.revision == 1
    else:
        assert record.state == durable.STATE_RESULT_STAGED
        assert head.revision == 0
    assert reopened.integrity_check()["healthy"] is True
