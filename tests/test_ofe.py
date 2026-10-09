"""Storm Tracker V3 — tests/test_ofe.py v0.1.0

Tests voor engine/observation_fusion_engine.py — voorheen volledig
ongetest, ondanks dat dit de centrale dedup/batch-laag is waar ALLE
providers doorheen gaan.
"""
from __future__ import annotations

import asyncio
import random
import time

import pytest


def _obs(observation_module, obs_type, lat, lon, ts=None, **kw):
    return observation_module.Observation(
        obs_type=obs_type, lat=lat, lon=lon,
        timestamp=ts if ts is not None else time.time(),
        **kw,
    )


# ── Deduplicatie ────────────────────────────────────────────────────────────

def test_duplicate_lightning_same_position_is_dropped(ofe_module, observation_module):
    async def _noop(batch):
        return None

    async def _run():
        ofe = ofe_module.ObservationFusionEngine(on_batch=_noop)
        now = time.time()
        o1 = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.026, 4.478, ts=now)
        o2 = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.0261, 4.4781, ts=now + 0.1)
        await ofe.add_observation(o1)
        await ofe.add_observation(o2)
        return ofe

    ofe = asyncio.run(_run())
    assert len(ofe._buffer) == 1, "een bijna-identieke observatie binnen 1s/~111m moet als duplicaat gelden"


def test_different_type_same_position_is_not_a_duplicate(ofe_module, observation_module):
    async def _run():
        ofe = ofe_module.ObservationFusionEngine(on_batch=_noop)
        now = time.time()
        lightning = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.026, 4.478, ts=now)
        radar = _obs(observation_module, observation_module.ObservationType.RADAR, 51.026, 4.478, ts=now)
        await ofe.add_observation(lightning)
        await ofe.add_observation(radar)
        return ofe

    async def _noop(batch):
        return None

    ofe = asyncio.run(_run())
    assert len(ofe._buffer) == 2, "verschillende obs_type op dezelfde positie is GEEN duplicaat"


def test_rain_dedup_is_by_station_id(ofe_module, observation_module):
    async def _run():
        ofe = ofe_module.ObservationFusionEngine(on_batch=_noop)
        now = time.time()
        r1 = _obs(observation_module, observation_module.ObservationType.RAIN,
                  51.0, 4.0, ts=now, station_id="netatmo-1", rain_mm=0.5)
        # Zelfde station_id, ANDERE locatie -> toch duplicaat (dedup is op station_id voor RAIN)
        r2 = _obs(observation_module, observation_module.ObservationType.RAIN,
                  60.0, 10.0, ts=now + 0.1, station_id="netatmo-1", rain_mm=0.6)
        await ofe.add_observation(r1)
        await ofe.add_observation(r2)
        return ofe

    async def _noop(batch):
        return None

    ofe = asyncio.run(_run())
    assert len(ofe._buffer) == 1


def test_rain_different_station_ids_are_not_duplicates(ofe_module, observation_module):
    async def _run():
        ofe = ofe_module.ObservationFusionEngine(on_batch=_noop)
        now = time.time()
        r1 = _obs(observation_module, observation_module.ObservationType.RAIN,
                  51.0, 4.0, ts=now, station_id="station-a", rain_mm=0.5)
        r2 = _obs(observation_module, observation_module.ObservationType.RAIN,
                  51.0, 4.0, ts=now + 0.1, station_id="station-b", rain_mm=0.5)
        await ofe.add_observation(r1)
        await ofe.add_observation(r2)
        return ofe

    async def _noop(batch):
        return None

    ofe = asyncio.run(_run())
    assert len(ofe._buffer) == 2


def test_observation_outside_dedup_window_is_not_a_duplicate(ofe_module, observation_module):
    async def _run():
        ofe = ofe_module.ObservationFusionEngine(on_batch=_noop)
        now = time.time()
        o1 = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.026, 4.478, ts=now)
        # Zelfde positie, maar 5s later -> buiten DEDUP_WINDOW_S=1.0
        o2 = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.026, 4.478, ts=now + 5.0)
        await ofe.add_observation(o1)
        await ofe.add_observation(o2)
        return ofe

    async def _noop(batch):
        return None

    ofe = asyncio.run(_run())
    assert len(ofe._buffer) == 2, "buiten het dedup-venster moet dezelfde positie WEL als nieuw gelden"


# ── Buffer cleanup ──────────────────────────────────────────────────────────

def test_old_observations_are_pruned_from_buffer(ofe_module, observation_module):
    async def _run():
        ofe = ofe_module.ObservationFusionEngine(on_batch=_noop)
        very_old = time.time() - ofe_module.BUFFER_MAX_AGE_S - 60  # ruim ouder dan 1u
        recent = time.time()
        o_old = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.0, 4.0, ts=very_old)
        o_new = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 60.0, 10.0, ts=recent)
        await ofe.add_observation(o_old)
        await ofe.add_observation(o_new)
        return ofe

    async def _noop(batch):
        return None

    ofe = asyncio.run(_run())
    assert len(ofe._buffer) == 1
    assert ofe._buffer[0].lat == 60.0


# ── Batching ─────────────────────────────────────────────────────────────

def test_batch_is_flushed_after_batch_interval(ofe_module, observation_module):
    async def _run():
        received = []

        async def _on_batch(batch):
            received.append(batch)

        ofe = ofe_module.ObservationFusionEngine(on_batch=_on_batch)
        o1 = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.0, 4.0)
        o2 = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 60.0, 10.0, ts=time.time() + 2.0)
        await ofe.add_observation(o1)
        await ofe.add_observation(o2)
        # Wacht iets langer dan BATCH_INTERVAL_S zodat de flush-task afrondt
        await asyncio.sleep(ofe_module.BATCH_INTERVAL_S + 0.2)
        return received

    received = asyncio.run(_run())
    assert len(received) == 1, "beide observaties horen in ÉÉN batch te landen (binnen hetzelfde interval)"
    assert len(received[0]) == 2


def test_pending_batch_can_be_flushed_deterministically(
    ofe_module, observation_module,
):
    async def _run():
        received = []

        async def _on_batch(batch):
            received.append(batch)

        ofe = ofe_module.ObservationFusionEngine(on_batch=_on_batch)
        observation = _obs(
            observation_module,
            observation_module.ObservationType.RADAR,
            51.0,
            4.0,
        )
        await ofe.add_observation(observation)
        delivered = await ofe.async_flush_pending()
        await asyncio.sleep(0)
        return delivered, received, ofe

    delivered, received, ofe = asyncio.run(_run())
    assert delivered == 1
    assert len(received) == 1
    assert len(received[0]) == 1
    assert ofe._pending == []


def test_explicit_flush_cancellation_does_not_cancel_inflight_delivery(
    ofe_module, observation_module, monkeypatch,
):
    async def _run():
        monkeypatch.setattr(ofe_module, "BATCH_INTERVAL_S", 0.01)
        callback_started = asyncio.Event()
        release_callback = asyncio.Event()
        callback_cancelled = False
        received = []

        async def _on_batch(batch):
            nonlocal callback_cancelled
            callback_started.set()
            try:
                await release_callback.wait()
            except asyncio.CancelledError:
                callback_cancelled = True
                raise
            received.append(list(batch))

        ofe = ofe_module.ObservationFusionEngine(on_batch=_on_batch)
        observation = _obs(
            observation_module,
            observation_module.ObservationType.RADAR,
            51.0,
            4.0,
        )
        await ofe.add_observation(observation)
        background = ofe._batch_task
        await asyncio.wait_for(callback_started.wait(), timeout=1.0)

        explicit_flush = asyncio.create_task(ofe.async_flush_pending())
        await asyncio.sleep(0)
        explicit_flush.cancel()
        with pytest.raises(asyncio.CancelledError):
            await explicit_flush

        assert not background.done()
        assert callback_cancelled is False
        release_callback.set()
        delivered = await asyncio.wait_for(background, timeout=1.0)
        await ofe.reset()
        return delivered, observation, received, callback_cancelled, ofe

    delivered, observation, received, callback_cancelled, ofe = asyncio.run(
        _run()
    )
    assert delivered == 1
    assert received == [[observation]]
    assert callback_cancelled is False
    assert ofe._pending == []


def test_observation_added_during_successful_callback_is_delivered_next(
    ofe_module, observation_module, monkeypatch,
):
    async def _run():
        monkeypatch.setattr(ofe_module, "BATCH_INTERVAL_S", 60.0)
        callback_started = asyncio.Event()
        release_callback = asyncio.Event()
        received = []

        async def _on_batch(batch):
            received.append(list(batch))
            if len(received) == 1:
                callback_started.set()
                await release_callback.wait()

        ofe = ofe_module.ObservationFusionEngine(on_batch=_on_batch)
        first = _obs(
            observation_module,
            observation_module.ObservationType.RADAR,
            51.0,
            4.0,
        )
        second = _obs(
            observation_module,
            observation_module.ObservationType.RADAR,
            52.0,
            5.0,
            ts=time.time() + 2.0,
        )
        await ofe.add_observation(first)

        first_flush = asyncio.create_task(ofe.async_flush_pending())
        await asyncio.wait_for(callback_started.wait(), timeout=1.0)
        await ofe.add_observation(second)
        release_callback.set()

        first_delivered = await first_flush
        second_delivered = await ofe.async_flush_pending()
        third_delivered = await ofe.async_flush_pending()
        await ofe.reset()
        return (
            first_delivered,
            second_delivered,
            third_delivered,
            first,
            second,
            received,
        )

    first_count, second_count, third_count, first, second, received = asyncio.run(
        _run()
    )
    # Een expliciete flush draineert ook observaties die tijdens de callback
    # binnenkwamen, maar houdt ze als een afzonderlijke batch.
    assert (first_count, second_count, third_count) == (2, 0, 0)
    assert received == [[first], [second]]


def test_failed_callback_requeues_batch_before_newer_observations(
    ofe_module, observation_module, monkeypatch,
):
    async def _run():
        monkeypatch.setattr(ofe_module, "BATCH_INTERVAL_S", 60.0)
        callback_started = asyncio.Event()
        release_callback = asyncio.Event()
        attempts = 0
        attempted_ids = []
        successful_batches = []

        async def _on_batch(batch):
            nonlocal attempts
            attempts += 1
            attempted_ids.append(batch.batch_id)
            if attempts == 1:
                callback_started.set()
                await release_callback.wait()
                raise RuntimeError("tijdelijke Storm Engine-fout")
            successful_batches.append(list(batch))

        ofe = ofe_module.ObservationFusionEngine(on_batch=_on_batch)
        observations = [
            _obs(
                observation_module,
                observation_module.ObservationType.RADAR,
                51.0 + index,
                4.0 + index,
                ts=time.time() + index * 2.0,
            )
            for index in range(3)
        ]
        await ofe.add_observation(observations[0])
        await ofe.add_observation(observations[1])

        failed_flush = asyncio.create_task(ofe.async_flush_pending())
        await asyncio.wait_for(callback_started.wait(), timeout=1.0)
        await ofe.add_observation(observations[2])
        release_callback.set()

        failed_count = await failed_flush
        pending_after_failure = list(ofe._pending)
        retry_after_failure = [
            list(batch) for batch in ofe._retry_batches
        ]
        retry_count = await ofe.async_flush_pending()
        pending_after_success = list(ofe._pending)
        await ofe.reset()
        return (
            failed_count,
            retry_count,
            attempts,
            observations,
            pending_after_failure,
            retry_after_failure,
            pending_after_success,
            successful_batches,
            attempted_ids,
        )

    (
        failed_count,
        retry_count,
        attempts,
        observations,
        pending_after_failure,
        retry_after_failure,
        pending_after_success,
        successful_batches,
        attempted_ids,
    ) = asyncio.run(_run())
    assert failed_count == 0
    assert retry_count == 3
    assert attempts == 3
    assert pending_after_failure == observations[2:]
    assert retry_after_failure == [observations[:2]]
    assert pending_after_success == []
    assert successful_batches == [observations[:2], observations[2:]]
    assert attempted_ids[0] == attempted_ids[1]
    assert attempted_ids[2] != attempted_ids[0]


def test_failed_background_callback_is_retried_once_without_duplicate(
    ofe_module, observation_module, monkeypatch,
):
    async def _run():
        monkeypatch.setattr(ofe_module, "BATCH_INTERVAL_S", 0.01)
        monkeypatch.setattr(
            ofe_module, "BATCH_RETRY_BACKOFF_BASE_S", 0.01
        )
        monkeypatch.setattr(
            ofe_module, "BATCH_RETRY_BACKOFF_MAX_S", 0.02
        )
        delivered = asyncio.Event()
        attempts = []
        successful_batches = []

        async def _on_batch(batch):
            attempts.append(list(batch))
            if len(attempts) == 1:
                raise RuntimeError("tijdelijke fout")
            successful_batches.append(list(batch))
            delivered.set()

        ofe = ofe_module.ObservationFusionEngine(on_batch=_on_batch)
        observation = _obs(
            observation_module,
            observation_module.ObservationType.RADAR,
            51.0,
            4.0,
        )
        await ofe.add_observation(observation)
        await asyncio.wait_for(delivered.wait(), timeout=1.0)
        await asyncio.sleep(0.03)
        pending = list(ofe._pending)
        await ofe.reset()
        return observation, attempts, successful_batches, pending

    observation, attempts, successful_batches, pending = asyncio.run(_run())
    assert attempts == [[observation], [observation]]
    assert successful_batches == [[observation]]
    assert pending == []


def test_commit_then_throw_is_not_requeued(
    ofe_module, observation_module, monkeypatch,
):
    async def _run():
        monkeypatch.setattr(ofe_module, "BATCH_INTERVAL_S", 60.0)
        attempts = []

        async def _on_batch(batch):
            attempts.append(batch.batch_id)
            batch.acknowledge()
            raise RuntimeError("niet-kritieke fout na statecommit")

        ofe = ofe_module.ObservationFusionEngine(on_batch=_on_batch)
        observation = _obs(
            observation_module,
            observation_module.ObservationType.RADAR,
            51.0,
            4.0,
        )
        await ofe.add_observation(observation)
        delivered = await ofe.async_flush_pending()
        delivered_again = await ofe.async_flush_pending()
        retry_count = len(ofe._retry_batches)
        await ofe.reset()
        return delivered, delivered_again, attempts, retry_count

    delivered, delivered_again, attempts, retry_count = asyncio.run(_run())
    assert delivered == 1
    assert delivered_again == 0
    assert len(attempts) == 1
    assert retry_count == 0


def test_two_concurrent_flushes_deliver_one_batch_once(
    ofe_module, observation_module, monkeypatch,
):
    async def _run():
        monkeypatch.setattr(ofe_module, "BATCH_INTERVAL_S", 60.0)
        callback_started = asyncio.Event()
        release_callback = asyncio.Event()
        received_ids = []

        async def _on_batch(batch):
            received_ids.append(batch.batch_id)
            callback_started.set()
            await release_callback.wait()

        ofe = ofe_module.ObservationFusionEngine(on_batch=_on_batch)
        observation = _obs(
            observation_module,
            observation_module.ObservationType.LIGHTNING,
            51.0,
            4.0,
        )
        await ofe.add_observation(observation)
        first = asyncio.create_task(ofe.async_flush_pending())
        await asyncio.wait_for(callback_started.wait(), timeout=1.0)
        second = asyncio.create_task(ofe.async_flush_pending())
        await asyncio.sleep(0)
        release_callback.set()
        counts = await asyncio.gather(first, second)
        await ofe.reset()
        return counts, received_ids

    counts, received_ids = asyncio.run(_run())
    assert sum(counts) == 1
    assert len(received_ids) == 1


def test_cancelled_delivery_requeues_batch_before_newer_observations(
    ofe_module, observation_module, monkeypatch,
):
    async def _run():
        monkeypatch.setattr(ofe_module, "BATCH_INTERVAL_S", 60.0)
        callback_started = asyncio.Event()
        attempts = 0
        successful_batches = []

        async def _on_batch(batch):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                callback_started.set()
                await asyncio.Event().wait()
            successful_batches.append(list(batch))

        ofe = ofe_module.ObservationFusionEngine(on_batch=_on_batch)
        observations = [
            _obs(
                observation_module,
                observation_module.ObservationType.LIGHTNING,
                51.0 + index,
                4.0 + index,
                ts=time.time() + index * 2.0,
            )
            for index in range(3)
        ]
        await ofe.add_observation(observations[0])
        await ofe.add_observation(observations[1])

        background = ofe._batch_task
        background.cancel()
        with pytest.raises(asyncio.CancelledError):
            await background

        cancelled_flush = asyncio.create_task(ofe._deliver_pending())
        await asyncio.wait_for(callback_started.wait(), timeout=1.0)
        await ofe.add_observation(observations[2])
        cancelled_flush.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled_flush

        pending_after_cancellation = list(ofe._pending)
        retry_after_cancellation = [
            list(batch) for batch in ofe._retry_batches
        ]
        retry_count = await ofe.async_flush_pending()
        pending_after_success = list(ofe._pending)
        await ofe.reset()
        return (
            retry_count,
            attempts,
            observations,
            pending_after_cancellation,
            retry_after_cancellation,
            pending_after_success,
            successful_batches,
        )

    (
        retry_count,
        attempts,
        observations,
        pending_after_cancellation,
        retry_after_cancellation,
        pending_after_success,
        successful_batches,
    ) = asyncio.run(_run())
    assert retry_count == 3
    assert attempts == 3
    assert pending_after_cancellation == observations[2:]
    assert retry_after_cancellation == [observations[:2]]
    assert pending_after_success == []
    assert successful_batches == [observations[:2], observations[2:]]


def test_cancelled_background_callback_is_retried_automatically(
    ofe_module, observation_module, monkeypatch,
):
    async def _run():
        monkeypatch.setattr(ofe_module, "BATCH_INTERVAL_S", 0.01)
        callback_started = asyncio.Event()
        delivered = asyncio.Event()
        attempted_ids = []

        async def _on_batch(batch):
            attempted_ids.append(batch.batch_id)
            if len(attempted_ids) == 1:
                callback_started.set()
                await asyncio.Event().wait()
            delivered.set()

        ofe = ofe_module.ObservationFusionEngine(on_batch=_on_batch)
        observation = _obs(
            observation_module,
            observation_module.ObservationType.RADAR,
            51.0,
            4.0,
        )
        await ofe.add_observation(observation)
        await asyncio.wait_for(callback_started.wait(), timeout=1.0)

        background = ofe._batch_task
        background.cancel()
        with pytest.raises(asyncio.CancelledError):
            await background

        await asyncio.wait_for(delivered.wait(), timeout=1.0)
        await asyncio.sleep(0.03)
        retry_count = len(ofe._retry_batches)
        pending = list(ofe._pending)
        await ofe.reset()
        return attempted_ids, retry_count, pending

    attempted_ids, retry_count, pending = asyncio.run(_run())
    assert len(attempted_ids) == 2
    assert attempted_ids[0] == attempted_ids[1]
    assert retry_count == 0
    assert pending == []


def test_cancelled_sleeping_backoff_reschedules_without_new_input(
    ofe_module, observation_module, monkeypatch,
):
    async def _run():
        monkeypatch.setattr(ofe_module, "BATCH_INTERVAL_S", 0.005)
        monkeypatch.setattr(
            ofe_module, "BATCH_RETRY_BACKOFF_BASE_S", 0.05
        )
        monkeypatch.setattr(
            ofe_module, "BATCH_RETRY_BACKOFF_MAX_S", 0.05
        )
        attempts = []
        delivered = asyncio.Event()

        async def _on_batch(batch):
            attempts.append(batch.batch_id)
            if len(attempts) == 1:
                raise RuntimeError("tijdelijke fout vóór backoff")
            delivered.set()

        async def _wait_until(predicate):
            limit = time.monotonic() + 1.0
            while not predicate() and time.monotonic() < limit:
                await asyncio.sleep(0.005)
            assert predicate()

        ofe = ofe_module.ObservationFusionEngine(on_batch=_on_batch)
        observation = _obs(
            observation_module,
            observation_module.ObservationType.RADAR,
            51.0,
            4.0,
        )
        await ofe.add_observation(observation)
        await _wait_until(
            lambda: (
                ofe.delivery_diagnostics()["retry_batches"] == 1
                and ofe._batch_task is not None
                and not ofe._batch_task.done()
                and not ofe._batch_delivery_started
            )
        )

        sleeping_backoff = ofe._batch_task
        sleeping_backoff.cancel()
        with pytest.raises(asyncio.CancelledError):
            await sleeping_backoff

        await asyncio.wait_for(delivered.wait(), timeout=1.0)
        diagnostics = ofe.delivery_diagnostics()
        await ofe.reset()
        return attempts, diagnostics

    attempts, diagnostics = asyncio.run(_run())
    assert len(attempts) == 2
    assert attempts[0] == attempts[1]
    assert diagnostics["state"] == "healthy"
    assert diagnostics["retry_batches"] == 0


def test_poison_observation_does_not_block_newer_data_and_is_preserved(
    ofe_module, observation_module, monkeypatch,
):
    async def _run():
        monkeypatch.setattr(ofe_module, "BATCH_INTERVAL_S", 0.005)
        monkeypatch.setattr(
            ofe_module, "BATCH_RETRY_BACKOFF_BASE_S", 0.05
        )
        monkeypatch.setattr(
            ofe_module, "BATCH_RETRY_BACKOFF_MAX_S", 0.05
        )
        monkeypatch.setattr(
            ofe_module, "BATCH_SPLIT_AFTER_ATTEMPTS", 2
        )
        allow_poison = False
        attempts = []
        delivered = []

        async def _on_batch(batch):
            attempts.append((batch.batch_id, list(batch)))
            if (
                not allow_poison
                and any(item is poison for item in batch)
            ):
                raise ValueError("deterministische poison-observation")
            delivered.extend(batch)

        async def _wait_until(predicate):
            limit = time.monotonic() + 1.0
            while not predicate() and time.monotonic() < limit:
                await asyncio.sleep(0.005)
            assert predicate()

        ofe = ofe_module.ObservationFusionEngine(on_batch=_on_batch)
        poison = _obs(
            observation_module,
            observation_module.ObservationType.RADAR,
            51.0,
            4.0,
        )
        newer = _obs(
            observation_module,
            observation_module.ObservationType.RADAR,
            52.0,
            5.0,
            ts=time.time() + 2.0,
        )
        await ofe.add_observation(poison)
        await _wait_until(
            lambda: ofe.delivery_diagnostics()["retry_batches"] == 1
        )
        await ofe.add_observation(newer)
        await _wait_until(lambda: newer in delivered)
        await _wait_until(
            lambda: (
                ofe.delivery_diagnostics()["quarantined_batches"] == 1
            )
        )

        blocked = ofe.delivery_diagnostics()
        attempts_before_wait = len(attempts)
        await asyncio.sleep(0.08)
        attempts_after_wait = len(attempts)
        quarantined_id = ofe._quarantined_batches[0].batch_id

        allow_poison = True
        retried = await ofe.async_retry_quarantined()
        await _wait_until(lambda: poison in delivered)
        recovered = ofe.delivery_diagnostics()
        poison_attempt_ids = [
            batch_id
            for batch_id, batch in attempts
            if poison in batch
        ]
        newer_attempt_ids = [
            batch_id
            for batch_id, batch in attempts
            if newer in batch
        ]
        await ofe.reset()
        return (
            blocked,
            recovered,
            retried,
            attempts_before_wait,
            attempts_after_wait,
            quarantined_id,
            poison_attempt_ids,
            newer_attempt_ids,
        )

    (
        blocked,
        recovered,
        retried,
        attempts_before_wait,
        attempts_after_wait,
        quarantined_id,
        poison_attempt_ids,
        newer_attempt_ids,
    ) = asyncio.run(_run())
    assert blocked["state"] == "blocked_observations"
    assert blocked["quarantined_observations"] == 1
    assert blocked["pending_observations"] == 0
    assert blocked["retry_observations"] == 0
    assert "ValueError" in blocked["last_error"]
    assert attempts_after_wait == attempts_before_wait
    assert retried == 1
    assert recovered["state"] == "healthy"
    assert poison_attempt_ids == [
        quarantined_id, quarantined_id, quarantined_id,
    ]
    assert len(newer_attempt_ids) == 1
    assert newer_attempt_ids[0] != quarantined_id


def test_failed_multi_observation_batch_is_split_before_quarantine(
    ofe_module, observation_module, monkeypatch,
):
    async def _run():
        monkeypatch.setattr(ofe_module, "BATCH_INTERVAL_S", 0.01)
        monkeypatch.setattr(
            ofe_module, "BATCH_RETRY_BACKOFF_BASE_S", 0.01
        )
        monkeypatch.setattr(
            ofe_module, "BATCH_RETRY_BACKOFF_MAX_S", 0.01
        )
        monkeypatch.setattr(
            ofe_module, "BATCH_SPLIT_AFTER_ATTEMPTS", 2
        )
        attempts = []
        delivered = []

        async def _on_batch(batch):
            attempts.append(
                (batch.batch_id, batch.parent_batch_id, list(batch))
            )
            if any(item is poison for item in batch):
                raise ValueError("één poison-observation")
            delivered.extend(batch)

        async def _wait_until(predicate):
            limit = time.monotonic() + 1.0
            while not predicate() and time.monotonic() < limit:
                await asyncio.sleep(0.005)
            assert predicate()

        ofe = ofe_module.ObservationFusionEngine(on_batch=_on_batch)
        poison = _obs(
            observation_module,
            observation_module.ObservationType.RADAR,
            51.0,
            4.0,
        )
        valid = _obs(
            observation_module,
            observation_module.ObservationType.RADAR,
            52.0,
            5.0,
            ts=time.time() + 2.0,
        )
        await ofe.add_observation(poison)
        await ofe.add_observation(valid)
        await _wait_until(lambda: valid in delivered)
        await _wait_until(
            lambda: (
                ofe.delivery_diagnostics()["quarantined_observations"]
                == 1
            )
        )
        quarantined = list(ofe._quarantined_batches[0])
        await ofe.reset()
        return attempts, poison, valid, quarantined

    attempts, poison, valid, quarantined = asyncio.run(_run())
    combined = [attempt for attempt in attempts if len(attempt[2]) == 2]
    poison_only = [
        attempt
        for attempt in attempts
        if attempt[2] == [poison]
    ]
    valid_only = [
        attempt
        for attempt in attempts
        if attempt[2] == [valid]
    ]
    assert len(combined) == 2
    assert combined[0][0] == combined[1][0]
    assert len(poison_only) == 1
    assert poison_only[0][1] == combined[0][0]
    assert len(valid_only) == 1
    assert valid_only[0][1] == combined[0][0]
    assert quarantined == [poison]


def test_systemic_batch_failure_opens_bounded_lineage_circuit(
    ofe_module, observation_module, monkeypatch,
):
    async def _run():
        monkeypatch.setattr(ofe_module, "BATCH_INTERVAL_S", 0.005)
        monkeypatch.setattr(
            ofe_module, "BATCH_RETRY_BACKOFF_BASE_S", 0.005
        )
        monkeypatch.setattr(
            ofe_module, "BATCH_RETRY_BACKOFF_MAX_S", 0.005
        )
        monkeypatch.setattr(
            ofe_module, "BATCH_SPLIT_AFTER_ATTEMPTS", 2
        )
        monkeypatch.setattr(
            ofe_module, "BATCH_SYSTEMIC_ERROR_THRESHOLD", 4
        )
        monkeypatch.setattr(
            ofe_module, "BATCH_LINEAGE_FAILURE_BUDGET", 10
        )
        attempts = []

        async def _on_batch(batch):
            attempts.append(
                (batch.batch_id, batch.root_batch_id, len(batch))
            )
            raise ofe_module.SystemicBatchError(
                "systemische callbackfout"
            )

        async def _wait_until(predicate):
            limit = time.monotonic() + 1.0
            while not predicate() and time.monotonic() < limit:
                await asyncio.sleep(0.005)
            assert predicate()

        ofe = ofe_module.ObservationFusionEngine(on_batch=_on_batch)
        observations = [
            _obs(
                observation_module,
                observation_module.ObservationType.RADAR,
                50.0 + index,
                3.0 + index,
                ts=time.time() + index * 2.0,
            )
            for index in range(8)
        ]
        for observation in observations:
            await ofe.add_observation(observation)
        await _wait_until(
            lambda: (
                ofe.delivery_diagnostics()["quarantined_observations"]
                == len(observations)
            )
        )
        diagnostics = ofe.delivery_diagnostics()
        roots = {
            batch.root_batch_id
            for batch in ofe._quarantined_batches
        }
        attempts_before_wait = len(attempts)
        await asyncio.sleep(0.05)
        attempts_after_wait = len(attempts)
        await ofe.reset()
        return (
            diagnostics,
            roots,
            attempts_before_wait,
            attempts_after_wait,
        )

    (
        diagnostics,
        roots,
        attempts_before_wait,
        attempts_after_wait,
    ) = asyncio.run(_run())
    assert diagnostics["state"] == "blocked_observations"
    assert diagnostics["quarantined_observations"] == 8
    assert diagnostics["quarantined_root_batches"] == 1
    assert diagnostics["retry_observations"] == 0
    assert diagnostics["pending_observations"] == 0
    assert len(roots) == 1
    assert attempts_before_wait <= 6
    assert attempts_after_wait == attempts_before_wait


def test_transient_infrastructure_failure_keeps_root_batch_intact(
    ofe_module, observation_module, monkeypatch,
):
    async def _run():
        monkeypatch.setattr(ofe_module, "BATCH_INTERVAL_S", 60.0)
        monkeypatch.setattr(
            ofe_module, "INFRASTRUCTURE_RETRY_SECONDS", 60.0
        )
        attempts = []

        async def _on_batch(batch):
            attempts.append((batch.batch_id, len(batch), batch.attempts))
            raise ofe_module.TransientInfrastructureError("worker offline")

        engine = ofe_module.ObservationFusionEngine(on_batch=_on_batch)
        observations = [
            _obs(
                observation_module,
                observation_module.ObservationType.RADAR,
                50.0 + index * 0.1,
                3.0 + index * 0.1,
                ts=time.time() + index * 2.0,
            )
            for index in range(8)
        ]
        await engine.add_observations(observations)
        assert await engine._deliver_pending() == 0
        diagnostics = engine.delivery_diagnostics()
        queued = list(engine._retry_batches)
        await engine.reset()
        return attempts, observations, queued, diagnostics

    attempts, observations, queued, diagnostics = asyncio.run(_run())
    assert len(attempts) == 1
    assert attempts[0][1:] == (len(observations), 0)
    assert len(queued) == 1
    assert list(queued[0]) == observations
    assert queued[0].parent_batch_id is None
    assert queued[0].attempts == 0
    assert diagnostics["split_transitions_total"] == 0
    assert diagnostics["quarantined_observations"] == 0
    assert diagnostics["infrastructure_defer_transitions_total"] == 1
    assert diagnostics["retry_observations"] == len(observations)
    assert diagnostics["pending_observations"] == 0


def test_untyped_systemic_failure_stops_at_consecutive_lineage_budget(
    ofe_module, observation_module, monkeypatch,
):
    async def _run():
        monkeypatch.setattr(ofe_module, "BATCH_INTERVAL_S", 60.0)
        monkeypatch.setattr(
            ofe_module, "BATCH_RETRY_BACKOFF_BASE_S", 0.001
        )
        monkeypatch.setattr(
            ofe_module, "BATCH_RETRY_BACKOFF_MAX_S", 0.001
        )
        monkeypatch.setattr(
            ofe_module, "BATCH_SPLIT_AFTER_ATTEMPTS", 2
        )
        monkeypatch.setattr(
            ofe_module, "BATCH_LINEAGE_FAILURE_BUDGET", 10
        )
        attempts = []

        async def _on_batch(batch):
            attempts.append((batch.root_batch_id, len(batch)))
            raise RuntimeError("ongetypeerde systeemfout")

        async def _wait_until(predicate):
            limit = time.monotonic() + 1.0
            while not predicate() and time.monotonic() < limit:
                await asyncio.sleep(0.001)
            assert predicate()

        ofe = ofe_module.ObservationFusionEngine(on_batch=_on_batch)
        observations = [
            _obs(
                observation_module,
                observation_module.ObservationType.RADAR,
                40.0 + index * 0.01,
                -75.0,
                ts=time.time() + index * 2.0,
            )
            for index in range(64)
        ]
        for observation in observations:
            await ofe.add_observation(observation)
        await ofe.async_flush_pending()
        await _wait_until(
            lambda: (
                ofe.delivery_diagnostics()["quarantined_observations"]
                == len(observations)
            )
        )
        quarantined = [
            observation
            for batch in ofe._quarantined_batches
            for observation in batch
        ]
        diagnostics = ofe.delivery_diagnostics()
        await ofe.reset()
        return observations, attempts, quarantined, diagnostics

    observations, attempts, quarantined, diagnostics = asyncio.run(
        _run()
    )
    assert len(attempts) == 10
    assert len({root for root, _size in attempts}) == 1
    assert {id(item) for item in quarantined} == {
        id(item) for item in observations
    }
    assert diagnostics["retry_observations"] == 0
    assert diagnostics["pending_observations"] == 0
    assert diagnostics["quarantined_root_batches"] == 1


@pytest.mark.parametrize("poison_count", [2, 16])
def test_sparse_poison_observations_do_not_quarantine_valid_siblings(
    ofe_module, observation_module, monkeypatch, poison_count,
):
    """Gelijke poisonfouten mogen geen bewijs voor een rootfout zijn."""

    async def _run():
        monkeypatch.setattr(ofe_module, "BATCH_INTERVAL_S", 60.0)
        monkeypatch.setattr(
            ofe_module, "BATCH_RETRY_BACKOFF_BASE_S", 0.001
        )
        monkeypatch.setattr(
            ofe_module, "BATCH_RETRY_BACKOFF_MAX_S", 0.001
        )
        observation_count = 8000
        poison_indexes = {
            index * observation_count // poison_count
            for index in range(poison_count)
        }
        poison_object_ids = set()
        attempts = []
        delivered = []

        async def _on_batch(batch):
            attempts.append((batch.root_batch_id, len(batch)))
            if any(id(item) in poison_object_ids for item in batch):
                raise ValueError("dezelfde deterministische poisonfout")
            delivered.extend(batch)

        async def _wait_until(predicate):
            limit = time.monotonic() + 5.0
            while not predicate() and time.monotonic() < limit:
                await asyncio.sleep(0.001)
            assert predicate()

        ofe = ofe_module.ObservationFusionEngine(on_batch=_on_batch)
        observations = [
            observation_module.Observation(
                timestamp=time.time() + index * 2.0,
                lat=40.0 + (index % 80) * 0.01,
                lon=-75.0 + (index // 80) * 0.01,
                obs_type=observation_module.ObservationType.RADAR,
                source="sparse-poison-test",
            )
            for index in range(observation_count)
        ]
        poison_object_ids.update(
            id(observations[index]) for index in poison_indexes
        )
        for observation in observations:
            await ofe.add_observation(observation)
        await ofe.async_flush_pending()

        await _wait_until(
            lambda: (
                len(delivered) == observation_count - len(poison_indexes)
                and ofe.delivery_diagnostics()[
                    "quarantined_observations"
                ] == len(poison_indexes)
            )
        )
        quarantined = [
            observation
            for batch in ofe._quarantined_batches
            for observation in batch
        ]
        diagnostics = ofe.delivery_diagnostics()
        await ofe.reset()
        return (
            observations,
            poison_indexes,
            delivered,
            quarantined,
            diagnostics,
            attempts,
        )

    (
        observations,
        poison_indexes,
        delivered,
        quarantined,
        diagnostics,
        attempts,
    ) = asyncio.run(_run())
    poison = [
        observation
        for index, observation in enumerate(observations)
        if index in poison_indexes
    ]
    valid = [
        observation
        for index, observation in enumerate(observations)
        if index not in poison_indexes
    ]
    assert {id(observation) for observation in quarantined} == {
        id(observation) for observation in poison
    }
    assert {id(observation) for observation in delivered} == {
        id(observation) for observation in valid
    }
    assert len({id(observation) for observation in delivered}) == len(valid)
    assert (
        diagnostics["quarantined_observations"]
        == len(poison_indexes)
    )
    assert diagnostics["retry_observations"] == 0
    assert diagnostics["pending_observations"] == 0
    assert diagnostics["quarantined_root_batches"] == 1
    assert len({root for root, _size in attempts}) == 1


def test_dense_poison_pattern_is_processed_in_lossless_cooldown_epochs(
    ofe_module, observation_module, monkeypatch,
):
    async def _run():
        monkeypatch.setattr(ofe_module, "BATCH_INTERVAL_S", 60.0)
        monkeypatch.setattr(
            ofe_module, "BATCH_RETRY_BACKOFF_BASE_S", 0.001
        )
        monkeypatch.setattr(
            ofe_module, "BATCH_RETRY_BACKOFF_MAX_S", 0.001
        )
        monkeypatch.setattr(
            ofe_module, "BATCH_LINEAGE_ATTEMPT_BUDGET", 20
        )
        monkeypatch.setattr(
            ofe_module, "BATCH_LINEAGE_EPOCH_COOLDOWN_S", 0.002
        )
        attempts = []
        delivered = []
        observation_count = 64
        poison_ids = set()

        async def _on_batch(batch):
            attempts.append((time.monotonic(), batch.root_batch_id))
            if any(id(item) in poison_ids for item in batch):
                raise ValueError("afwisselende poison")
            delivered.extend(batch)

        async def _wait_until(predicate):
            limit = time.monotonic() + 2.0
            while not predicate() and time.monotonic() < limit:
                await asyncio.sleep(0.001)
            assert predicate()

        ofe = ofe_module.ObservationFusionEngine(on_batch=_on_batch)
        observations = [
            _obs(
                observation_module,
                observation_module.ObservationType.RADAR,
                40.0 + index * 0.01,
                -75.0,
                ts=time.time() + index * 2.0,
            )
            for index in range(observation_count)
        ]
        poison_ids.update(
            id(observation)
            for index, observation in enumerate(observations)
            if index % 2 == 0
        )
        for observation in observations:
            await ofe.add_observation(observation)
        await ofe.async_flush_pending()
        await _wait_until(
            lambda: (
                len(delivered) == observation_count // 2
                and ofe.delivery_diagnostics()[
                    "quarantined_observations"
                ] == observation_count // 2
                and ofe.delivery_diagnostics()["retry_observations"] == 0
            )
        )
        quarantined = [
            observation
            for batch in ofe._quarantined_batches
            for observation in batch
        ]
        lineage_epochs = max(
            batch._receipt.lineage.epochs
            for batch in ofe._quarantined_batches
        )
        diagnostics = ofe.delivery_diagnostics()
        await ofe.reset()
        return (
            observations,
            delivered,
            quarantined,
            attempts,
            lineage_epochs,
            diagnostics,
        )

    (
        observations,
        delivered,
        quarantined,
        attempts,
        lineage_epochs,
        diagnostics,
    ) = asyncio.run(_run())
    expected_valid = observations[1::2]
    expected_poison = observations[0::2]
    assert {id(item) for item in delivered} == {
        id(item) for item in expected_valid
    }
    assert {id(item) for item in quarantined} == {
        id(item) for item in expected_poison
    }
    assert len({id(item) for item in delivered}) == len(delivered)
    assert lineage_epochs > 1
    assert len(attempts) > ofe_module.BATCH_LINEAGE_ATTEMPT_BUDGET
    assert diagnostics["cooldown_observations"] == 0
    assert diagnostics["retry_observations"] == 0


def test_expensive_callbacks_are_spread_over_time_bounded_epochs(
    ofe_module, observation_module, monkeypatch,
):
    async def _run():
        monkeypatch.setattr(ofe_module, "BATCH_INTERVAL_S", 60.0)
        monkeypatch.setattr(
            ofe_module, "BATCH_RETRY_BACKOFF_BASE_S", 0.001
        )
        monkeypatch.setattr(
            ofe_module, "BATCH_RETRY_BACKOFF_MAX_S", 0.001
        )
        monkeypatch.setattr(
            ofe_module, "BATCH_LINEAGE_FAILURE_BUDGET", 999
        )
        monkeypatch.setattr(
            ofe_module, "BATCH_LINEAGE_ATTEMPT_BUDGET", 999
        )
        monkeypatch.setattr(
            ofe_module, "BATCH_LINEAGE_EPOCH_TIME_BUDGET_S", 0.01
        )
        monkeypatch.setattr(
            ofe_module, "BATCH_LINEAGE_EPOCH_COOLDOWN_S", 0.002
        )
        attempts = 0

        async def _on_batch(batch):
            nonlocal attempts
            attempts += 1
            await asyncio.sleep(0.006)
            raise ValueError("dure callbackfout")

        async def _wait_until(predicate):
            limit = time.monotonic() + 2.0
            while not predicate() and time.monotonic() < limit:
                await asyncio.sleep(0.001)
            assert predicate()

        ofe = ofe_module.ObservationFusionEngine(on_batch=_on_batch)
        observations = [
            _obs(
                observation_module,
                observation_module.ObservationType.RADAR,
                40.0 + index,
                -75.0,
                ts=time.time() + index * 2.0,
            )
            for index in range(8)
        ]
        for observation in observations:
            await ofe.add_observation(observation)
        await ofe.async_flush_pending()
        await _wait_until(
            lambda: (
                ofe.delivery_diagnostics()["quarantined_observations"]
                == len(observations)
                and ofe.delivery_diagnostics()["retry_observations"] == 0
            )
        )
        epochs = max(
            batch._receipt.lineage.epochs
            for batch in ofe._quarantined_batches
        )
        await ofe.reset()
        return attempts, epochs

    attempts, epochs = asyncio.run(_run())
    assert attempts > 2
    assert epochs > 1


def test_on_batch_exception_does_not_crash_engine(ofe_module, observation_module):
    """Een callbackfout blijft intern en de observatie blijft leverbaar."""
    async def _failing_on_batch(batch):
        raise RuntimeError("StormEngine crashte hypothetisch")

    async def _run():
        ofe = ofe_module.ObservationFusionEngine(on_batch=_failing_on_batch)
        o1 = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.0, 4.0)
        await ofe.add_observation(o1)
        await asyncio.sleep(ofe_module.BATCH_INTERVAL_S + 0.2)
        retry_batches = [
            list(batch) for batch in ofe._retry_batches
        ]
        await ofe.reset()
        return o1, retry_batches

    observation, retry_batches = asyncio.run(_run())
    assert retry_batches == [[observation]]


# ── observations_last_n_min / total_observations ──────────────────────────

def test_observations_last_n_min_filters_by_type_and_age(ofe_module, observation_module):
    async def _run():
        ofe = ofe_module.ObservationFusionEngine(on_batch=_noop)
        now = time.time()
        lightning = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.0, 4.0, ts=now)
        old_radar = _obs(observation_module, observation_module.ObservationType.RADAR, 60.0, 10.0, ts=now - 3600)
        await ofe.add_observation(lightning)
        await ofe.add_observation(old_radar)
        return ofe

    async def _noop(batch):
        return None

    ofe = asyncio.run(_run())
    recent_lightning = ofe.observations_last_n_min(5, observation_module.ObservationType.LIGHTNING)
    assert len(recent_lightning) == 1

    recent_radar = ofe.observations_last_n_min(5, observation_module.ObservationType.RADAR)
    assert len(recent_radar) == 0, "radar-observatie van 1u geleden mag niet binnen de laatste 5 min tellen"

    assert ofe.total_observations(5, observation_module.ObservationType.LIGHTNING) == 1


def test_reset_clears_old_region_and_cancels_pending_batch(
    ofe_module, observation_module
):
    async def _run():
        received = []

        async def _on_batch(batch):
            received.append(batch)

        ofe = ofe_module.ObservationFusionEngine(on_batch=_on_batch)
        observation = _obs(
            observation_module,
            observation_module.ObservationType.LIGHTNING,
            44.8378,
            -0.5792,
        )
        await ofe.add_observation(observation)
        await ofe.reset()
        await asyncio.sleep(ofe_module.BATCH_INTERVAL_S + 0.1)
        return ofe, received

    ofe, received = asyncio.run(_run())
    assert ofe._buffer == []
    assert ofe._pending == []
    assert received == []


def test_durable_bulk_ingest_happens_before_runtime_acceptance(
    ofe_module, observation_module
):
    persisted = []
    delivered = []

    async def persist(observations):
        persisted.append([item.station_id for item in observations])
        return (True, False)

    async def on_batch(batch):
        delivered.extend(item.station_id for item in batch)

    async def scenario():
        engine = ofe_module.ObservationFusionEngine(on_batch)
        engine.configure_durable_ingest(persist)
        now = time.time()
        first = _obs(
            observation_module,
            observation_module.ObservationType.RAIN,
            40.6,
            -73.8,
            ts=now,
            station_id="accepted",
            source="metar",
        )
        second = _obs(
            observation_module,
            observation_module.ObservationType.RAIN,
            40.7,
            -73.9,
            ts=now,
            station_id="duplicate",
            source="metar",
        )
        assert await engine.add_observations([first, second]) == 1
        assert persisted == [["accepted", "duplicate"]]
        assert await engine.async_flush_pending() == 1
        assert delivered == ["accepted"]
        await engine.reset()

    asyncio.run(scenario())


def test_classified_deduplication_is_persisted_atomically(
    ofe_module, observation_module
):
    classifications = []
    delivered = []

    async def persist(observations, *, deduplicated):
        classifications.append(tuple(deduplicated))
        return tuple(True for _ in observations)

    async def on_batch(batch):
        delivered.extend(batch)

    async def scenario():
        engine = ofe_module.ObservationFusionEngine(on_batch)
        engine.configure_durable_ingest(
            persist, classified_deduplication=True
        )
        now = time.time()
        first = _obs(
            observation_module,
            observation_module.ObservationType.LIGHTNING,
            40.7000,
            -74.0000,
            ts=now,
            source="blitzortung",
        )
        duplicate = _obs(
            observation_module,
            observation_module.ObservationType.LIGHTNING,
            40.7005,
            -74.0005,
            ts=now + 0.5,
            source="blitzortung",
        )
        assert await engine.add_observations([first, duplicate]) == 1
        assert classifications == [(False, True)]
        assert await engine.async_flush_pending() == 1
        assert delivered == [first]
        await engine.reset()

    asyncio.run(scenario())


def test_recovered_received_input_bypasses_second_persist(
    ofe_module, observation_module
):
    persist_calls = 0
    delivered = []

    async def persist(observations):
        nonlocal persist_calls
        persist_calls += 1
        return tuple(True for _ in observations)

    async def on_batch(batch):
        delivered.extend(batch)

    async def scenario():
        engine = ofe_module.ObservationFusionEngine(on_batch)
        engine.configure_durable_ingest(persist)
        observation = _obs(
            observation_module,
            observation_module.ObservationType.LIGHTNING,
            51.0,
            4.0,
            source="blitzortung",
        )
        assert await engine.async_restore_observations(
            [observation]
        ) == 1
        assert persist_calls == 0
        await engine.async_flush_pending()
        assert delivered == [observation]
        await engine.reset()

    asyncio.run(scenario())


def test_delivery_accounting_success_is_cumulative_and_side_effect_free(
    ofe_module, observation_module, monkeypatch,
):
    async def scenario():
        monkeypatch.setattr(ofe_module, "BATCH_INTERVAL_S", 60.0)
        delivered = []

        async def on_batch(batch):
            delivered.extend(batch)

        engine = ofe_module.ObservationFusionEngine(on_batch)
        engine._schedule_batch_locked = lambda: None
        now = time.time()
        observations = [
            _obs(
                observation_module,
                observation_module.ObservationType.RADAR,
                50.0 + index,
                4.0 + index,
                ts=now + index * 2.0,
            )
            for index in range(3)
        ]
        assert await engine.add_observations(observations) == 3
        before = engine.delivery_diagnostics()
        before_again = engine.delivery_diagnostics()
        assert before_again == before
        assert before["observations_offered_total"] == 3
        assert before["observations_outstanding_total"] == 3
        assert before["batches_offered_total"] == 0
        assert before["accounting_invariant_ok"] is True

        assert await engine.async_flush_pending() == 3
        after = engine.delivery_diagnostics()
        after_again = engine.delivery_accounting()
        assert {
            key: after[key] for key in after_again
        } == after_again
        await engine.reset()
        final = engine.delivery_accounting()
        return observations, delivered, after, final

    observations, delivered, after, final = asyncio.run(scenario())
    assert delivered == observations
    assert after["observations_delivered_total"] == 3
    assert after["observations_requeued_total"] == 0
    assert after["observations_discarded_total"] == 0
    assert after["observations_lost_total"] == 0
    assert after["batches_offered_total"] == 1
    assert after["batches_delivered_total"] == 1
    assert after["batches_acked_total"] == 1
    assert after["batches_requeued_total"] == 0
    assert after["batches_discarded_total"] == 0
    assert after["batches_lost_total"] == 0
    assert after["accounting_invariant_ok"] is True
    # Een reset na volledige delivery wist alleen de regiobuffer en houdt de
    # cumulatieve terminale boekhouding intact.
    assert final["observations_delivered_total"] == 3
    assert final["observations_discarded_total"] == 0
    assert final["batches_delivered_total"] == 1
    assert final["resets_total"] == 1
    assert final["accounting_invariant_ok"] is True


def test_retry_and_cancellation_transitions_do_not_reoffer_root(
    ofe_module, observation_module, monkeypatch,
):
    async def scenario():
        monkeypatch.setattr(ofe_module, "BATCH_INTERVAL_S", 60.0)
        monkeypatch.setattr(
            ofe_module, "BATCH_RETRY_BACKOFF_BASE_S", 0.0
        )
        monkeypatch.setattr(
            ofe_module, "BATCH_RETRY_BACKOFF_MAX_S", 0.0
        )
        second_started = asyncio.Event()
        attempts = 0
        delivered = []

        async def on_batch(batch):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("transient vóór commit")
            if attempts == 2:
                second_started.set()
                await asyncio.Event().wait()
            delivered.extend(batch)

        engine = ofe_module.ObservationFusionEngine(on_batch)
        engine._schedule_batch_locked = lambda: None
        now = time.time()
        observations = [
            _obs(
                observation_module,
                observation_module.ObservationType.RADAR,
                51.0 + index,
                4.0 + index,
                ts=now + index * 2.0,
            )
            for index in range(2)
        ]
        await engine.add_observations(observations)
        assert await engine._deliver_pending() == 0

        cancelled = asyncio.create_task(engine._deliver_pending())
        await asyncio.wait_for(second_started.wait(), timeout=1.0)
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled

        assert await engine._deliver_pending() == 2
        diagnostics = engine.delivery_diagnostics()
        await engine.reset()
        return observations, delivered, attempts, diagnostics

    observations, delivered, attempts, diagnostics = asyncio.run(
        scenario()
    )
    assert attempts == 3
    assert delivered == observations
    assert diagnostics["observations_offered_total"] == 2
    assert diagnostics["observations_delivered_total"] == 2
    assert diagnostics["observations_requeued_total"] == 4
    assert diagnostics["batches_offered_total"] == 1
    assert diagnostics["batches_delivered_total"] == 1
    assert diagnostics["batches_requeued_total"] == 2
    assert diagnostics["failure_retry_transitions_total"] == 1
    assert diagnostics["cancellation_requeue_transitions_total"] == 1
    assert diagnostics["split_transitions_total"] == 0
    assert diagnostics["observations_lost_total"] == 0
    assert diagnostics["batches_lost_total"] == 0
    assert diagnostics["accounting_invariant_ok"] is True


def test_unexpected_baseexception_is_requeued_before_propagation(
    ofe_module, observation_module, monkeypatch,
):
    class FatalCallbackSignal(BaseException):
        pass

    async def scenario():
        monkeypatch.setattr(ofe_module, "BATCH_INTERVAL_S", 60.0)
        attempts = 0
        delivered = []

        async def on_batch(batch):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise FatalCallbackSignal("fatal-but-recoverable liability")
            delivered.extend(batch)

        engine = ofe_module.ObservationFusionEngine(on_batch)
        engine._schedule_batch_locked = lambda: None
        observation = _obs(
            observation_module,
            observation_module.ObservationType.RADAR,
            51.0,
            4.0,
        )
        await engine.add_observation(observation)
        with pytest.raises(FatalCallbackSignal):
            await engine._deliver_pending()
        after_fatal = engine.delivery_diagnostics()
        assert await engine._deliver_pending() == 1
        recovered = engine.delivery_diagnostics()
        await engine.reset()
        return observation, delivered, after_fatal, recovered

    observation, delivered, after_fatal, recovered = asyncio.run(
        scenario()
    )
    assert delivered == [observation]
    assert after_fatal["fatal_requeue_transitions_total"] == 1
    assert after_fatal["batches_requeued_total"] == 1
    assert after_fatal["observations_requeued_total"] == 1
    assert after_fatal["observations_lost_total"] == 0
    assert after_fatal["batches_lost_total"] == 0
    assert after_fatal["accounting_invariant_ok"] is True
    assert recovered["observations_delivered_total"] == 1
    assert recovered["batches_delivered_total"] == 1
    assert recovered["observations_lost_total"] == 0
    assert recovered["batches_lost_total"] == 0
    assert recovered["accounting_invariant_ok"] is True


def test_precommit_callback_mutation_is_restored_before_retry(
    ofe_module, observation_module, monkeypatch,
):
    async def scenario():
        monkeypatch.setattr(ofe_module, "BATCH_INTERVAL_S", 60.0)
        monkeypatch.setattr(
            ofe_module, "BATCH_RETRY_BACKOFF_BASE_S", 0.0
        )
        monkeypatch.setattr(
            ofe_module, "BATCH_RETRY_BACKOFF_MAX_S", 0.0
        )
        attempts = 0
        delivered = []

        async def on_batch(batch):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                batch.pop()
                batch.reverse()
                raise RuntimeError("callback muteerde vóór commit")
            delivered.extend(batch)

        engine = ofe_module.ObservationFusionEngine(on_batch)
        engine._schedule_batch_locked = lambda: None
        now = time.time()
        observations = [
            _obs(
                observation_module,
                observation_module.ObservationType.RADAR,
                45.0 + index,
                3.0 + index,
                ts=now + index * 2.0,
            )
            for index in range(3)
        ]
        await engine.add_observations(observations)
        assert await engine._deliver_pending() == 0
        assert await engine._deliver_pending() == 3
        diagnostics = engine.delivery_diagnostics()
        await engine.reset()
        return observations, delivered, diagnostics

    observations, delivered, diagnostics = asyncio.run(scenario())
    assert delivered == observations
    assert diagnostics["observations_offered_total"] == 3
    assert diagnostics["observations_delivered_total"] == 3
    assert diagnostics["observations_requeued_total"] == 3
    assert diagnostics["observations_lost_total"] == 0
    assert diagnostics["batches_delivered_total"] == 1
    assert diagnostics["batches_lost_total"] == 0
    assert diagnostics["accounting_invariant_ok"] is True


def test_split_and_manual_retry_share_one_root_account(
    ofe_module, observation_module, monkeypatch,
):
    async def scenario():
        monkeypatch.setattr(ofe_module, "BATCH_INTERVAL_S", 60.0)
        monkeypatch.setattr(
            ofe_module, "BATCH_SPLIT_AFTER_ATTEMPTS", 1
        )
        allow_poison = False
        delivered = []

        async def on_batch(batch):
            if not allow_poison and poison in batch:
                raise ValueError("poison")
            delivered.extend(batch)

        engine = ofe_module.ObservationFusionEngine(on_batch)
        engine._schedule_batch_locked = lambda: None
        now = time.time()
        observations = [
            _obs(
                observation_module,
                observation_module.ObservationType.RADAR,
                48.0 + index,
                2.0 + index,
                ts=now + index * 2.0,
            )
            for index in range(4)
        ]
        poison = observations[0]
        await engine.add_observations(observations)
        for _ in range(5):
            await engine._deliver_pending()
        blocked = engine.delivery_diagnostics()

        allow_poison = True
        assert await engine.async_retry_quarantined() == 1
        assert await engine._deliver_pending() == 1
        recovered = engine.delivery_diagnostics()
        await engine.reset()
        return observations, poison, delivered, blocked, recovered

    observations, poison, delivered, blocked, recovered = asyncio.run(
        scenario()
    )
    assert {id(item) for item in delivered} == {
        id(item) for item in observations
    }
    assert len(delivered) == len(observations)
    assert poison not in delivered[:-1]
    assert blocked["observations_offered_total"] == 4
    assert blocked["observations_delivered_total"] == 3
    assert blocked["observations_outstanding_total"] == 1
    assert blocked["batches_offered_total"] == 1
    assert blocked["batches_delivered_total"] == 0
    assert blocked["batches_outstanding_total"] == 1
    assert blocked["batches_requeued_total"] == 4
    assert blocked["observations_requeued_total"] == 6
    assert blocked["split_transitions_total"] == 2
    assert blocked["accounting_invariant_ok"] is True

    assert recovered["observations_offered_total"] == 4
    assert recovered["observations_delivered_total"] == 4
    assert recovered["batches_offered_total"] == 1
    assert recovered["batches_delivered_total"] == 1
    assert recovered["batches_requeued_total"] == 5
    assert recovered["observations_requeued_total"] == 7
    assert recovered["manual_requeue_transitions_total"] == 1
    assert recovered["observations_lost_total"] == 0
    assert recovered["batches_lost_total"] == 0
    assert recovered["accounting_invariant_ok"] is True


def test_restore_groups_sibling_leaves_as_one_runtime_root(
    ofe_module, observation_module, monkeypatch,
):
    async def scenario():
        monkeypatch.setattr(ofe_module, "BATCH_INTERVAL_S", 60.0)
        delivered = []

        async def on_batch(batch):
            delivered.extend(batch)

        engine = ofe_module.ObservationFusionEngine(on_batch)
        engine._schedule_batch_locked = lambda: None
        now = time.time()
        first = _obs(
            observation_module,
            observation_module.ObservationType.RADAR,
            51.0,
            4.0,
            ts=now,
        )
        second = _obs(
            observation_module,
            observation_module.ObservationType.RADAR,
            52.0,
            5.0,
            ts=now + 2.0,
        )
        left = ofe_module.ObservationBatch(
            [first],
            batch_id="left",
            parent_batch_id="parent",
            root_batch_id="root",
        )
        right = ofe_module.ObservationBatch(
            [second],
            batch_id="right",
            parent_batch_id="parent",
            root_batch_id="root",
        )
        assert await engine.async_restore_batches([left, right]) == 2
        restored = engine.delivery_diagnostics()
        assert await engine.async_flush_pending() == 2
        completed = engine.delivery_diagnostics()
        await engine.reset()
        return first, second, delivered, restored, completed

    first, second, delivered, restored, completed = asyncio.run(
        scenario()
    )
    assert delivered == [first, second]
    assert restored["observations_offered_total"] == 2
    assert restored["observations_requeued_total"] == 2
    assert restored["batches_offered_total"] == 1
    assert restored["batches_requeued_total"] == 2
    assert restored["recovery_requeue_transitions_total"] == 2
    assert restored["batches_outstanding_total"] == 1
    assert restored["resident_root_batches"] == 1
    assert restored["accounting_invariant_ok"] is True
    assert completed["observations_delivered_total"] == 2
    assert completed["batches_delivered_total"] == 1
    assert completed["batches_outstanding_total"] == 0
    assert completed["accounting_invariant_ok"] is True


def test_restore_cannot_duplicate_an_inflight_batch(
    ofe_module, observation_module, monkeypatch,
):
    async def scenario():
        monkeypatch.setattr(ofe_module, "BATCH_INTERVAL_S", 60.0)
        callback_started = asyncio.Event()
        callback_release = asyncio.Event()
        seen_batches = []
        delivered = []

        async def on_batch(batch):
            seen_batches.append(batch)
            callback_started.set()
            await callback_release.wait()
            delivered.extend(batch)

        engine = ofe_module.ObservationFusionEngine(on_batch)
        engine._schedule_batch_locked = lambda: None
        observation = _obs(
            observation_module,
            observation_module.ObservationType.RADAR,
            51.0,
            4.0,
        )
        await engine.add_observation(observation)
        delivery = asyncio.create_task(engine._deliver_pending())
        await asyncio.wait_for(callback_started.wait(), timeout=1.0)
        assert await engine.async_restore_batches(seen_batches) == 0
        callback_release.set()
        assert await delivery == 1
        diagnostics = engine.delivery_diagnostics()
        await engine.reset()
        return observation, delivered, seen_batches, diagnostics

    observation, delivered, seen_batches, diagnostics = asyncio.run(
        scenario()
    )
    assert delivered == [observation]
    assert len(seen_batches) == 1
    assert diagnostics["observations_offered_total"] == 1
    assert diagnostics["observations_delivered_total"] == 1
    assert diagnostics["observations_requeued_total"] == 0
    assert diagnostics["batches_offered_total"] == 1
    assert diagnostics["batches_delivered_total"] == 1
    assert diagnostics["batches_requeued_total"] == 0
    assert diagnostics["recovery_requeue_transitions_total"] == 0
    assert diagnostics["accounting_invariant_ok"] is True


def test_reset_accounts_pending_and_inflight_as_explicit_discard(
    ofe_module, observation_module, monkeypatch,
):
    async def scenario():
        monkeypatch.setattr(ofe_module, "BATCH_INTERVAL_S", 60.0)
        callback_started = asyncio.Event()
        release_callback = asyncio.Event()

        async def on_batch(batch):
            callback_started.set()
            await release_callback.wait()
            raise RuntimeError("oude regio faalt tijdens teardown")

        engine = ofe_module.ObservationFusionEngine(on_batch)
        engine._schedule_batch_locked = lambda: None
        now = time.time()
        pending = _obs(
            observation_module,
            observation_module.ObservationType.RADAR,
            50.0,
            3.0,
            ts=now,
        )
        await engine.add_observation(pending)
        await engine.reset()
        after_pending_reset = engine.delivery_diagnostics()

        inflight = [
            _obs(
                observation_module,
                observation_module.ObservationType.RADAR,
                55.0 + index,
                8.0 + index,
                ts=now + 10.0 + index * 2.0,
            )
            for index in range(2)
        ]
        await engine.add_observations(inflight)
        delivery = asyncio.create_task(engine._deliver_pending())
        await asyncio.wait_for(callback_started.wait(), timeout=1.0)
        reset_task = asyncio.create_task(engine.reset())
        while not engine._resetting:
            await asyncio.sleep(0)
        release_callback.set()
        assert await delivery == 0
        await reset_task
        after_inflight_reset = engine.delivery_diagnostics()
        return after_pending_reset, after_inflight_reset

    after_pending, after_inflight = asyncio.run(scenario())
    assert after_pending["observations_offered_total"] == 1
    assert after_pending["observations_discarded_total"] == 1
    assert after_pending["batches_offered_total"] == 0
    assert after_pending["batches_discarded_total"] == 0
    assert (
        after_pending["reset_discarded_pending_observations_total"]
        == 1
    )
    assert after_pending["reset_discard_events_total"] == 1
    assert after_pending["accounting_invariant_ok"] is True

    assert after_inflight["observations_offered_total"] == 3
    assert after_inflight["observations_discarded_total"] == 3
    assert after_inflight["batches_offered_total"] == 1
    assert after_inflight["batches_discarded_total"] == 1
    assert (
        after_inflight["reset_discarded_pending_observations_total"]
        == 1
    )
    assert (
        after_inflight["reset_discarded_batched_observations_total"]
        == 2
    )
    assert after_inflight["reset_discarded_batches_total"] == 1
    assert after_inflight["reset_discard_events_total"] == 2
    assert after_inflight["resets_total"] == 2
    assert after_inflight["observations_lost_total"] == 0
    assert after_inflight["batches_lost_total"] == 0
    assert after_inflight["accounting_invariant_ok"] is True


def test_reset_cancels_background_callback_as_discard_not_loss(
    ofe_module, observation_module, monkeypatch,
):
    async def scenario():
        monkeypatch.setattr(ofe_module, "BATCH_INTERVAL_S", 0.001)
        callback_started = asyncio.Event()

        async def on_batch(batch):
            callback_started.set()
            await asyncio.Event().wait()

        engine = ofe_module.ObservationFusionEngine(on_batch)
        observation = _obs(
            observation_module,
            observation_module.ObservationType.RADAR,
            50.0,
            4.0,
        )
        await engine.add_observation(observation)
        await asyncio.wait_for(callback_started.wait(), timeout=1.0)
        await engine.reset()
        return engine.delivery_diagnostics()

    diagnostics = asyncio.run(scenario())
    assert diagnostics["observations_offered_total"] == 1
    assert diagnostics["observations_discarded_total"] == 1
    assert diagnostics["observations_lost_total"] == 0
    assert diagnostics["batches_offered_total"] == 1
    assert diagnostics["batches_discarded_total"] == 1
    assert diagnostics["batches_lost_total"] == 0
    assert diagnostics["reset_discarded_batched_observations_total"] == 1
    assert diagnostics["reset_discarded_batches_total"] == 1
    assert diagnostics["reset_discard_events_total"] == 1
    assert diagnostics["accounting_invariant_ok"] is True


def test_concurrent_resets_serialize_one_real_discard_event(
    ofe_module, observation_module, monkeypatch,
):
    async def scenario():
        monkeypatch.setattr(ofe_module, "BATCH_INTERVAL_S", 60.0)

        async def on_batch(batch):
            return None

        engine = ofe_module.ObservationFusionEngine(on_batch)
        engine._schedule_batch_locked = lambda: None
        observation = _obs(
            observation_module,
            observation_module.ObservationType.RADAR,
            50.0,
            4.0,
        )
        await engine.add_observation(observation)
        await asyncio.gather(engine.reset(), engine.reset())
        return engine.delivery_diagnostics()

    diagnostics = asyncio.run(scenario())
    assert diagnostics["resets_total"] == 2
    assert diagnostics["reset_discard_events_total"] == 1
    assert diagnostics["observations_offered_total"] == 1
    assert diagnostics["observations_discarded_total"] == 1
    assert diagnostics["observations_lost_total"] == 0
    assert diagnostics["batches_offered_total"] == 0
    assert diagnostics["batches_discarded_total"] == 0
    assert diagnostics["accounting_invariant_ok"] is True


def test_cancelled_reset_finishes_teardown_before_propagating(
    ofe_module, observation_module, monkeypatch,
):
    async def scenario():
        monkeypatch.setattr(ofe_module, "BATCH_INTERVAL_S", 60.0)
        callback_started = asyncio.Event()
        callback_release = asyncio.Event()

        async def on_batch(batch):
            callback_started.set()
            await callback_release.wait()
            raise RuntimeError("teardown blijft leidend")

        engine = ofe_module.ObservationFusionEngine(on_batch)
        engine._schedule_batch_locked = lambda: None
        observation = _obs(
            observation_module,
            observation_module.ObservationType.RADAR,
            50.0,
            4.0,
        )
        await engine.add_observation(observation)
        delivery = asyncio.create_task(engine._deliver_pending())
        await asyncio.wait_for(callback_started.wait(), timeout=1.0)
        reset_task = asyncio.create_task(engine.reset())
        while not engine._resetting:
            await asyncio.sleep(0)
        reset_task.cancel()
        callback_release.set()
        assert await delivery == 0
        with pytest.raises(asyncio.CancelledError):
            await reset_task
        return engine, engine.delivery_diagnostics()

    engine, diagnostics = asyncio.run(scenario())
    assert engine._resetting is False
    assert engine._pending == []
    assert not engine._retry_batches
    assert not engine._quarantined_batches
    assert diagnostics["resets_total"] == 1
    assert diagnostics["reset_discard_events_total"] == 1
    assert diagnostics["observations_discarded_total"] == 1
    assert diagnostics["batches_discarded_total"] == 1
    assert diagnostics["observations_lost_total"] == 0
    assert diagnostics["batches_lost_total"] == 0
    assert diagnostics["accounting_invariant_ok"] is True


def test_seeded_randomized_concurrency_retry_split_cancel_and_reset_accounting(
    ofe_module, observation_module, monkeypatch,
):
    """Property-run: iedere offered observation eindigt exact één keer."""

    async def scenario():
        monkeypatch.setattr(ofe_module, "BATCH_INTERVAL_S", 60.0)
        monkeypatch.setattr(
            ofe_module, "BATCH_RETRY_BACKOFF_BASE_S", 0.0
        )
        monkeypatch.setattr(
            ofe_module, "BATCH_RETRY_BACKOFF_MAX_S", 0.0
        )
        monkeypatch.setattr(
            ofe_module, "BATCH_SPLIT_AFTER_ATTEMPTS", 2
        )
        rng = random.Random(20260730)
        base_modes = [
            "success",
            "transient",
            "cancel",
            "split",
            "reset_pending",
            "reset_inflight",
        ]
        modes = base_modes + [
            rng.choice(base_modes) for _ in range(30)
        ]
        rng.shuffle(modes)
        round_by_observation = {}
        attempts_by_root = {}
        allow_poison = set()
        poison_ids = {}
        callback_started = {
            index: asyncio.Event() for index in range(len(modes))
        }
        callback_release = {
            index: asyncio.Event() for index in range(len(modes))
        }
        delivered_ids = []

        async def on_batch(batch):
            round_index = round_by_observation[id(batch[0])]
            mode = modes[round_index]
            attempts = attempts_by_root.get(batch.root_batch_id, 0) + 1
            attempts_by_root[batch.root_batch_id] = attempts
            if mode == "transient" and attempts == 1:
                raise RuntimeError("seeded transient")
            if mode == "cancel" and attempts == 1:
                callback_started[round_index].set()
                await asyncio.Event().wait()
            if (
                mode == "split"
                and round_index not in allow_poison
                and poison_ids[round_index] in {
                    id(item) for item in batch
                }
            ):
                raise ValueError("seeded poison")
            if mode == "reset_inflight" and attempts == 1:
                callback_started[round_index].set()
                await callback_release[round_index].wait()
                raise RuntimeError("seeded teardown failure")
            delivered_ids.extend(id(item) for item in batch)

        engine = ofe_module.ObservationFusionEngine(on_batch)
        engine._schedule_batch_locked = lambda: None
        offered = 0
        expected_delivered = 0
        expected_discarded = 0
        expected_root_offered = 0
        expected_root_delivered = 0
        expected_root_discarded = 0
        reset_modes = 0
        all_observations = []
        now = time.time()

        async def drain_concurrently(limit=64):
            for _ in range(limit):
                diagnostics = engine.delivery_diagnostics()
                if (
                    diagnostics["pending_observations"] == 0
                    and diagnostics["retry_batches"] == 0
                    and diagnostics["inflight_batches"] == 0
                ):
                    return
                await asyncio.gather(
                    *(engine._deliver_pending() for _ in range(4))
                )
            raise AssertionError("seeded randomized drain bleef actief")

        for round_index, mode in enumerate(modes):
            size = rng.randint(1, 8)
            observations = [
                _obs(
                    observation_module,
                    observation_module.ObservationType.RADAR,
                    20.0 + round_index * 0.1 + item_index * 0.002,
                    -130.0 + round_index * 0.1,
                    ts=now + round_index * 30.0 + item_index * 2.0,
                )
                for item_index in range(size)
            ]
            rng.shuffle(observations)
            all_observations.extend(observations)
            for observation in observations:
                round_by_observation[id(observation)] = round_index
            poison_ids[round_index] = id(rng.choice(observations))
            added = await asyncio.gather(
                *(engine.add_observation(item) for item in observations)
            )
            assert added == [None] * size
            offered += size

            if mode == "reset_pending":
                await engine.reset()
                expected_discarded += size
                reset_modes += 1
            elif mode == "reset_inflight":
                delivery = asyncio.create_task(engine._deliver_pending())
                await asyncio.wait_for(
                    callback_started[round_index].wait(), timeout=1.0
                )
                reset_task = asyncio.create_task(engine.reset())
                while not engine._resetting:
                    await asyncio.sleep(0)
                callback_release[round_index].set()
                assert await delivery == 0
                await reset_task
                expected_discarded += size
                expected_root_offered += 1
                expected_root_discarded += 1
                reset_modes += 1
            elif mode == "cancel":
                delivery = asyncio.create_task(engine._deliver_pending())
                await asyncio.wait_for(
                    callback_started[round_index].wait(), timeout=1.0
                )
                delivery.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await delivery
                await drain_concurrently()
                expected_delivered += size
                expected_root_offered += 1
                expected_root_delivered += 1
            elif mode == "split":
                await drain_concurrently()
                assert (
                    engine.delivery_diagnostics()[
                        "quarantined_observations"
                    ]
                    == 1
                )
                allow_poison.add(round_index)
                assert await engine.async_retry_quarantined() == 1
                await drain_concurrently()
                expected_delivered += size
                expected_root_offered += 1
                expected_root_delivered += 1
            else:
                await drain_concurrently()
                expected_delivered += size
                expected_root_offered += 1
                expected_root_delivered += 1

            assert engine.delivery_diagnostics()[
                "accounting_invariant_ok"
            ] is True

        before_final_reset = engine.delivery_diagnostics()
        await engine.reset()
        final = engine.delivery_diagnostics()
        return {
            "modes": modes,
            "offered": offered,
            "expected_delivered": expected_delivered,
            "expected_discarded": expected_discarded,
            "expected_root_offered": expected_root_offered,
            "expected_root_delivered": expected_root_delivered,
            "expected_root_discarded": expected_root_discarded,
            "reset_modes": reset_modes,
            "delivered_ids": delivered_ids,
            "before_final_reset": before_final_reset,
            "final": final,
        }

    result = asyncio.run(scenario())
    before = result["before_final_reset"]
    final = result["final"]
    assert set(result["modes"]) == {
        "success",
        "transient",
        "cancel",
        "split",
        "reset_pending",
        "reset_inflight",
    }
    assert before["observations_offered_total"] == result["offered"]
    assert (
        before["observations_delivered_total"]
        == result["expected_delivered"]
    )
    assert (
        before["observations_discarded_total"]
        == result["expected_discarded"]
    )
    assert len(result["delivered_ids"]) == result["expected_delivered"]
    assert len(set(result["delivered_ids"])) == len(
        result["delivered_ids"]
    )
    assert (
        before["batches_offered_total"]
        == result["expected_root_offered"]
    )
    assert (
        before["batches_delivered_total"]
        == result["expected_root_delivered"]
    )
    assert (
        before["batches_discarded_total"]
        == result["expected_root_discarded"]
    )
    assert before["observations_outstanding_total"] == 0
    assert before["batches_outstanding_total"] == 0
    assert before["observations_lost_total"] == 0
    assert before["batches_lost_total"] == 0
    assert before["failure_retry_transitions_total"] > 0
    assert before["cancellation_requeue_transitions_total"] > 0
    assert before["split_transitions_total"] > 0
    assert before["manual_requeue_transitions_total"] > 0
    assert before["accounting_invariant_ok"] is True
    assert final["resets_total"] == result["reset_modes"] + 1
    assert (
        final["reset_discard_events_total"] == result["reset_modes"]
    )
    assert final["accounting_invariant_ok"] is True
