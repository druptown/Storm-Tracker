"""Storm Tracker V3 — tests/test_ofe.py v0.1.0

Tests voor engine/observation_fusion_engine.py — voorheen volledig
ongetest, ondanks dat dit de centrale dedup/batch-laag is waar ALLE
providers doorheen gaan.
"""
from __future__ import annotations

import asyncio
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


def test_on_batch_exception_does_not_crash_engine(ofe_module, observation_module):
    """Een exception in de on_batch-callback mag de OFE niet laten crashen (alleen loggen)."""
    async def _failing_on_batch(batch):
        raise RuntimeError("StormEngine crashte hypothetisch")

    async def _run():
        ofe = ofe_module.ObservationFusionEngine(on_batch=_failing_on_batch)
        o1 = _obs(observation_module, observation_module.ObservationType.LIGHTNING, 51.0, 4.0)
        await ofe.add_observation(o1)
        await asyncio.sleep(ofe_module.BATCH_INTERVAL_S + 0.2)
        # Als dit bereikt wordt zonder exception omhoog te laten borrelen, is de test geslaagd
        return True

    assert asyncio.run(_run()) is True


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
