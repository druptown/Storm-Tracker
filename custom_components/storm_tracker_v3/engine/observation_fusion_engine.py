"""Storm Tracker V3 — engine/observation_fusion_engine.py v0.1.0

Module 2: Observation Fusion Engine (hernoemd van Strike Engine)

Verantwoordelijkheden:
  - Ontvangt Observation-objecten van ALLE providers (Blitzortung, KMI,
    RainViewer, Netatmo, toekomstige providers)
  - Dedupliceert per type en bron
  - Buffert en batcht (1s venster) zodat de Storm Engine niet bij elke
    individuele observatie wordt gewekt
  - Stuurt de gebatchte observaties door naar de Storm Engine

Wat de OFE NIET doet:
  - Kent geen Storm-objecten
  - Doet geen clustering, merge of regressie
  - Weet niet welke observatie bij welk fysiek systeem hoort
  — dat is de verantwoordelijkheid van de Storm Engine

Door de OFE generiek te houden (Observation i.p.v. Strike) is de
architectuur direct klaar voor radar, regen, wind, hagel en
toekomstige providers zonder enige aanpassing aan deze module.

Versiegeschiedenis:
  v0.1.0 — eerste versie, hernoemd van StrikeEngine naar
            ObservationFusionEngine, generiek Observation-model
"""
from __future__ import annotations

import asyncio
from contextlib import suppress
import logging
import time
from typing import Callable

from .observation import Observation, ObservationType

_LOGGER = logging.getLogger(__name__)

DEDUP_WINDOW_S   = 1.0    # observaties binnen 1s op zelfde locatie + type = duplicaat
BUFFER_MAX_AGE_S = 3600   # observaties ouder dan 1u worden uit buffer verwijderd
BATCH_INTERVAL_S = 1.0    # batchen per seconde: geen update bij elke individuele observatie


class ObservationFusionEngine:
    """
    Input-hygiëne-laag tussen providers en Storm Engine.

    Eén instantie per RegionEngine — gedeeld door alle
    ProjectionTargets in die regio.
    """

    def __init__(self, on_batch: Callable) -> None:
        """
        Args:
            on_batch: coroutine aangeroepen met list[Observation]
                      zodra een batch klaar is (naar Storm Engine)
        """
        self._on_batch    = on_batch
        self._buffer:  list[Observation] = []   # volledige buffer voor deduplicatie
        self._pending: list[Observation] = []   # wacht op flush naar Storm Engine
        self._batch_task: asyncio.Task | None = None
        self._lock = asyncio.Lock()

    async def add_observation(self, obs: Observation) -> None:
        """Voeg een nieuwe observatie toe vanuit een provider."""
        async with self._lock:
            if self._is_duplicate(obs):
                return

            self._buffer.append(obs)
            self._pending.append(obs)
            self._cleanup_buffer()

            if self._batch_task is None or self._batch_task.done():
                self._batch_task = asyncio.get_event_loop().create_task(
                    self._flush_batch()
                )

    def observations_last_n_min(
        self,
        minutes: int,
        obs_type: ObservationType | None = None,
    ) -> list[Observation]:
        """Geef observaties terug uit de buffer, optioneel gefilterd op type."""
        cutoff = time.time() - minutes * 60
        return [
            o for o in self._buffer
            if o.timestamp >= cutoff
            and (obs_type is None or o.obs_type == obs_type)
        ]

    def total_observations(
        self,
        minutes: int = 60,
        obs_type: ObservationType | None = None,
    ) -> int:
        return len(self.observations_last_n_min(minutes, obs_type))

    async def reset(self) -> None:
        """Wis observaties en annuleer een batch uit een vorige regio."""
        task = self._batch_task
        if task is not None and not task.done():
            task.cancel()

        async with self._lock:
            self._buffer.clear()
            self._pending.clear()
            self._batch_task = None

    async def async_flush_pending(self) -> int:
        """Verwerk de huidige batch onmiddellijk en wacht tot ze klaar is.

        Providercycli gebruiken dit vóór ze een gelijktijdige
        verificatiesnapshot nemen. De normale éénsecondebatching blijft gelden
        voor losse pushobservaties.
        """
        task = self._batch_task
        current = asyncio.current_task()
        if task is not None and task is not current and not task.done():
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        delivered = await self._deliver_pending()
        if self._batch_task is task:
            self._batch_task = None
        return delivered

    # ── Interne logica ────────────────────────────────────────────────────

    def _is_duplicate(self, obs: Observation) -> bool:
        """
        True als een bijna-identieke observatie recent al gezien werd.
        Deduplicatie is per type — een Netatmo RAIN-observatie op dezelfde
        locatie als een Blitzortung LIGHTNING-observatie is GEEN duplicaat.
        """
        cutoff = obs.timestamp - DEDUP_WINDOW_S
        for o in reversed(self._buffer):
            if o.timestamp < cutoff:
                break
            if o.obs_type != obs.obs_type:
                continue
            # Voor RAIN: dedupliceer ook op station_id
            if obs.obs_type == ObservationType.RAIN:
                if obs.station_id and o.station_id == obs.station_id:
                    return True
                continue
            # Voor LIGHTNING en RADAR: dedupliceer op positie (~111m)
            if abs(o.lat - obs.lat) < 0.001 and abs(o.lon - obs.lon) < 0.001:
                return True
        return False

    def _cleanup_buffer(self) -> None:
        cutoff = time.time() - BUFFER_MAX_AGE_S
        self._buffer = [o for o in self._buffer if o.timestamp >= cutoff]

    async def _flush_batch(self) -> None:
        try:
            await asyncio.sleep(BATCH_INTERVAL_S)
            await self._deliver_pending()
        finally:
            if self._batch_task is asyncio.current_task():
                self._batch_task = None

    async def _deliver_pending(self) -> int:
        async with self._lock:
            if not self._pending:
                return 0
            batch = list(self._pending)
            self._pending.clear()

        _LOGGER.debug(
            "ObservationFusionEngine: batch van %d observaties (%s)",
            len(batch),
            ", ".join(f"{t.value}:{sum(1 for o in batch if o.obs_type==t)}"
                      for t in ObservationType
                      if any(o.obs_type == t for o in batch))
        )
        try:
            await self._on_batch(batch)
        except Exception:
            _LOGGER.exception("ObservationFusionEngine: fout bij verwerken batch")
            return 0
        return len(batch)
