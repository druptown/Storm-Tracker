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
from collections import deque
from contextlib import suppress
import logging
import time
from typing import Callable
import uuid

from .observation import Observation, ObservationType

_LOGGER = logging.getLogger(__name__)

DEDUP_WINDOW_S   = 1.0    # observaties binnen 1s op zelfde locatie + type = duplicaat
BUFFER_MAX_AGE_S = 3600   # observaties ouder dan 1u worden uit buffer verwijderd
BATCH_INTERVAL_S = 1.0    # batchen per seconde: geen update bij elke individuele observatie
BATCH_RETRY_BACKOFF_BASE_S = 1.0
BATCH_RETRY_BACKOFF_MAX_S = 30.0
BATCH_SPLIT_AFTER_ATTEMPTS = 3
BATCH_SYSTEMIC_ERROR_THRESHOLD = 6
BATCH_LINEAGE_FAILURE_BUDGET = 64
BATCH_LINEAGE_ATTEMPT_BUDGET = 512
BATCH_LINEAGE_EPOCH_TIME_BUDGET_S = 2.0
BATCH_LINEAGE_EPOCH_COOLDOWN_S = 5.0
INFRASTRUCTURE_RETRY_SECONDS = 30.0


class SystemicBatchError(RuntimeError):
    """Expliciet signaal dat de callback geen enkele batch kan verwerken.

    Een gewone exception is geen bewijs van een systeemfout: twee afzonderlijke
    poison-observaties kunnen immers exact dezelfde fouttekst produceren.
    Alleen callbacks die de fout als batchonafhankelijk herkennen, mogen dit
    type gebruiken om het lineagecircuit vroeg te openen.
    """


class TransientInfrastructureError(SystemicBatchError):
    """Keep one intact batch waiting for unavailable external capacity.

    Splitting cannot repair an offline worker and only amplifies HA load.
    """


class _BatchLineage:
    """Gedeeld foutbudget voor een rootbatch en alle gesplitste kinderen."""

    __slots__ = (
        "root_batch_id",
        "failures",
        "failures_since_success",
        "successes",
        "error_counts",
        "epoch_attempts",
        "epoch_started_at",
        "epochs",
        "accounting_offered",
        "accounting_observations_total",
        "accounting_delivered_observations",
        "accounting_discarded_observations",
        "accounting_lost_observations",
        "accounting_terminal",
    )

    def __init__(self, root_batch_id: str) -> None:
        self.root_batch_id = root_batch_id
        self.failures = 0
        self.failures_since_success = 0
        self.successes = 0
        self.error_counts: dict[str, int] = {}
        self.epoch_attempts = 0
        self.epoch_started_at: float | None = None
        self.epochs = 1
        # Deze velden volgen de delivery-liability van de rootbatch. Kinderen
        # uit een split delen dezelfde lineage, zodat offered/terminal nooit
        # per deelbatch dubbel wordt geteld.
        self.accounting_offered = False
        self.accounting_observations_total = 0
        self.accounting_delivered_observations = 0
        self.accounting_discarded_observations = 0
        self.accounting_lost_observations = 0
        self.accounting_terminal: str | None = None


class _BatchReceipt:
    """Gedeeld commitbewijs voor een volledige batch en afgeleide subsets."""

    __slots__ = (
        "batch_id",
        "committed",
        "parent_batch_id",
        "attempts",
        "next_retry_at",
        "last_error",
        "lineage",
        "cooldown_protected",
        "observation_count",
        "delivery_accounted",
        "discard_accounted",
        "lost_accounted",
    )

    def __init__(
        self,
        batch_id: str,
        parent_batch_id: str | None = None,
        lineage: _BatchLineage | None = None,
        root_batch_id: str | None = None,
        observation_count: int = 0,
    ) -> None:
        self.batch_id = batch_id
        self.committed = False
        self.parent_batch_id = parent_batch_id
        self.attempts = 0
        self.next_retry_at = 0.0
        self.last_error = None
        self.lineage = lineage or _BatchLineage(
            root_batch_id or parent_batch_id or batch_id
        )
        self.cooldown_protected = False
        # Leg de omvang vast vóór de vertrouwde callback de lijst ziet. De
        # counters mogen niet veranderen door latere list-mutatie of retries.
        self.observation_count = int(observation_count)
        self.delivery_accounted = False
        self.discard_accounted = False
        self.lost_accounted = False


class ObservationBatch(list):
    """Lijstcompatibele batch met een stabiele idempotency-sleutel."""

    __slots__ = ("_receipt",)

    def __init__(
        self,
        observations=(),
        *,
        batch_id: str | None = None,
        parent_batch_id: str | None = None,
        root_batch_id: str | None = None,
        _lineage: _BatchLineage | None = None,
        _receipt: _BatchReceipt | None = None,
    ) -> None:
        super().__init__(observations)
        self._receipt = _receipt or _BatchReceipt(
            batch_id or uuid.uuid4().hex,
            parent_batch_id=parent_batch_id,
            lineage=_lineage,
            root_batch_id=root_batch_id,
            observation_count=len(self),
        )

    @property
    def batch_id(self) -> str:
        return self._receipt.batch_id

    @property
    def committed(self) -> bool:
        return self._receipt.committed

    @property
    def parent_batch_id(self) -> str | None:
        return self._receipt.parent_batch_id

    @property
    def attempts(self) -> int:
        return self._receipt.attempts

    @property
    def root_batch_id(self) -> str:
        return self._receipt.lineage.root_batch_id

    def acknowledge(self) -> None:
        """Markeer de meteorologische statecommit als voltooid."""
        self._receipt.committed = True

    def subset(self, observations) -> "ObservationBatch":
        """Maak een gefilterde batch die hetzelfde commitbewijs deelt."""
        return ObservationBatch(observations, _receipt=self._receipt)


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
        self._retry_batches: deque[ObservationBatch] = deque()
        self._quarantined_batches: deque[ObservationBatch] = deque()
        self._batch_task: asyncio.Task | None = None
        self._batch_due_at: float | None = None
        self._lock = asyncio.Lock()
        self._delivery_lock = asyncio.Lock()
        self._reset_lock = asyncio.Lock()
        self._resetting = False
        self._batch_delivery_started = False
        self._inflight_batch: ObservationBatch | None = None
        self._last_delivery_error: str | None = None
        self._persist_observations = None
        self._persist_quarantine = None
        self._durable_classified_ingest = False
        # Vaste, O(1)-begrensde cumulatieve counters. ``reset()`` wist de
        # regiostate, maar niet deze auditgeschiedenis.
        self._delivery_counters = {
            "observations_offered_total": 0,
            "observations_delivered_total": 0,
            "observations_requeued_total": 0,
            "observations_discarded_total": 0,
            "observations_lost_total": 0,
            "batches_offered_total": 0,
            "batches_delivered_total": 0,
            "batches_requeued_total": 0,
            "batches_discarded_total": 0,
            "batches_lost_total": 0,
            "failure_retry_transitions_total": 0,
            "infrastructure_defer_transitions_total": 0,
            "cancellation_requeue_transitions_total": 0,
            "fatal_requeue_transitions_total": 0,
            "split_transitions_total": 0,
            "cooldown_requeue_transitions_total": 0,
            "manual_requeue_transitions_total": 0,
            "recovery_requeue_transitions_total": 0,
            "quarantine_transitions_total": 0,
            "resets_total": 0,
            "reset_discard_events_total": 0,
            "reset_discarded_observations_total": 0,
            "reset_discarded_pending_observations_total": 0,
            "reset_discarded_batched_observations_total": 0,
            "reset_discarded_batches_total": 0,
            "accounting_errors_total": 0,
        }

    def configure_durable_ingest(
        self,
        persist_observations,
        *,
        classified_deduplication: bool = False,
    ) -> None:
        """Koppel een async bulkreceipt vóór opname in de runtimecache."""
        self._persist_observations = persist_observations
        self._durable_classified_ingest = bool(classified_deduplication)

    def configure_durable_quarantine(self, persist_quarantine) -> None:
        """Koppel de definitieve OFE-quarantaine aan het durable journal."""
        self._persist_quarantine = persist_quarantine

    async def add_observation(self, obs: Observation) -> None:
        """Voeg een nieuwe observatie toe vanuit een provider."""
        await self.add_observations([obs])

    async def add_observations(
        self,
        observations,
        *,
        already_persisted: bool = False,
    ) -> int:
        """Voeg een groep toe met hoogstens één duurzame DB-transactie."""
        observations = list(observations)
        if (
            observations
            and self._persist_observations is not None
            and not already_persisted
            and self._durable_classified_ingest
        ):
            # De dedupbeslissing en RECEIVED/DEDUPED-receipts horen bij één
            # atomische ingesttransactie. Houd de regioklok tijdens de korte
            # executorcall vast zodat twee gelijktijdige groepen niet beide
            # als eerste exemplaar worden geclassificeerd.
            async with self._lock:
                comparison_buffer = list(self._buffer)
                deduplicated = []
                for obs in observations:
                    duplicate = self._is_duplicate(
                        obs, buffer=comparison_buffer
                    )
                    deduplicated.append(duplicate)
                    if not duplicate:
                        comparison_buffer.append(obs)
                accepted = list(
                    await self._persist_observations(
                        observations,
                        deduplicated=deduplicated,
                    )
                )
                if len(accepted) != len(observations):
                    raise RuntimeError(
                        "durable ingest gaf geen resultaat per observation"
                    )
                added = 0
                for obs, durable_new, duplicate in zip(
                    observations,
                    accepted,
                    deduplicated,
                    strict=True,
                ):
                    if not durable_new or duplicate:
                        continue
                    self._buffer.append(obs)
                    self._pending.append(obs)
                    added += 1
                self._delivery_counters[
                    "observations_offered_total"
                ] += added
                self._cleanup_buffer()
                self._schedule_batch_locked()
                return added
        accepted = [True] * len(observations)
        if (
            observations
            and self._persist_observations is not None
            and not already_persisted
        ):
            accepted = list(
                await self._persist_observations(observations)
            )
            if len(accepted) != len(observations):
                raise RuntimeError(
                    "durable ingest gaf geen resultaat per observation"
                )
        added = 0
        async with self._lock:
            for obs, durable_new in zip(
                observations, accepted, strict=True
            ):
                if (
                    not durable_new
                    or (
                        not already_persisted
                        and self._is_duplicate(obs)
                    )
                ):
                    continue
                self._buffer.append(obs)
                self._pending.append(obs)
                added += 1
            self._delivery_counters[
                "observations_offered_total"
            ] += added
            self._cleanup_buffer()
            self._schedule_batch_locked()
        return added

    async def async_restore_observations(self, observations) -> int:
        """Herstel RECEIVED input zonder een tweede durable insert."""
        return await self.add_observations(
            observations, already_persisted=True
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

    def _account_root_batch_offered_locked(
        self,
        batch: ObservationBatch,
        *,
        observations_already_offered: bool,
    ) -> None:
        """Registreer één nieuwe root-liability of een herstelde sibling.

        Een normale snapshot van ``_pending`` opent precies één rootbatch.
        Durable recovery kan meerdere nog open deelbatches met dezelfde root
        teruggeven; die delen één lineage en vergroten alleen het
        observationtotaal, nooit ``batches_offered_total``.
        """
        receipt = batch._receipt
        lineage = receipt.lineage
        count = receipt.observation_count
        if lineage.accounting_terminal is not None:
            self._delivery_counters["accounting_errors_total"] += 1
            return
        if not lineage.accounting_offered:
            lineage.accounting_offered = True
            self._delivery_counters["batches_offered_total"] += 1
        lineage.accounting_observations_total += count
        if not observations_already_offered:
            self._delivery_counters[
                "observations_offered_total"
            ] += count

    def _account_requeue_locked(
        self,
        batch: ObservationBatch,
        *,
        reason: str,
        batch_count: int = 1,
        observation_count: int | None = None,
    ) -> None:
        """Tel een echte queue-overgang, los van root-offered/terminal."""
        count = (
            batch._receipt.observation_count
            if observation_count is None
            else int(observation_count)
        )
        self._delivery_counters["batches_requeued_total"] += int(
            batch_count
        )
        self._delivery_counters["observations_requeued_total"] += count
        transition_key = {
            "failure": "failure_retry_transitions_total",
            "infrastructure": "infrastructure_defer_transitions_total",
            "cancellation": "cancellation_requeue_transitions_total",
            "fatal": "fatal_requeue_transitions_total",
            "split": "split_transitions_total",
            "cooldown": "cooldown_requeue_transitions_total",
            "manual": "manual_requeue_transitions_total",
            "recovery": "recovery_requeue_transitions_total",
        }.get(reason)
        if transition_key is None:
            self._delivery_counters["accounting_errors_total"] += 1
        else:
            self._delivery_counters[transition_key] += 1

    def _finalize_root_accounting_locked(
        self, lineage: _BatchLineage
    ) -> None:
        """Sluit een root pas wanneer al zijn observaties terminaal zijn."""
        if (
            not lineage.accounting_offered
            or lineage.accounting_terminal is not None
        ):
            return
        terminal_observations = (
            lineage.accounting_delivered_observations
            + lineage.accounting_discarded_observations
            + lineage.accounting_lost_observations
        )
        if terminal_observations < lineage.accounting_observations_total:
            return
        if terminal_observations > lineage.accounting_observations_total:
            self._delivery_counters["accounting_errors_total"] += 1
            return
        if lineage.accounting_discarded_observations:
            # Ook een deels geleverde of eerder beschadigde root moet zichtbaar
            # maken dat een reset de resterende liability bewust verwijderde.
            self._delivery_counters[
                "reset_discarded_batches_total"
            ] += 1
        if lineage.accounting_lost_observations:
            outcome = "lost"
        elif lineage.accounting_discarded_observations:
            outcome = "discarded"
        else:
            outcome = "delivered"
        lineage.accounting_terminal = outcome
        self._delivery_counters[f"batches_{outcome}_total"] += 1

    def _account_batch_delivered_locked(
        self, batch: ObservationBatch
    ) -> None:
        """Boek een gecommitteerde leaf exact één keer op zijn root."""
        receipt = batch._receipt
        if receipt.delivery_accounted:
            return
        if receipt.discard_accounted or receipt.lost_accounted:
            self._delivery_counters["accounting_errors_total"] += 1
            return
        if not receipt.lineage.accounting_offered:
            self._delivery_counters["accounting_errors_total"] += 1
            return
        receipt.delivery_accounted = True
        count = receipt.observation_count
        receipt.lineage.accounting_delivered_observations += count
        self._delivery_counters["observations_delivered_total"] += count
        self._finalize_root_accounting_locked(receipt.lineage)

    def _account_batch_reset_discard_locked(
        self, batch: ObservationBatch
    ) -> None:
        """Boek doelbewust resetverlies apart van werkelijk dataverlies."""
        receipt = batch._receipt
        if receipt.discard_accounted:
            return
        if receipt.delivery_accounted or receipt.lost_accounted:
            self._delivery_counters["accounting_errors_total"] += 1
            return
        if not receipt.lineage.accounting_offered:
            self._delivery_counters["accounting_errors_total"] += 1
            return
        receipt.discard_accounted = True
        count = receipt.observation_count
        receipt.lineage.accounting_discarded_observations += count
        self._delivery_counters["observations_discarded_total"] += count
        self._delivery_counters[
            "reset_discarded_observations_total"
        ] += count
        self._delivery_counters[
            "reset_discarded_batched_observations_total"
        ] += count
        self._finalize_root_accounting_locked(receipt.lineage)

    def delivery_accounting(self) -> dict:
        """Geef een pure momentopname van cumulatieve deliveryboekhouding."""
        counters = dict(self._delivery_counters)
        retry_observations = sum(
            batch._receipt.observation_count
            for batch in self._retry_batches
        )
        quarantined_observations = sum(
            batch._receipt.observation_count
            for batch in self._quarantined_batches
        )
        inflight_observations = (
            self._inflight_batch._receipt.observation_count
            if self._inflight_batch is not None
            else 0
        )
        resident_observations = (
            len(self._pending)
            + retry_observations
            + quarantined_observations
            + inflight_observations
        )
        resident_roots = {
            batch.root_batch_id for batch in self._retry_batches
        } | {
            batch.root_batch_id for batch in self._quarantined_batches
        }
        if self._inflight_batch is not None:
            resident_roots.add(self._inflight_batch.root_batch_id)
        observations_outstanding = (
            counters["observations_offered_total"]
            - counters["observations_delivered_total"]
            - counters["observations_discarded_total"]
            - counters["observations_lost_total"]
        )
        batches_outstanding = (
            counters["batches_offered_total"]
            - counters["batches_delivered_total"]
            - counters["batches_discarded_total"]
            - counters["batches_lost_total"]
        )
        counters.update(
            {
                # Roadmapnaam voor dezelfde root-terminal als delivered.
                "batches_acked_total": counters[
                    "batches_delivered_total"
                ],
                # Oudere P0-tekst gebruikte noun-first namen; houd die als
                # pure aliases zodat dashboards/tests niet ambigu worden.
                "lost_observations_total": counters[
                    "observations_lost_total"
                ],
                "lost_batches_total": counters["batches_lost_total"],
                "observations_outstanding_total": observations_outstanding,
                "batches_outstanding_total": batches_outstanding,
                "resident_observations": resident_observations,
                "resident_root_batches": len(resident_roots),
                "inflight_batches": int(
                    self._inflight_batch is not None
                ),
                "inflight_observations": inflight_observations,
                "accounting_invariant_ok": bool(
                    counters["accounting_errors_total"] == 0
                    and observations_outstanding >= 0
                    and batches_outstanding >= 0
                    and observations_outstanding
                    == resident_observations
                    and batches_outstanding == len(resident_roots)
                ),
            }
        )
        return counters

    def delivery_diagnostics(self) -> dict:
        """Uitlegbare runtimehealth zonder observaties stil te verwijderen."""
        retry_observations = sum(
            batch._receipt.observation_count
            for batch in self._retry_batches
        )
        quarantined_observations = sum(
            batch._receipt.observation_count
            for batch in self._quarantined_batches
        )
        quarantined_lineages = {
            batch.root_batch_id
            for batch in self._quarantined_batches
        }
        cooldown_batches = [
            batch
            for batch in self._retry_batches
            if batch._receipt.cooldown_protected
        ]
        cooldown_lineages = {
            batch.root_batch_id for batch in cooldown_batches
        }
        state = (
            "blocked_observations"
            if self._quarantined_batches
            else "cooldown"
            if cooldown_batches
            else "retrying"
            if self._retry_batches
            else "healthy"
        )
        return {
            "state": state,
            "pending_observations": len(self._pending),
            "retry_batches": len(self._retry_batches),
            "retry_observations": retry_observations,
            "quarantined_batches": len(self._quarantined_batches),
            "quarantined_observations": quarantined_observations,
            "quarantined_root_batches": len(quarantined_lineages),
            "cooldown_batches": len(cooldown_batches),
            "cooldown_observations": sum(
                batch._receipt.observation_count
                for batch in cooldown_batches
            ),
            "cooldown_root_batches": len(cooldown_lineages),
            "last_error": self._last_delivery_error,
            **self.delivery_accounting(),
        }

    async def async_retry_quarantined(self) -> int:
        """Zet bewaarde poison-observaties expliciet opnieuw in verwerking."""
        async with self._lock:
            count = len(self._quarantined_batches)
            reset_lineages: set[int] = set()
            while self._quarantined_batches:
                batch = self._quarantined_batches.popleft()
                lineage = batch._receipt.lineage
                lineage_key = id(lineage)
                if lineage_key not in reset_lineages:
                    lineage.failures_since_success = 0
                    lineage.epoch_attempts = 0
                    lineage.epoch_started_at = None
                    lineage.epochs += 1
                    lineage.error_counts.clear()
                    reset_lineages.add(lineage_key)
                batch._receipt.attempts = 0
                batch._receipt.next_retry_at = 0.0
                batch._receipt.cooldown_protected = False
                self._retry_batches.append(batch)
                self._account_requeue_locked(batch, reason="manual")
            self._schedule_batch_locked()
        return count

    async def async_restore_batches(
        self,
        batches,
    ) -> int:
        """Herstel durable PREPARED batches met dezelfde identiteit en FIFO."""
        restored = 0
        async with self._lock:
            known_ids = {
                batch.batch_id for batch in self._retry_batches
            } | {
                batch.batch_id for batch in self._quarantined_batches
            }
            if self._inflight_batch is not None:
                known_ids.add(self._inflight_batch.batch_id)
            lineages_by_root = {
                batch.root_batch_id: batch._receipt.lineage
                for batch in (
                    *self._retry_batches,
                    *self._quarantined_batches,
                )
            }
            if self._inflight_batch is not None:
                lineages_by_root[
                    self._inflight_batch.root_batch_id
                ] = self._inflight_batch._receipt.lineage
            for raw in batches:
                batch = (
                    raw
                    if isinstance(raw, ObservationBatch)
                    else ObservationBatch(
                        getattr(raw, "observations", ()),
                        batch_id=getattr(raw, "batch_id"),
                        parent_batch_id=getattr(
                            raw, "parent_batch_id", None
                        ),
                        root_batch_id=getattr(
                            raw, "root_batch_id", None
                        ),
                    )
                )
                if batch.batch_id in known_ids or batch.committed:
                    continue
                existing_lineage = lineages_by_root.get(
                    batch.root_batch_id
                )
                if existing_lineage is None:
                    lineages_by_root[batch.root_batch_id] = (
                        batch._receipt.lineage
                    )
                elif batch._receipt.lineage is not existing_lineage:
                    # Durable leaves van dezelfde oorspronkelijke root moeten
                    # binnen deze runtime één accounting-liability delen.
                    batch._receipt.lineage = existing_lineage
                self._retry_batches.append(batch)
                self._account_root_batch_offered_locked(
                    batch,
                    observations_already_offered=False,
                )
                self._account_requeue_locked(batch, reason="recovery")
                known_ids.add(batch.batch_id)
                restored += 1
            self._schedule_batch_locked()
        return restored

    async def reset(self) -> None:
        """Wis een regio en boek doelbewust verwijderde liability als discard."""
        async with self._reset_lock:
            operation = asyncio.create_task(self._reset_once())
            try:
                await asyncio.shield(operation)
            except asyncio.CancelledError:
                # Een afgebroken teardown mag ``_resetting`` en oude
                # delivery-liability niet half achterlaten. Rond de reeds
                # begonnen reset af en propageer daarna pas cancellation.
                await operation
                raise

    async def _reset_once(self) -> None:
        """Voer één geserialiseerde region-reset uit."""
        async with self._lock:
            self._resetting = True
            task = self._batch_task
            discarded_observations_before = self._delivery_counters[
                "reset_discarded_observations_total"
            ]
            discarded_batches_before = self._delivery_counters[
                "reset_discarded_batches_total"
            ]

        if task is not None and not task.done():
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

        # Wacht ook op een eventuele handmatige levering. Zo kan een callback
        # uit de oude regio na reset geen observaties terug in _pending zetten.
        async with self._delivery_lock:
            async with self._lock:
                pending_count = len(self._pending)
                if pending_count:
                    self._delivery_counters[
                        "observations_discarded_total"
                    ] += pending_count
                    self._delivery_counters[
                        "reset_discarded_observations_total"
                    ] += pending_count
                    self._delivery_counters[
                        "reset_discarded_pending_observations_total"
                    ] += pending_count
                for batch in (
                    *self._retry_batches,
                    *self._quarantined_batches,
                ):
                    self._account_batch_reset_discard_locked(batch)
                self._buffer.clear()
                self._pending.clear()
                self._retry_batches.clear()
                self._quarantined_batches.clear()
                self._inflight_batch = None
                self._batch_task = None
                self._batch_due_at = None
                self._resetting = False
                self._last_delivery_error = None
                self._delivery_counters["resets_total"] += 1
                if (
                    self._delivery_counters[
                        "reset_discarded_observations_total"
                    ] > discarded_observations_before
                    or self._delivery_counters[
                        "reset_discarded_batches_total"
                    ] > discarded_batches_before
                ):
                    self._delivery_counters[
                        "reset_discard_events_total"
                    ] += 1

    async def async_flush_pending(self) -> int:
        """Verwerk de huidige batch onmiddellijk en wacht tot ze klaar is.

        Providercycli gebruiken dit vóór ze een gelijktijdige
        verificatiesnapshot nemen. De normale éénsecondebatching blijft gelden
        voor losse pushobservaties.
        """
        current = asyncio.current_task()
        delivered = 0
        cancel_sleeping_task = False
        async with self._lock:
            if self._retry_batches:
                # Een expliciete operator-/providercyclus mag één geplande
                # retry bewust vervroegen; fouten die tijdens déze flush
                # ontstaan behouden opnieuw hun backoff.
                receipt = self._retry_batches[0]._receipt
                if not receipt.cooldown_protected:
                    receipt.next_retry_at = 0.0
            task = self._batch_task
            if task is not None and task is not current and not task.done():
                if self._batch_delivery_started:
                    pass
                else:
                    # De task wacht nog uitsluitend op het batchvenster.
                    # Onder dezelfde lock kan ze niet tegelijk de callback
                    # binnengaan; annuleren is hier dus veilig en houdt een
                    # expliciete flush onmiddellijk.
                    task.cancel()
                    cancel_sleeping_task = True
        if cancel_sleeping_task:
            with suppress(asyncio.CancelledError):
                await task
        elif task is not None and task is not current and not task.done():
            # Annuleer nooit een reeds gestarte callback om een expliciete
            # flush te versnellen. Een workerjob zou dan op de achtergrond
            # doorrekenen terwijl zijn HA-future al verdwenen is. Shield
            # voorkomt bovendien dat cancellation van de aanroeper de
            # bestaande aflevering orphaned maakt.
            result = await asyncio.shield(task)
            delivered += int(result or 0)
        while True:
            delivered_now = await self._deliver_pending()
            delivered += delivered_now
            if delivered_now <= 0:
                break
            async with self._lock:
                if not self._retry_batches and not self._pending:
                    break

        # Een succesvolle retry kan tijdens deze handmatige drain kort een
        # nieuwe éénsecondetimer gepland hebben voor nieuwere observaties.
        # Als alles nu synchroon verwerkt is, ruim die lege timer op.
        async with self._lock:
            tail_task = self._batch_task
            cancel_empty_timer = bool(
                tail_task is not None
                and tail_task is not current
                and not tail_task.done()
                and not self._batch_delivery_started
                and not self._retry_batches
                and not self._pending
            )
            if cancel_empty_timer:
                tail_task.cancel()
            else:
                # Een expliciete flush kan een backofftimer annuleren terwijl
                # de retry nog niet due is. Laat die bewaarde batch niet
                # zonder volgende wekker achter.
                self._schedule_batch_locked()
        if cancel_empty_timer:
            with suppress(asyncio.CancelledError):
                await tail_task
        return delivered

    # ── Interne logica ────────────────────────────────────────────────────

    def _is_duplicate(
        self,
        obs: Observation,
        *,
        buffer=None,
    ) -> bool:
        """
        True als een bijna-identieke observatie recent al gezien werd.
        Deduplicatie is per type — een Netatmo RAIN-observatie op dezelfde
        locatie als een Blitzortung LIGHTNING-observatie is GEEN duplicaat.
        """
        cutoff = obs.timestamp - DEDUP_WINDOW_S
        source_buffer = self._buffer if buffer is None else buffer
        for o in reversed(source_buffer):
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

    def _schedule_batch_locked(self) -> None:
        """Plan een volgende levering; aanroepen terwijl ``_lock`` vaststaat."""
        if self._resetting or (
            not self._retry_batches and not self._pending
        ):
            return
        due_at = time.monotonic() + self._next_delivery_delay_locked()
        if self._batch_task is not None and not self._batch_task.done():
            if (
                not self._batch_delivery_started
                and self._batch_due_at is not None
                and due_at + 1e-6 < self._batch_due_at
            ):
                # Nieuwe echte input hoeft niet achter een lange poison-batch
                # backoff te wachten. De oude task slaapt nog en kan dus
                # veilig door een eerdere timer worden vervangen.
                self._batch_task.cancel()
            else:
                return
        self._batch_due_at = due_at
        self._batch_task = asyncio.get_running_loop().create_task(
            self._flush_batch()
        )
        self._batch_task.add_done_callback(self._batch_task_finished)

    def _batch_task_finished(self, task: asyncio.Task) -> None:
        """Herstel de wekker als een task vóór haar eerste coroutine-step stopt."""
        if self._batch_task is not task:
            return
        if not self._runtime_can_reschedule(task):
            return

        async def _restore_schedule() -> None:
            async with self._lock:
                if self._batch_task is not task:
                    return
                self._batch_task = None
                self._batch_due_at = None
                self._schedule_batch_locked()

        try:
            asyncio.get_running_loop().create_task(_restore_schedule())
        except RuntimeError:
            # De eventloop zelf is al gesloten; duurzame restartrecovery hoort
            # bij het persistente receiptcontract en niet bij deze runtime.
            return

    @staticmethod
    def _runtime_can_reschedule(current: asyncio.Task) -> bool:
        """False tijdens asyncio-shutdown, wanneer iedere task wordt gestopt."""
        try:
            tasks = asyncio.all_tasks()
        except RuntimeError:
            return False
        return any(
            task is not current
            and not task.done()
            and task.cancelling() == 0
            for task in tasks
        )

    def _next_delivery_delay_locked(self) -> float:
        """Respecteer retrybackoff zonder nieuwere input te blokkeren."""
        retry_delay = None
        if self._retry_batches:
            retry_delay = min(
                max(
                    0.0,
                    batch._receipt.next_retry_at - time.monotonic(),
                )
                for batch in self._retry_batches
            )
            if retry_delay <= 0:
                return 0.0
        if self._pending:
            return (
                min(float(BATCH_INTERVAL_S), retry_delay)
                if retry_delay is not None
                else float(BATCH_INTERVAL_S)
            )
        return retry_delay or 0.0

    async def _flush_batch(self) -> int:
        cancelled = False
        delivered = 0
        try:
            async with self._lock:
                delay = max(
                    0.0,
                    float(self._batch_due_at or time.monotonic())
                    - time.monotonic(),
                )
            await asyncio.sleep(delay)
            async with self._lock:
                self._batch_delivery_started = True
            delivered = await self._deliver_pending()
            return delivered
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            async with self._lock:
                self._batch_delivery_started = False
                if self._batch_task is asyncio.current_task():
                    self._batch_task = None
                    self._batch_due_at = None
                # Houd tijdens runtime de invariant dat bewaarde data een
                # wekker heeft. Tijdens asyncio.run/HA-loopshutdown worden
                # alle tasks tegelijk geannuleerd; plan daar geen orphan task.
                if (
                    not cancelled
                    or self._runtime_can_reschedule(
                        asyncio.current_task()
                    )
                ):
                    self._schedule_batch_locked()

    def _stage_failed_batch_locked(
        self,
        batch: ObservationBatch,
        error: Exception,
    ) -> str:
        """Backoff, splits en isoleer zonder één observatie te verwijderen."""
        receipt = batch._receipt
        receipt.last_error = (
            f"{type(error).__name__}: {str(error)[:240]}"
        )
        self._last_delivery_error = receipt.last_error
        if isinstance(error, TransientInfrastructureError):
            # Raw observations already have durable ingest receipts. Keep the
            # root intact without consuming poison/failure budgets; otherwise
            # an offline worker creates an approximately 2N callback tree.
            receipt.next_retry_at = (
                time.monotonic() + float(INFRASTRUCTURE_RETRY_SECONDS)
            )
            receipt.cooldown_protected = True
            self._retry_batches.appendleft(batch)
            self._account_requeue_locked(batch, reason="infrastructure")
            return "infrastructure_wait"
        receipt.attempts += 1
        lineage = receipt.lineage
        lineage.failures += 1
        lineage.failures_since_success += 1
        lineage.error_counts[receipt.last_error] = (
            lineage.error_counts.get(receipt.last_error, 0) + 1
        )
        explicitly_systemic = (
            isinstance(error, SystemicBatchError)
            and lineage.error_counts[receipt.last_error]
            >= BATCH_SYSTEMIC_ERROR_THRESHOLD
        )
        failure_budget_exhausted = (
            lineage.failures_since_success
            >= BATCH_LINEAGE_FAILURE_BUDGET
        )
        if explicitly_systemic or failure_budget_exhausted:
            self._quarantine_lineage_locked(batch)
            self._delivery_counters[
                "quarantine_transitions_total"
            ] += 1
            return "quarantine"

        # Alleen de rootbatch krijgt een kort transient-retryvenster. Zodra
        # een aantoonbaar persistente batch is opgesplitst, moet ieder falend
        # kind meteen verder worden geïsoleerd. Anders kost elke boomlaag
        # opnieuw meerdere identieke pogingen en kunnen enkele poisonrecords
        # het gedeelde lineagebudget opgebruiken voordat geldige siblings aan
        # bod komen.
        is_split_child = batch.parent_batch_id is not None
        if (
            not is_split_child
            and receipt.attempts < BATCH_SPLIT_AFTER_ATTEMPTS
        ):
            backoff = min(
                float(BATCH_RETRY_BACKOFF_MAX_S),
                float(BATCH_RETRY_BACKOFF_BASE_S)
                * (2 ** (receipt.attempts - 1)),
            )
            receipt.next_retry_at = time.monotonic() + backoff
            receipt.cooldown_protected = False
            self._retry_batches.appendleft(batch)
            self._account_requeue_locked(batch, reason="failure")
            return "retry"

        if len(batch) > 1:
            midpoint = max(1, len(batch) // 2)
            # Bewaar de directe ouder, niet alleen de root. De root staat
            # afzonderlijk in lineage; dit maakt iedere duurzame splitstap
            # reconstructeerbaar en laat memberships veilig doorgeven.
            parent_batch_id = batch.batch_id
            left = ObservationBatch(
                batch[:midpoint],
                parent_batch_id=parent_batch_id,
                _lineage=receipt.lineage,
            )
            right = ObservationBatch(
                batch[midpoint:],
                parent_batch_id=parent_batch_id,
                _lineage=receipt.lineage,
            )
            # appendleft in omgekeerde volgorde behoudt de oorspronkelijke
            # observatievolgorde zonder beide helften hetzelfde receipt te
            # laten delen.
            self._retry_batches.appendleft(right)
            self._retry_batches.appendleft(left)
            self._account_requeue_locked(
                batch,
                reason="split",
                batch_count=2,
                observation_count=(
                    left._receipt.observation_count
                    + right._receipt.observation_count
                ),
            )
            return "split"

        self._quarantined_batches.append(batch)
        self._delivery_counters["quarantine_transitions_total"] += 1
        return "quarantine"

    def _quarantine_lineage_locked(
        self, failed_batch: ObservationBatch
    ) -> None:
        """Parkeer in één stap alle nog wachtende delen van dezelfde root."""
        root_batch_id = failed_batch.root_batch_id
        self._quarantined_batches.append(failed_batch)
        retained: deque[ObservationBatch] = deque()
        while self._retry_batches:
            candidate = self._retry_batches.popleft()
            if candidate.root_batch_id == root_batch_id:
                self._quarantined_batches.append(candidate)
            else:
                retained.append(candidate)
        self._retry_batches = retained

    def _defer_lineage_locked(
        self, next_batch: ObservationBatch
    ) -> int:
        """Pauzeer onopgeloste siblings; verwijder of keur niets af."""
        lineage = next_batch._receipt.lineage
        root_batch_id = next_batch.root_batch_id
        due_at = (
            time.monotonic() + BATCH_LINEAGE_EPOCH_COOLDOWN_S
        )
        deferred = [next_batch]
        retained: deque[ObservationBatch] = deque()
        while self._retry_batches:
            candidate = self._retry_batches.popleft()
            if candidate.root_batch_id == root_batch_id:
                deferred.append(candidate)
            else:
                retained.append(candidate)
        for candidate in deferred:
            candidate._receipt.next_retry_at = due_at
            candidate._receipt.cooldown_protected = True
            retained.append(candidate)
        self._retry_batches = retained
        lineage.epoch_attempts = 0
        lineage.epoch_started_at = None
        lineage.epochs += 1
        self._account_requeue_locked(next_batch, reason="cooldown")
        self._last_delivery_error = (
            "LineageEpochCooldown: callbackbudget bereikt; "
            f"root {root_batch_id} hervat automatisch"
        )
        return len(deferred)

    @staticmethod
    def _record_lineage_success(batch: ObservationBatch) -> None:
        """Een geldige sibling bewijst dat de callback niet globaal stuk is."""
        lineage = batch._receipt.lineage
        lineage.successes += 1
        lineage.failures_since_success = 0
        lineage.error_counts.clear()

    @staticmethod
    def _should_log_lineage_failure(batch: ObservationBatch) -> bool:
        """Sample grote isolatiebomen zonder duizenden identieke logregels."""
        failures = batch._receipt.lineage.failures
        return (
            batch.parent_batch_id is None
            or failures <= 8
            or failures & (failures - 1) == 0
            or failures % BATCH_LINEAGE_ATTEMPT_BUDGET == 0
        )

    async def _deliver_pending(self) -> int:
        async with self._delivery_lock:
            async with self._lock:
                due_index = next(
                    (
                        index
                        for index, candidate
                        in enumerate(self._retry_batches)
                        if candidate._receipt.next_retry_at
                        <= time.monotonic()
                    ),
                    None,
                )
                if due_index is not None:
                    batch = self._retry_batches[due_index]
                    del self._retry_batches[due_index]
                elif self._pending:
                    batch = ObservationBatch(self._pending)
                    self._pending.clear()
                    self._account_root_batch_offered_locked(
                        batch,
                        observations_already_offered=True,
                    )
                else:
                    return 0
                self._inflight_batch = batch
                lineage = batch._receipt.lineage
                epoch_time_exhausted = (
                    lineage.epoch_attempts > 0
                    and lineage.epoch_started_at is not None
                    and time.monotonic() - lineage.epoch_started_at
                    >= BATCH_LINEAGE_EPOCH_TIME_BUDGET_S
                )
                if (
                    lineage.epoch_attempts
                    >= BATCH_LINEAGE_ATTEMPT_BUDGET
                    or epoch_time_exhausted
                ):
                    deferred_count = self._defer_lineage_locked(batch)
                    _LOGGER.warning(
                        "ObservationFusionEngine: callback-/tijdbudget voor "
                        "rootbatch %s bereikt; %d onopgeloste deelbatches "
                        "hervatten na %.1f seconden",
                        batch.root_batch_id,
                        deferred_count,
                        BATCH_LINEAGE_EPOCH_COOLDOWN_S,
                    )
                    self._schedule_batch_locked()
                    self._inflight_batch = None
                    return 0
                if lineage.epoch_attempts == 0:
                    lineage.epoch_started_at = time.monotonic()
                lineage.epoch_attempts += 1
                batch._receipt.cooldown_protected = False

            # De callback krijgt voor compatibiliteit de lijst zelf. Bewaar
            # daarom één batchbegrensde snapshot: bij precommitmutatie plus
            # fout/cancellation wordt inhoud én volgorde hersteld vóór requeue,
            # split, quarantaine of reset-discard.
            original_observations = tuple(batch)
            try:
                _LOGGER.debug(
                    "ObservationFusionEngine: batch %s van %d observaties (%s)",
                    batch.batch_id,
                    len(batch),
                    ", ".join(
                        f"{obs_type.value}:"
                        f"{sum(1 for item in batch if item.obs_type == obs_type)}"
                        for obs_type in ObservationType
                        if any(
                            item.obs_type == obs_type for item in batch
                        )
                    ),
                )
                await self._on_batch(batch)
                batch.acknowledge()
                async with self._lock:
                    self._record_lineage_success(batch)
                    self._account_batch_delivered_locked(batch)
                    if self._inflight_batch is batch:
                        self._inflight_batch = None
            except asyncio.CancelledError:
                async with self._lock:
                    if not batch.committed:
                        batch[:] = original_observations
                    if batch.committed:
                        self._record_lineage_success(batch)
                        self._account_batch_delivered_locked(batch)
                    elif self._resetting:
                        self._account_batch_reset_discard_locked(batch)
                    else:
                        batch._receipt.next_retry_at = 0.0
                        self._retry_batches.appendleft(batch)
                        self._account_requeue_locked(
                            batch, reason="cancellation"
                        )
                    if self._inflight_batch is batch:
                        self._inflight_batch = None
                raise
            except Exception as err:
                quarantined_now = []
                async with self._lock:
                    if not batch.committed:
                        batch[:] = original_observations
                    if not batch.committed and not self._resetting:
                        quarantine_start = len(
                            self._quarantined_batches
                        )
                        action = self._stage_failed_batch_locked(batch, err)
                        if action == "quarantine":
                            quarantined_now = list(
                                self._quarantined_batches
                            )[quarantine_start:]
                    elif not batch.committed:
                        self._account_batch_reset_discard_locked(batch)
                        action = "reset_discard"
                    else:
                        action = "committed"
                        self._record_lineage_success(batch)
                        self._account_batch_delivered_locked(batch)
                    if self._inflight_batch is batch:
                        self._inflight_batch = None
                if action == "infrastructure_wait":
                    _LOGGER.debug(
                        "ObservationFusionEngine: rootbatch %s wacht intact "
                        "op worker/capaciteit; %d observaties blijven bewaard",
                        batch.root_batch_id,
                        len(batch),
                    )
                    return 0
                if action == "retry":
                    _LOGGER.exception(
                        "ObservationFusionEngine: fout vóór commit van batch "
                        "%s; %d observaties na backoff opnieuw ingepland "
                        "(poging %d)",
                        batch.batch_id,
                        len(batch),
                        batch.attempts,
                    )
                    return 0
                if action == "split":
                    if batch.parent_batch_id is None:
                        _LOGGER.exception(
                            "ObservationFusionEngine: rootbatch %s na %d "
                            "pogingen atomair opgesplitst; alle %d "
                            "observaties blijven bewaard",
                            batch.batch_id,
                            batch.attempts,
                            len(batch),
                        )
                    elif self._should_log_lineage_failure(batch):
                        _LOGGER.warning(
                            "ObservationFusionEngine: deelbatch %s "
                            "opgesplitst (%d observaties; lineagefout %d; %s)",
                            batch.batch_id,
                            len(batch),
                            batch._receipt.lineage.failures,
                            batch._receipt.last_error,
                        )
                    return 0
                if action == "quarantine":
                    if (
                        quarantined_now
                        and self._persist_quarantine is not None
                    ):
                        await self._persist_quarantine(
                            quarantined_now, err
                        )
                    if self._should_log_lineage_failure(batch):
                        _LOGGER.warning(
                            "ObservationFusionEngine: %d observaties uit "
                            "deelbatch %s geïsoleerd (lineagefout %d; %s); "
                            "handmatige retry blijft mogelijk",
                            len(batch),
                            batch.batch_id,
                            batch._receipt.lineage.failures,
                            batch._receipt.last_error,
                        )
                    return 0
                if action == "reset_discard":
                    _LOGGER.info(
                        "ObservationFusionEngine: batch %s faalde tijdens "
                        "bewuste region-reset; %d observaties als discard "
                        "geboekt",
                        batch.batch_id,
                        batch._receipt.observation_count,
                    )
                    return 0
                _LOGGER.exception(
                    "ObservationFusionEngine: niet-kritieke fout na commit "
                    "van batch %s; batch wordt niet opnieuw toegepast",
                    batch.batch_id,
                )
                return batch._receipt.observation_count
            except BaseException as err:
                # CancelledError heeft hierboven een herstelpad. Een andere
                # BaseException wordt vóór propagatie teruggezet. Als de
                # runtime daarna stopt, kan durable recovery dezelfde root
                # hervatten; in een nog levende runtime blijft de liability
                # dus evenmin stil verdwijnen.
                async with self._lock:
                    if not batch.committed:
                        batch[:] = original_observations
                    if batch.committed:
                        self._record_lineage_success(batch)
                        self._account_batch_delivered_locked(batch)
                    elif self._resetting:
                        self._account_batch_reset_discard_locked(batch)
                    else:
                        batch._receipt.next_retry_at = 0.0
                        self._retry_batches.appendleft(batch)
                        self._account_requeue_locked(
                            batch, reason="fatal"
                        )
                        self._last_delivery_error = (
                            f"{type(err).__name__}: {str(err)[:240]}"
                        )
                    if self._inflight_batch is batch:
                        self._inflight_batch = None
                raise
            finally:
                async with self._lock:
                    # Bij een directe flush kan er geen achtergrondtask meer
                    # bestaan. Laat achtergebleven of teruggezette observaties
                    # dan niet zonder volgende afleverpoging staan.
                    self._schedule_batch_locked()
            return batch._receipt.observation_count
