"""Persistent worker-side lifecycle for operational snapshot publication.

This module is deliberately a small composition boundary.  It owns no
provider, network or Home Assistant integration and performs no
meteorological interpretation.  It always supports the target-complete
``unknown`` snapshot and can additionally consume an exact durable admitted
fact revision through the fact-store handoff.

One revision is created only for an active desired-state change, a controlled
pre-expiry renewal or an explicit durable fact trigger. Worker status has its own MQTT topic and is
sampled into a snapshot only when such a revision is already required.  This
prevents status churn from consuming the append-only snapshot history.
"""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
import inspect
import json
import math
import time
from typing import Any, Protocol

from .desired_state import DesiredState, DesiredStateStore
from .operational_snapshot_publication import (
    FactRevisionAuditStore,
    OperationalSnapshotPublicationCoordinator,
    PublicationOutboxStore,
    PublicationTransportHead,
    RichSnapshotBuilder,
)
from .operational_snapshot_store import OperationalSnapshotStore
from .worker_first_runtime import (
    FactPublicationBinding,
    FactPublicationReceipt,
)


CONTRACT = "stv3-worker-operational-snapshot-runtime-v1"
REASON_CODE = "awaiting_meteorological_evidence"
MAX_COUNTER = 1_000_000_000
MAX_DIAGNOSTIC_BYTES = 4 * 1024
# The worker lifecycle ticks every ten seconds by default.  Renewing thirty
# seconds before the builder's five-minute expiry leaves multiple bounded
# publication opportunities without creating a revision on every tick.
SNAPSHOT_REFRESH_MARGIN_SECONDS = 30.0

_WORKER_STATES = frozenset({
    "online",
    "busy",
    "degraded",
    "offline",
    "unknown",
})


class OperationalSnapshotRuntimeError(RuntimeError):
    """The runtime cannot safely evaluate or publish its bounded state."""


class StatusSupplier(Protocol):
    def __call__(self) -> Mapping[str, Any] | Awaitable[Mapping[str, Any]]: ...


class DesiredStateReader(Protocol):
    def load_active(self) -> DesiredState | None: ...


class SnapshotStore(Protocol):
    def head(self) -> dict[str, Any] | None: ...

    def commit(
        self,
        commit_id: str,
        *,
        expected_revision: int,
        snapshot: Mapping[str, Any],
    ) -> dict[str, Any]: ...


class TransportOutbox(Protocol):
    def load(self) -> PublicationTransportHead | None: ...

    def stage(
        self,
        *,
        revision: int,
        envelope_sha256: str,
        desired_state_sha256: str,
        publication_id_sha256: str,
    ) -> PublicationTransportHead: ...

    def adopt_pending(
        self,
        *,
        revision: int,
        envelope_sha256: str,
        publication_id_sha256: str,
    ) -> PublicationTransportHead: ...

    def mark_pending(
        self, *, revision: int, envelope_sha256: str
    ) -> PublicationTransportHead: ...

    def record_attempt(
        self, *, revision: int, envelope_sha256: str
    ) -> PublicationTransportHead: ...

    def mark_published(
        self, *, revision: int, envelope_sha256: str
    ) -> PublicationTransportHead: ...

    def mark_superseded(
        self, *, revision: int, envelope_sha256: str
    ) -> PublicationTransportHead: ...

    def discard_staged(
        self, *, revision: int, envelope_sha256: str
    ) -> None: ...


class FactRevisionSourceStore(FactRevisionAuditStore, Protocol):
    def select_for_publication(
        self,
        desired_state: DesiredState,
        *,
        expected_snapshot_revision: int,
    ) -> Any: ...

    def load(self, pipeline_item_id: str) -> Any: ...


Publisher = Callable[[bytes], Awaitable[Any] | Any]


@dataclass(frozen=True, slots=True)
class _WorkerStatus:
    state: str
    queue_depth: int
    failed_jobs: int


@dataclass(frozen=True, slots=True)
class _ChangeSignature:
    desired_state_sha256: str


@dataclass(frozen=True, slots=True)
class _DurablePublication:
    signature: _ChangeSignature
    valid_until: float


def _bounded_counter(value: object, name: str) -> int:
    if type(value) is not int or not 0 <= value <= MAX_COUNTER:
        raise OperationalSnapshotRuntimeError(
            f"workerstatus {name} is geen begrensde teller"
        )
    return value


def _worker_status(value: object) -> _WorkerStatus:
    if not isinstance(value, Mapping):
        raise OperationalSnapshotRuntimeError(
            "status_supplier gaf geen mapping terug"
        )
    state = value.get("state")
    if state not in _WORKER_STATES:
        raise OperationalSnapshotRuntimeError(
            "workerstatus state is niet ondersteund"
        )
    return _WorkerStatus(
        state=str(state),
        queue_depth=_bounded_counter(value.get("queue_depth"), "queue_depth"),
        failed_jobs=_bounded_counter(value.get("failed_jobs"), "failed_jobs"),
    )


class OperationalSnapshotRuntime:
    """Single-flight persistent lifecycle around the durable coordinator.

    ``async_start`` performs crash recovery and one initial change evaluation.
    Subsequent calls to ``async_tick`` are serialized and idempotent for an
    unchanged desired-state identity before its renewal window.  ``async_stop`` cancels an
    in-flight transport attempt through the coordinator; its durable pending
    record remains recoverable by a fresh runtime.
    """

    def __init__(
        self,
        *,
        publisher: Publisher,
        status_supplier: StatusSupplier,
        desired_state_store: DesiredStateReader | None = None,
        snapshot_store: SnapshotStore | None = None,
        outbox_store: TransportOutbox | None = None,
        rich_snapshot_builder: RichSnapshotBuilder | None = None,
        fact_revision_store: FactRevisionAuditStore | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if not callable(publisher):
            raise TypeError("publisher moet callable zijn")
        if not callable(status_supplier):
            raise TypeError("status_supplier moet callable zijn")
        if not callable(clock):
            raise TypeError("clock moet callable zijn")

        self._desired_store = (
            DesiredStateStore()
            if desired_state_store is None
            else desired_state_store
        )
        self._snapshot_store = (
            OperationalSnapshotStore(clock=clock)
            if snapshot_store is None
            else snapshot_store
        )
        self._outbox = (
            PublicationOutboxStore(clock=clock)
            if outbox_store is None
            else outbox_store
        )
        self._status_supplier = status_supplier
        self._clock = clock
        select_ready = (
            None
            if fact_revision_store is None
            else getattr(fact_revision_store, "select_for_publication", None)
        )
        self._fact_revision_source = (
            fact_revision_store if callable(select_ready) else None
        )
        if rich_snapshot_builder is None and self._fact_revision_source is not None:
            def stored_fact_builder(desired: DesiredState, **kwargs: Any):
                record = self._fact_revision_source.select_for_publication(
                    desired,
                    expected_snapshot_revision=kwargs["revision"],
                )
                return None if record is None else record.revision

            rich_snapshot_builder = stored_fact_builder
        self._coordinator = OperationalSnapshotPublicationCoordinator(
            desired_state_store=self._desired_store,
            snapshot_store=self._snapshot_store,
            outbox_store=self._outbox,
            publisher=publisher,
            clock=clock,
            rich_snapshot_builder=rich_snapshot_builder,
            fact_revision_store=fact_revision_store,
        )

        self._gate = asyncio.Lock()
        self._started = False
        self._stop_requested = False
        self._closed = False
        self._in_flight = False
        self._state = "new"
        self._configured = False
        self._last_error_code: str | None = None
        self._published_signature: _ChangeSignature | None = None
        self._published_valid_until: float | None = None
        self._counts = {
            "start_calls": 0,
            "ticks": 0,
            "unconfigured_ticks": 0,
            "unchanged_ticks": 0,
            "publication_requests": 0,
            "published_changes": 0,
            "ttl_refreshes": 0,
            "fact_triggers": 0,
            "fact_trigger_noops": 0,
            "fact_publications": 0,
            "recovery_attempts": 0,
            "failures": 0,
            "cancellations": 0,
            "stop_calls": 0,
        }

    def _increment(self, name: str) -> None:
        self._counts[name] = min(MAX_COUNTER, self._counts[name] + 1)

    def _record_failure(self, code: str) -> None:
        # Error messages and arbitrary exception text are intentionally never
        # retained: diagnostics must stay fixed, bounded and secret-free.
        self._increment("failures")
        self._last_error_code = code
        if not self._closed and not self._stop_requested:
            self._state = "degraded"

    @staticmethod
    def _coordinator_error(status: Mapping[str, Any]) -> str | None:
        code = status.get("last_error_code")
        if code is None:
            return None
        if code not in {
            "head_mismatch",
            "cas_conflict",
            "outbox_error",
            "publication_failed",
        }:
            return "publication_error"
        return f"publication_{code}"

    def _load_active(self) -> DesiredState | None:
        desired = self._desired_store.load_active()
        if desired is not None and not isinstance(desired, DesiredState):
            raise OperationalSnapshotRuntimeError(
                "desired_state_store gaf geen gevalideerde DesiredState"
            )
        self._configured = desired is not None
        return desired

    def _clock_now(self) -> float:
        value = self._clock()
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise OperationalSnapshotRuntimeError("runtimeclock is geen tijdstip")
        result = float(value)
        if not math.isfinite(result) or result < 0:
            raise OperationalSnapshotRuntimeError(
                "runtimeclock is geen eindig positief tijdstip"
            )
        return result

    def _durable_publication(
        self,
        desired: DesiredState,
    ) -> _DurablePublication | None:
        """Return one exactly corroborated published identity and expiry."""

        transport = self._outbox.load()
        if (
            transport is None
            or transport.state != "published"
            or transport.desired_state_sha256 != desired.state_sha256
        ):
            return None
        head = self._snapshot_store.head()
        if not isinstance(head, Mapping) or (
            head.get("revision") != transport.revision
            or head.get("envelope_sha256") != transport.envelope_sha256
        ):
            raise OperationalSnapshotRuntimeError(
                "snapshothead en gepubliceerde outbox wijken af"
            )
        snapshot = head.get("snapshot")
        if not isinstance(snapshot, Mapping):
            raise OperationalSnapshotRuntimeError(
                "snapshothead bevat geen gevalideerde snapshot"
            )
        if snapshot.get("desired_state_sha256") != desired.state_sha256:
            raise OperationalSnapshotRuntimeError(
                "snapshothead hoort bij een andere desired-state-authoriteit"
            )
        valid_until = snapshot.get("valid_until")
        if (
            isinstance(valid_until, bool)
            or not isinstance(valid_until, (int, float))
            or not math.isfinite(float(valid_until))
            or float(valid_until) < 0
        ):
            raise OperationalSnapshotRuntimeError(
                "snapshothead bevat geen geldige valid_until"
            )
        return _DurablePublication(
            signature=_ChangeSignature(
                desired_state_sha256=desired.state_sha256,
            ),
            valid_until=float(valid_until),
        )

    async def _supply_worker_status(self) -> _WorkerStatus:
        supplied = self._status_supplier()
        if inspect.isawaitable(supplied):
            supplied = await supplied
        return _worker_status(supplied)

    async def _recover(self) -> bool:
        self._increment("recovery_attempts")
        status = await self._coordinator.async_retry_pending()
        error_code = self._coordinator_error(status)
        if error_code is not None:
            self._record_failure(error_code)
            return False
        return True

    def _ready_fact_available(self, desired: DesiredState) -> bool:
        if self._fact_revision_source is None:
            return False
        head = self._snapshot_store.head()
        expected_revision = 1 if head is None else int(head["revision"]) + 1
        record = self._fact_revision_source.select_for_publication(
            desired,
            expected_snapshot_revision=expected_revision,
        )
        return record is not None

    async def _evaluate_locked(
        self,
        *,
        recover: bool,
        publish_ready_fact: bool = False,
    ) -> None:
        self._in_flight = True
        try:
            if recover and not await self._recover():
                return

            desired = self._load_active()
            if desired is None:
                self._published_signature = None
                self._published_valid_until = None
                self._increment("unconfigured_ticks")
                self._last_error_code = None
                self._state = "unconfigured"
                return

            # A normal unchanged tick stays on the in-memory corroborated head:
            # it performs no outbox/head SQLite reads.  Startup and dirty-outbox
            # recovery always re-corroborate durability.
            if recover or self._published_signature is None:
                durable = self._durable_publication(desired)
                if durable is not None:
                    self._published_signature = durable.signature
                    self._published_valid_until = durable.valid_until

            candidate = _ChangeSignature(
                desired_state_sha256=desired.state_sha256,
            )
            # Validate the bounded status tuple on every tick so malformed
            # worker control state degrades the runtime immediately.  It is
            # O(1) and never independently triggers a snapshot revision.
            worker = await self._supply_worker_status()
            now = self._clock_now()
            fact_ready = publish_ready_fact and self._ready_fact_available(desired)
            signature_unchanged = candidate == self._published_signature
            refresh_due = signature_unchanged and (
                self._published_valid_until is None
                or now
                >= self._published_valid_until - SNAPSHOT_REFRESH_MARGIN_SECONDS
            )
            if signature_unchanged and not refresh_due and not fact_ready:
                if publish_ready_fact:
                    self._increment("fact_trigger_noops")
                self._increment("unchanged_ticks")
                self._last_error_code = None
                self._state = "running"
                return

            # Worker state is an attribute of the newly required revision, not
            # an independent revision trigger.  The already-validated latest
            # tuple is sampled into that revision.
            self._increment("publication_requests")
            result = await self._coordinator.async_publish(
                worker_state=worker.state,
                queue_depth=worker.queue_depth,
                failed_jobs=worker.failed_jobs,
                reason_code=REASON_CODE,
                worker_status_text="",
            )
            error_code = self._coordinator_error(result)
            if error_code is not None:
                self._record_failure(error_code)
                return
            if result.get("transport_state") != "published" or result.get("dirty"):
                self._record_failure("publication_incomplete")
                return

            # Reload both mutable authority and durable transport truth.  If HA
            # changed desired state during the awaited publication, only the
            # actually corroborated signature becomes the deduplication head.
            active_after = self._load_active()
            if active_after is None:
                self._published_signature = None
                self._published_valid_until = None
                self._state = "unconfigured"
                return
            published = self._durable_publication(active_after)
            if published is None:
                self._record_failure("publication_identity_mismatch")
                return
            self._published_signature = published.signature
            self._published_valid_until = published.valid_until
            self._increment("published_changes")
            if refresh_due:
                self._increment("ttl_refreshes")
            if fact_ready:
                self._increment("fact_publications")
            self._last_error_code = None
            self._state = "running"
        finally:
            self._in_flight = False

    async def async_start(self) -> dict[str, Any]:
        """Recover durable transport state and publish the initial change."""

        async with self._gate:
            if self._closed or self._stop_requested:
                raise RuntimeError("operational snapshot runtime is gesloten")
            self._increment("start_calls")
            if not self._started:
                self._started = True
                self._state = "starting"
            try:
                await self._evaluate_locked(
                    recover=True,
                    publish_ready_fact=self._fact_revision_source is not None,
                )
            except asyncio.CancelledError:
                self._increment("cancellations")
                if not self._stop_requested:
                    self._state = "degraded"
                    self._last_error_code = "operation_cancelled"
                raise
            except Exception:
                self._record_failure("runtime_failure")
            return self.diagnostics()

    async def async_tick(self) -> dict[str, Any]:
        """Evaluate one serialized status/desired-state change boundary."""

        if not self._started:
            raise RuntimeError("operational snapshot runtime is niet gestart")
        if self._closed or self._stop_requested:
            raise RuntimeError("operational snapshot runtime is gesloten")
        async with self._gate:
            if self._closed or self._stop_requested:
                raise RuntimeError("operational snapshot runtime is gesloten")
            self._increment("ticks")
            try:
                # Only a known dirty transport needs another durable recovery
                # read.  The normal ten-second tick remains an O(1) fast path.
                recover = bool(self._coordinator.transport_status()["dirty"])
                await self._evaluate_locked(recover=recover)
            except asyncio.CancelledError:
                self._increment("cancellations")
                if not self._stop_requested:
                    self._state = "degraded"
                    self._last_error_code = "operation_cancelled"
                raise
            except Exception:
                self._record_failure("runtime_failure")
            return self.diagnostics()

    async def async_publish_ready_fact(self) -> dict[str, Any]:
        """Publish one durable admitted fact revision, if exactly ready."""

        if self._fact_revision_source is None:
            raise RuntimeError("operational fact source is niet geconfigureerd")
        if not self._started:
            raise RuntimeError("operational snapshot runtime is niet gestart")
        if self._closed or self._stop_requested:
            raise RuntimeError("operational snapshot runtime is gesloten")
        async with self._gate:
            if self._closed or self._stop_requested:
                raise RuntimeError("operational snapshot runtime is gesloten")
            self._increment("fact_triggers")
            try:
                recover = bool(self._coordinator.transport_status()["dirty"])
                await self._evaluate_locked(
                    recover=recover,
                    publish_ready_fact=True,
                )
            except asyncio.CancelledError:
                self._increment("cancellations")
                if not self._stop_requested:
                    self._state = "degraded"
                    self._last_error_code = "operation_cancelled"
                raise
            except Exception:
                self._record_failure("runtime_failure")
            return self.diagnostics()

    def _validate_exact_fact_record(
        self,
        binding: FactPublicationBinding,
        *,
        allowed_states: frozenset[str],
    ) -> Any:
        source = self._fact_revision_source
        if source is None or not callable(getattr(source, "load", None)):
            raise OperationalSnapshotRuntimeError(
                "exacte fact revision source is niet geconfigureerd"
            )
        record = source.load(binding.pipeline_item_id)
        if (
            getattr(record, "pipeline_item_id", None)
            != binding.pipeline_item_id
            or getattr(record, "revision_sha256", None)
            != binding.revision_sha256
            or getattr(record, "snapshot_sha256", None)
            != binding.snapshot_sha256
            or getattr(record, "desired_state_sha256", None)
            != binding.desired_state_sha256
            or getattr(record, "authority_epoch", None)
            != binding.authority_epoch
            or getattr(record, "snapshot_revision", None)
            != binding.snapshot_revision
            or getattr(record, "state", None) not in allowed_states
        ):
            raise OperationalSnapshotRuntimeError(
                "exacte fact-publicatiebinding wijkt af"
            )
        revision = getattr(record, "revision", None)
        receipts = getattr(revision, "admission_receipts", None)
        if not isinstance(receipts, tuple) or not any(
            getattr(item, "process_receipt_sha256", None)
            == binding.process_receipt_sha256
            for item in receipts
        ):
            raise OperationalSnapshotRuntimeError(
                "fact revision mist exact processreceipt"
            )
        return record

    async def async_publish_exact_fact(
        self,
        binding: FactPublicationBinding,
    ) -> FactPublicationReceipt:
        """Publish and reload one exact fact/outbox identity before ack."""

        if not isinstance(binding, FactPublicationBinding):
            raise TypeError("binding moet FactPublicationBinding zijn")
        if not self._started:
            raise RuntimeError("operational snapshot runtime is niet gestart")
        if self._closed or self._stop_requested:
            raise RuntimeError("operational snapshot runtime is gesloten")
        async with self._gate:
            if self._closed or self._stop_requested:
                raise RuntimeError("operational snapshot runtime is gesloten")
            self._increment("fact_triggers")
            try:
                desired = self._load_active()
                if (
                    desired is None
                    or desired.state_sha256 != binding.desired_state_sha256
                    or desired.authority_epoch != binding.authority_epoch
                ):
                    raise OperationalSnapshotRuntimeError(
                        "fact binding draagt niet de actuele desired-state"
                    )
                self._validate_exact_fact_record(
                    binding,
                    allowed_states=frozenset({"ready", "snapshot_committed"}),
                )
                recover = bool(self._coordinator.transport_status()["dirty"])
                await self._evaluate_locked(
                    recover=recover,
                    publish_ready_fact=True,
                )
                record = self._validate_exact_fact_record(
                    binding,
                    allowed_states=frozenset({"snapshot_committed"}),
                )
                desired_after = self._load_active()
                if (
                    desired_after is None
                    or desired_after.state_sha256
                    != binding.desired_state_sha256
                    or desired_after.authority_epoch != binding.authority_epoch
                ):
                    raise OperationalSnapshotRuntimeError(
                        "desired-state wijzigde tijdens factpublicatie"
                    )
                transport = self._outbox.load()
                envelope_sha = getattr(
                    record,
                    "publication_envelope_sha256",
                    None,
                )
                if (
                    transport is None
                    or transport.state != "published"
                    or transport.revision != binding.snapshot_revision
                    or transport.desired_state_sha256
                    != binding.desired_state_sha256
                    or transport.envelope_sha256 != envelope_sha
                    or transport.published_at is None
                ):
                    raise OperationalSnapshotRuntimeError(
                        "exacte facttransportreceipt ontbreekt"
                    )
                return FactPublicationReceipt(
                    pipeline_item_id=binding.pipeline_item_id,
                    revision_sha256=binding.revision_sha256,
                    snapshot_sha256=binding.snapshot_sha256,
                    process_receipt_sha256=binding.process_receipt_sha256,
                    desired_state_sha256=binding.desired_state_sha256,
                    authority_epoch=binding.authority_epoch,
                    snapshot_revision=binding.snapshot_revision,
                    publication_envelope_sha256=envelope_sha,
                    transport_receipt_sha256=(
                        transport.publication_id_sha256
                    ),
                    committed_at=transport.published_at,
                )
            except asyncio.CancelledError:
                self._increment("cancellations")
                if not self._stop_requested:
                    self._state = "degraded"
                    self._last_error_code = "operation_cancelled"
                raise
            except Exception:
                self._record_failure("exact_fact_publication_failed")
                raise OperationalSnapshotRuntimeError(
                    "exact_fact_publication_failed"
                ) from None

    async def async_stop(self) -> dict[str, Any]:
        """Stop the one coordinator lifecycle without discarding its outbox."""

        if self._closed:
            return self.diagnostics()
        self._stop_requested = True
        self._state = "stopping"
        self._increment("stop_calls")

        # Close before waiting for the runtime gate: this interrupts an awaited
        # publisher and leaves its already-recorded pending outbox recoverable.
        await self._coordinator.async_close()
        async with self._gate:
            self._closed = True
            self._in_flight = False
            self._state = "stopped"
        return self.diagnostics()

    def diagnostics(self) -> dict[str, Any]:
        """Return fixed O(1) diagnostics without store reads or private data."""

        publication = self._coordinator.transport_status()
        publication_counts = publication["counts"]
        result = {
            "contract": CONTRACT,
            "state": self._state,
            "started": self._started,
            "closed": self._closed,
            "in_flight": self._in_flight or bool(publication["in_flight"]),
            "desired_state_configured": self._configured,
            "last_error_code": self._last_error_code,
            "counts": dict(self._counts),
            "publication": {
                "state": publication["state"],
                "dirty": publication["dirty"],
                "snapshot_revision": publication["snapshot_revision"],
                "transport_state": publication["transport_state"],
                "transport_attempt_count": publication[
                    "transport_attempt_count"
                ],
                "last_error_code": publication["last_error_code"],
                "counts": {
                    "commits": publication_counts["commits"],
                    "publish_attempts": publication_counts["publish_attempts"],
                    "published": publication_counts["published"],
                    "recovered": publication_counts["recovered"],
                    "failures": publication_counts["failures"],
                    "cancellations": publication_counts["cancellations"],
                    "unknown_snapshots_built": publication_counts[
                        "unknown_snapshots_built"
                    ],
                    "rich_snapshots_built": publication_counts[
                        "rich_snapshots_built"
                    ],
                    "rich_snapshots_withheld": publication_counts[
                        "rich_snapshots_withheld"
                    ],
                    "rich_builder_failures": publication_counts[
                        "rich_builder_failures"
                    ],
                },
            },
        }
        encoded = json.dumps(
            result,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
        if len(encoded) > MAX_DIAGNOSTIC_BYTES:
            raise OperationalSnapshotRuntimeError(
                "runtime diagnostics overschrijden de vaste bytegrens"
            )
        return result


__all__ = [
    "CONTRACT",
    "FactRevisionSourceStore",
    "MAX_DIAGNOSTIC_BYTES",
    "OperationalSnapshotRuntime",
    "OperationalSnapshotRuntimeError",
    "REASON_CODE",
    "SNAPSHOT_REFRESH_MARGIN_SECONDS",
    "TransportOutbox",
]
